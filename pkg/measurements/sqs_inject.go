// Copyright 2025 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package measurements

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// SpotInstance represents a target instance for SQS injection.
type SpotInstance struct {
	InstanceID  string `json:"instanceId"`
	NodeName    string `json:"nodeName"`
	MachineName string `json:"machineName"`
}

const (
	EventTypeSpotITN    = "spot-itn"
	EventTypeRebalance  = "rebalance"
	EventTypeMixed      = "mixed"
	maxBatchSize        = 10
)

func buildMessageBody(instanceID, eventType, region, accountID string, eventTime time.Time) string {
	var detailType, detail string
	switch eventType {
	case EventTypeRebalance:
		detailType = "EC2 Instance Rebalance Recommendation"
		detail = fmt.Sprintf(`{"instance-id":"%s"}`, instanceID)
	default:
		detailType = "EC2 Spot Instance Interruption Warning"
		detail = fmt.Sprintf(`{"instance-id":"%s","instance-action":"terminate"}`, instanceID)
	}
	return fmt.Sprintf(
		`{"version":"0","source":"aws.ec2","detail-type":"%s","detail":%s,"id":"%s","time":"%s","region":"%s","account":"%s"}`,
		detailType, detail, uuid.New().String(), eventTime.Format(time.RFC3339), region, accountID,
	)
}

func resolveEventType(baseType string, index int) string {
	if baseType == EventTypeMixed {
		if index%2 == 0 {
			return EventTypeSpotITN
		}
		return EventTypeRebalance
	}
	return baseType
}

func PurgeSQSQueue(queueURL, region string) error {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %w", err)
	}
	client := sqs.New(sess)
	_, err = client.PurgeQueue(&sqs.PurgeQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	if err != nil {
		return fmt.Errorf("failed to purge SQS queue: %w", err)
	}
	log.Infof("Purged SQS queue %s", queueURL)
	return nil
}

// InjectSpotInterruptions sends synthetic SQS messages for the given instances
// and returns a map of instanceID → injection time (T0).
func InjectSpotInterruptions(queueURL, region, accountID, eventType string, instances []SpotInstance) (map[string]time.Time, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}
	client := sqs.New(sess)
	injectionTimes := make(map[string]time.Time, len(instances))

	for batchStart := 0; batchStart < len(instances); batchStart += maxBatchSize {
		batchEnd := batchStart + maxBatchSize
		if batchEnd > len(instances) {
			batchEnd = len(instances)
		}
		batch := instances[batchStart:batchEnd]

		t0 := time.Now().UTC()
		entries := make([]*sqs.SendMessageBatchRequestEntry, 0, len(batch))
		for i, inst := range batch {
			globalIdx := batchStart + i
			evtType := resolveEventType(eventType, globalIdx)
			body := buildMessageBody(inst.InstanceID, evtType, region, accountID, t0)
			entries = append(entries, &sqs.SendMessageBatchRequestEntry{
				Id:          aws.String(fmt.Sprintf("msg-%d", globalIdx)),
				MessageBody: aws.String(body),
			})
			injectionTimes[inst.InstanceID] = t0
		}

		out, err := client.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  entries,
		})
		if err != nil {
			return injectionTimes, fmt.Errorf("SQS SendMessageBatch failed at offset %d: %w", batchStart, err)
		}
		if len(out.Failed) > 0 {
			for _, f := range out.Failed {
				log.Errorf("SQS batch entry %s failed: %s — %s", aws.StringValue(f.Id), aws.StringValue(f.Code), aws.StringValue(f.Message))
			}
			return injectionTimes, fmt.Errorf("%d SQS messages failed in batch at offset %d", len(out.Failed), batchStart)
		}
		log.Infof("Injected %d spot interruption messages (batch %d-%d)", len(batch), batchStart, batchEnd-1)
	}
	log.Infof("Total injected: %d spot interruption messages", len(instances))
	return injectionTimes, nil
}

// SQSQueueSnapshot holds a point-in-time sample of SQS queue attributes.
type SQSQueueSnapshot struct {
	Timestamp         time.Time
	MessagesAvailable int
	MessagesInFlight  int
}

var sqsQueueAttributes = []*string{
	aws.String(sqs.QueueAttributeNameApproximateNumberOfMessages),
	aws.String(sqs.QueueAttributeNameApproximateNumberOfMessagesNotVisible),
}

// MonitorSQSQueue polls SQS queue attributes at the given interval and sends
// snapshots to the returned channel. Stops when ctx is cancelled.
func MonitorSQSQueue(ctx context.Context, queueURL, region string, interval time.Duration) <-chan SQSQueueSnapshot {
	ch := make(chan SQSQueueSnapshot, 256)
	go func() {
		defer close(ch)
		sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
		if err != nil {
			log.Errorf("SQS monitor: failed to create AWS session: %v", err)
			return
		}
		client := sqs.New(sess)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		poll := func() {
			out, err := client.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl:       aws.String(queueURL),
				AttributeNames: sqsQueueAttributes,
			})
			if err != nil {
				log.Debugf("SQS monitor: GetQueueAttributes error: %v", err)
				return
			}
			snap := SQSQueueSnapshot{Timestamp: time.Now().UTC()}
			if v, ok := out.Attributes[sqs.QueueAttributeNameApproximateNumberOfMessages]; ok {
				snap.MessagesAvailable, _ = strconv.Atoi(aws.StringValue(v))
			}
			if v, ok := out.Attributes[sqs.QueueAttributeNameApproximateNumberOfMessagesNotVisible]; ok {
				snap.MessagesInFlight, _ = strconv.Atoi(aws.StringValue(v))
			}
			select {
			case ch <- snap:
			default:
			}
		}

		poll()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				poll()
			}
		}
	}()
	return ch
}
