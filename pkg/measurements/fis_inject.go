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
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/fis"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

// SpotInstance represents a target instance for FIS interruption.
type SpotInstance struct {
	InstanceID  string `json:"instanceId"`
	NodeName    string `json:"nodeName"`
	MachineName string `json:"machineName"`
}

// tagInstanceBatch tags a batch of instances with the given tag key and value.
func tagInstanceBatch(ec2Client *ec2.EC2, instances []SpotInstance, tagKey, tagValue string) error {
	ids := make([]*string, 0, len(instances))
	for _, inst := range instances {
		ids = append(ids, aws.String(inst.InstanceID))
	}
	_, err := ec2Client.CreateTags(&ec2.CreateTagsInput{
		Resources: ids,
		Tags: []*ec2.Tag{{
			Key:   aws.String(tagKey),
			Value: aws.String(tagValue),
		}},
	})
	return err
}

const maxFISTargetsPerExperiment = 100

// createFISTemplate creates a FIS experiment template targeting instances
// with the given tag key/value using ALL selection mode.
func createFISTemplate(fisClient *fis.FIS, clusterName, fisRoleARN, tagKey, tagValue string) (string, error) {
	out, err := fisClient.CreateExperimentTemplate(&fis.CreateExperimentTemplateInput{
		Description: aws.String(fmt.Sprintf("NTH spot interruption test for %s", clusterName)),
		RoleArn:     aws.String(fisRoleARN),
		Actions: map[string]*fis.CreateExperimentTemplateActionInput{
			"interruptSpotInstances": {
				ActionId: aws.String("aws:ec2:send-spot-instance-interruptions"),
				Parameters: map[string]*string{
					"durationBeforeInterruption": aws.String("PT2M"),
				},
				Targets: map[string]*string{
					"SpotInstances": aws.String("spotTargets"),
				},
			},
		},
		Targets: map[string]*fis.CreateExperimentTemplateTargetInput{
			"spotTargets": {
				ResourceType: aws.String("aws:ec2:spot-instance"),
				ResourceTags: map[string]*string{
					tagKey: aws.String(tagValue),
				},
				Filters: []*fis.ExperimentTemplateTargetInputFilter{{
					Path:   aws.String("State.Name"),
					Values: []*string{aws.String("running")},
				}},
				SelectionMode: aws.String("ALL"),
			},
		},
		StopConditions: []*fis.CreateExperimentTemplateStopConditionInput{{
			Source: aws.String("none"),
		}},
		Tags: map[string]*string{
			"nth-cluster": aws.String(clusterName),
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create FIS experiment template: %w", err)
	}

	templateID := aws.StringValue(out.ExperimentTemplate.Id)
	return templateID, nil
}

// DeleteFISExperimentTemplate deletes a FIS experiment template.
func DeleteFISExperimentTemplate(region, templateID string) error {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %w", err)
	}
	client := fis.New(sess)
	_, err = client.DeleteExperimentTemplate(&fis.DeleteExperimentTemplateInput{
		Id: aws.String(templateID),
	})
	if err != nil {
		return fmt.Errorf("failed to delete FIS template %s: %w", templateID, err)
	}
	log.Infof("Deleted FIS experiment template %s", templateID)
	return nil
}

// TriggerFISExperiment starts an FIS experiment and returns the experiment ID
// and start time (T0).
func TriggerFISExperiment(region, templateID string) (string, time.Time, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to create AWS session: %w", err)
	}
	client := fis.New(sess)

	t0 := time.Now().UTC()
	out, err := client.StartExperiment(&fis.StartExperimentInput{
		ExperimentTemplateId: aws.String(templateID),
	})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to start FIS experiment: %w", err)
	}

	experimentID := aws.StringValue(out.Experiment.Id)
	log.Infof("FIS experiment started: %s (template=%s)", experimentID, templateID)
	return experimentID, t0, nil
}

// RunFISInterruption partitions instances into user-batches of batchSize,
// each sub-partitioned into FIS batches of 100 (API limit). Between each
// user-batch it waits batchInterval. Returns per-instance T0 timestamps.
// If batchSize <= 0, all instances are interrupted at once (single batch).
// The optional onBatchTriggered callback is invoked immediately after each
// user-batch is triggered, allowing callers to update metrics in real-time.
func RunFISInterruption(region, clusterName, fisRoleARN string, instances []SpotInstance, batchSize int, batchInterval time.Duration, onBatchTriggered func(instances []SpotInstance, t0 time.Time)) (templateIDs []string, t0Map map[string]time.Time, err error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create AWS session: %w", err)
	}
	ec2Client := ec2.New(sess)
	fisClient := fis.New(sess)

	if batchSize <= 0 || batchSize >= len(instances) {
		batchSize = len(instances)
	}
	numUserBatches := (len(instances) + batchSize - 1) / batchSize
	t0Map = make(map[string]time.Time, len(instances))

	log.Infof("FIS interruption: %d instances, user-batch size=%d, interval=%v, %d user-batches",
		len(instances), batchSize, batchInterval, numUserBatches)

	subBatchIdx := 0
	for ub := 0; ub < numUserBatches; ub++ {
		ubStart := ub * batchSize
		ubEnd := ubStart + batchSize
		if ubEnd > len(instances) {
			ubEnd = len(instances)
		}
		userBatch := instances[ubStart:ubEnd]

		// Sub-partition user-batch into FIS batches of 100
		numSubBatches := (len(userBatch) + maxFISTargetsPerExperiment - 1) / maxFISTargetsPerExperiment
		var batchTemplateIDs []string

		for sb := 0; sb < numSubBatches; sb++ {
			sbStart := sb * maxFISTargetsPerExperiment
			sbEnd := sbStart + maxFISTargetsPerExperiment
			if sbEnd > len(userBatch) {
				sbEnd = len(userBatch)
			}
			fisBatch := userBatch[sbStart:sbEnd]
			tagKey := fmt.Sprintf("nth-fis-batch-%d", subBatchIdx)
			tagValue := clusterName

			if err := tagInstanceBatch(ec2Client, fisBatch, tagKey, tagValue); err != nil {
				CleanupFISTemplates(region, templateIDs)
				return nil, nil, fmt.Errorf("failed to tag sub-batch %d: %w", subBatchIdx, err)
			}
			log.Infof("Tagged %d instances with %s=%s (user-batch %d, sub-batch %d)", len(fisBatch), tagKey, tagValue, ub, subBatchIdx)

			templateID, err := createFISTemplate(fisClient, clusterName, fisRoleARN, tagKey, tagValue)
			if err != nil {
				CleanupFISTemplates(region, templateIDs)
				return nil, nil, fmt.Errorf("failed to create FIS template sub-batch %d: %w", subBatchIdx, err)
			}
			log.Infof("Created FIS template %s (user-batch %d, sub-batch %d, %d instances)", templateID, ub, subBatchIdx, len(fisBatch))
			templateIDs = append(templateIDs, templateID)
			batchTemplateIDs = append(batchTemplateIDs, templateID)
			subBatchIdx++
		}

		// Trigger all templates for this user-batch
		t0 := time.Now().UTC()
		for _, templateID := range batchTemplateIDs {
			if _, _, err := TriggerFISExperiment(region, templateID); err != nil {
				log.Errorf("Failed to start FIS experiment (user-batch %d, template=%s): %v", ub, templateID, err)
			}
		}
		for _, inst := range userBatch {
			t0Map[inst.InstanceID] = t0
		}
		log.Infof("User batch %d/%d: triggered %d instances (%d FIS templates), t0=%v", ub+1, numUserBatches, len(userBatch), len(batchTemplateIDs), t0)
		if onBatchTriggered != nil {
			onBatchTriggered(userBatch, t0)
		}

		if ub < numUserBatches-1 && batchInterval > 0 {
			log.Infof("Waiting %v before next batch...", batchInterval)
			time.Sleep(batchInterval)
		}
	}

	return templateIDs, t0Map, nil
}

// CleanupFISTemplates deletes a list of FIS experiment templates.
func CleanupFISTemplates(region string, templateIDs []string) {
	for _, id := range templateIDs {
		if err := DeleteFISExperimentTemplate(region, id); err != nil {
			log.Warnf("Failed to delete FIS template %s: %v", id, err)
		}
	}
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
