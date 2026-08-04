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
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	awsconfig "github.com/aws/aws-sdk-go/aws"
	awssession "github.com/aws/aws-sdk-go/aws/session"
	ec2svc "github.com/aws/aws-sdk-go/service/ec2"
	"github.com/cloud-bulldozer/go-commons/v2/indexers"
	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements/types"
	"github.com/kube-burner/kube-burner/v2/pkg/util/fileutils"
	"github.com/kube-burner/kube-burner/v2/pkg/watchers"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	karpenterTaintKey = "karpenter.sh/disrupted"

	karpenterSpotLatencyMeasurement          = "karpenterSpotLatencyMeasurement"
	karpenterSpotLatencyQuantilesMeasurement = "karpenterSpotLatencyQuantilesMeasurement"
)

var karpenterNodeGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "nodes",
}

var karpenterPodGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "pods",
}

var nodeClaimGVR = schema.GroupVersionResource{
	Group:    "karpenter.sh",
	Version:  "v1",
	Resource: "nodeclaims",
}

type karpenterSQSSnapshotMetric struct {
	Timestamp         time.Time `json:"timestamp"`
	MessagesAvailable int       `json:"messagesAvailable"`
	MessagesInFlight  int       `json:"messagesInFlight"`
	MetricName        string    `json:"metricName"`
	UUID              string    `json:"uuid"`
	JobName           string    `json:"jobName,omitempty"`
	Metadata          any       `json:"metadata,omitempty"`
}

type karpenterSpotMetric struct {
	Timestamp   time.Time         `json:"timestamp"`
	InstanceID  string            `json:"instanceId"`
	NodeName    string            `json:"nodeName"`
	MachineName string            `json:"machineName"`
	BatchIndex  int               `json:"batchIndex"`
	Handler     string            `json:"handler"`

	TaintTime              time.Time `json:"-"`
	NodeClaimDeleteTime    time.Time `json:"-"`
	NodeClaimGoneTime      time.Time `json:"-"`
	NodeDeleteTime         time.Time `json:"-"`
	DrainCompleteTime      time.Time `json:"-"`
	InstanceTerminatedTime time.Time `json:"-"`
	ReplacementReadyTime   time.Time `json:"-"`

	InterruptionToTaintLatency               int `json:"interruptionToTaintLatency"`
	TaintToDrainCompleteLatency              int `json:"taintToDrainCompleteLatency"`
	InterruptionToNodeDeleteLatency          int `json:"interruptionToNodeDeleteLatency"`
	InterruptionToInstanceTerminatedLatency  int `json:"interruptionToInstanceTerminatedLatency"`
	DrainCompleteToNodeClaimDeleteLatency    int `json:"drainCompleteToNodeClaimDeleteLatency"`
	NodeClaimDeleteToReadyLatency            int `json:"nodeClaimDeleteToReadyLatency"`
	InterruptionToNodeClaimDeletionLatency     int `json:"interruptionToNodeClaimDeletionLatency"`
	EndToEndLatency                          int `json:"endToEndLatency"`

	PodsOnNode             int `json:"podsOnNode"`
	MetricName string            `json:"metricName"`
	UUID       string            `json:"uuid"`
	JobName    string            `json:"jobName,omitempty"`
	Labels     map[string]string `json:"labels"`
	Metadata   any               `json:"metadata,omitempty"`
}

type karpenterSpotLatency struct {
	measurements.BaseMeasurement
	queueURL      string
	region        string
	fisRoleARN    string
	clusterName   string
	fisTemplateIDs []string
	instances     []SpotInstance

	nodeClaimWatcher *watchers.Watcher
	nodeWatcher      *watchers.Watcher
	podWatcher       *watchers.Watcher

	nodeToInstance        map[string]string // nodeName → instanceID
	nodeClaimToInstance   map[string]string // nodeClaimName → instanceID

	preExistingNodeClaims sync.Map // nodeClaimName → struct{}
	matchedReplacements   sync.Map // replacementNodeClaimName → instanceID
	instanceToReplClaim   sync.Map // instanceID → replacementNodeClaimName

	doneCh           chan struct{}
	readyCount       int32
	timeout          time.Duration

	sqsSnapshots []SQSQueueSnapshot
	sqsMu        sync.Mutex
	sqsCancel    context.CancelFunc

	fisBatchSize     int
	fisBatchInterval time.Duration

	podsOnNodes       sync.Map
	drainablePodsLeft sync.Map
	taintedNodes      sync.Map
	deletedNodes      sync.Map
}

type karpenterSpotLatencyFactory struct {
	measurements.BaseMeasurementFactory
	queueURL         string
	region           string
	fisRoleARN       string
	clusterName      string
	instances        []SpotInstance
	timeout          time.Duration
	fisBatchSize     int
	fisBatchInterval time.Duration
}

func NewKarpenterSpotLatencyFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, _ string) (measurements.MeasurementFactory, error) {
	queueURL := os.Getenv("KARPENTER_QUEUE_URL")
	if queueURL == "" {
		return nil, fmt.Errorf("KARPENTER_QUEUE_URL env var is required")
	}
	region := os.Getenv("KARPENTER_REGION")
	if region == "" {
		return nil, fmt.Errorf("KARPENTER_REGION env var is required")
	}
	fisRoleARN := os.Getenv("KARPENTER_FIS_ROLE_ARN")
	if fisRoleARN == "" {
		return nil, fmt.Errorf("KARPENTER_FIS_ROLE_ARN env var is required")
	}
	clusterName := os.Getenv("KARPENTER_CLUSTER_NAME")
	if clusterName == "" {
		return nil, fmt.Errorf("KARPENTER_CLUSTER_NAME env var is required")
	}
	instancesJSON := os.Getenv("KARPENTER_SPOT_INSTANCES")
	if instancesJSON == "" {
		return nil, fmt.Errorf("KARPENTER_SPOT_INSTANCES env var is required")
	}
	var instances []SpotInstance
	if err := json.Unmarshal([]byte(instancesJSON), &instances); err != nil {
		return nil, fmt.Errorf("failed to parse KARPENTER_SPOT_INSTANCES: %w", err)
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("KARPENTER_SPOT_INSTANCES is empty")
	}

	timeout := 4 * time.Hour
	if t := os.Getenv("KARPENTER_TIMEOUT"); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
		}
	}

	var fisBatchSize int
	if bs := os.Getenv("KARPENTER_FIS_BATCH_SIZE"); bs != "" {
		if v, err := strconv.Atoi(bs); err == nil {
			fisBatchSize = v
		}
	}
	var fisBatchInterval time.Duration
	if bi := os.Getenv("KARPENTER_FIS_BATCH_INTERVAL"); bi != "" {
		if d, err := time.ParseDuration(bi); err == nil {
			fisBatchInterval = d
		}
	}

	log.Infof("Karpenter spot latency: %d target instances, fisRoleARN=%s, timeout=%v, batchSize=%d, batchInterval=%v",
		len(instances), fisRoleARN, timeout, fisBatchSize, fisBatchInterval)

	return karpenterSpotLatencyFactory{
		BaseMeasurementFactory: measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, ""),
		queueURL:               queueURL,
		region:                 region,
		fisRoleARN:             fisRoleARN,
		clusterName:            clusterName,
		instances:              instances,
		timeout:                timeout,
		fisBatchSize:           fisBatchSize,
		fisBatchInterval:       fisBatchInterval,
	}, nil
}

func (f karpenterSpotLatencyFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &karpenterSpotLatency{
		BaseMeasurement:  f.NewBaseLatency(jobConfig, clientSet, restConfig, karpenterSpotLatencyMeasurement, karpenterSpotLatencyQuantilesMeasurement, embedCfg),
		queueURL:         f.queueURL,
		region:           f.region,
		fisRoleARN:       f.fisRoleARN,
		clusterName:      f.clusterName,
		instances:        f.instances,
		timeout:          f.timeout,
		fisBatchSize:     f.fisBatchSize,
		fisBatchInterval: f.fisBatchInterval,
	}
}

func (ks *karpenterSpotLatency) Start(measurementWg *sync.WaitGroup) error {
	ks.LatencyQuantiles, ks.NormLatencies = nil, nil
	ks.Metrics = sync.Map{}
	ks.doneCh = make(chan struct{})
	ks.readyCount = 0
	defer measurementWg.Done()

	ks.nodeToInstance = make(map[string]string, len(ks.instances))
	ks.nodeClaimToInstance = make(map[string]string, len(ks.instances))
	for _, inst := range ks.instances {
		ks.nodeToInstance[inst.NodeName] = inst.InstanceID
		ks.nodeClaimToInstance[inst.MachineName] = inst.InstanceID
	}

	ks.collectPreExistingNodeClaims()

	if err := ks.startNodeClaimWatcher(); err != nil {
		return fmt.Errorf("failed to start NodeClaim watcher: %w", err)
	}

	if err := ks.startNodeWatcher(); err != nil {
		return fmt.Errorf("failed to start Node watcher: %w", err)
	}

	ks.startReplacementReadyPoller()

	// Pre-initialize metrics
	effectiveBatchSize := ks.fisBatchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = len(ks.instances)
	}
	for i, inst := range ks.instances {
		ks.Metrics.Store(inst.InstanceID, karpenterSpotMetric{
			InstanceID:  inst.InstanceID,
			NodeName:    inst.NodeName,
			MachineName: inst.MachineName,
			BatchIndex:  i / effectiveBatchSize,
			Handler:     "karpenter",
			MetricName:  karpenterSpotLatencyMeasurement,
			UUID:        ks.Uuid,
			JobName:     ks.JobConfig.Name,
			Metadata:    ks.Metadata,
		})
	}

	if err := ks.startPodWatcher(); err != nil {
		log.Warnf("Failed to start Pod watcher (drain tracking disabled): %v", err)
	}

	ks.snapshotPodsOnNodes()
	ks.startInstanceTerminationPoller()

	onBatchTriggered := func(batchInstances []SpotInstance, t0 time.Time) {
		for _, inst := range batchInstances {
			if value, ok := ks.Metrics.Load(inst.InstanceID); ok {
				m := value.(karpenterSpotMetric)
				m.Timestamp = t0
				ks.Metrics.Store(inst.InstanceID, m)
			}
		}
	}
	templateIDs, _, err := RunFISInterruption(ks.region, ks.clusterName, ks.fisRoleARN, ks.instances, ks.fisBatchSize, ks.fisBatchInterval, onBatchTriggered)
	if err != nil {
		return fmt.Errorf("FIS interruption failed: %w", err)
	}
	ks.fisTemplateIDs = templateIDs
	log.Infof("FIS experiments started (%d templates, batchSize=%d, batchInterval=%v)", len(templateIDs), ks.fisBatchSize, ks.fisBatchInterval)

	sqsCtx, sqsCancel := context.WithCancel(context.Background())
	ks.sqsCancel = sqsCancel
	sqsCh := MonitorSQSQueue(sqsCtx, ks.queueURL, ks.region, 2*time.Second)
	go func() {
		for snap := range sqsCh {
			ks.sqsMu.Lock()
			ks.sqsSnapshots = append(ks.sqsSnapshots, snap)
			ks.sqsMu.Unlock()
		}
	}()

	log.Infof("Karpenter spot latency measurement started: %d instances, FIS experiment triggered", len(ks.instances))
	return nil
}

func (ks *karpenterSpotLatency) collectPreExistingNodeClaims() {
	dynamicClient := dynamic.NewForConfigOrDie(ks.RestConfig)
	list, err := dynamicClient.Resource(nodeClaimGVR).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "karpenter.sh/capacity-type=spot",
	})
	if err != nil {
		log.Errorf("error listing pre-existing NodeClaims: %v", err)
		return
	}
	for _, item := range list.Items {
		ks.preExistingNodeClaims.Store(item.GetName(), struct{}{})
	}
	log.Infof("Recorded %d pre-existing spot NodeClaims", len(list.Items))
}

func (ks *karpenterSpotLatency) startNodeClaimWatcher() error {
	dynamicClient := dynamic.NewForConfigOrDie(ks.RestConfig)
	ks.nodeClaimWatcher = watchers.NewWatcher(
		dynamicClient,
		"karpenterNodeClaimWatcher",
		nodeClaimGVR,
		"",
		func(options *metav1.ListOptions) {
			options.LabelSelector = "karpenter.sh/capacity-type=spot"
		},
		nil,
	)
	if err := ks.nodeClaimWatcher.Informer.SetTransform(nodeClaimTransformFunc()); err != nil {
		log.Warnf("failed to set transform for karpenterNodeClaimWatcher: %v", err)
	}
	ks.nodeClaimWatcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ks.handleNodeClaimAdd,
		UpdateFunc: func(_, newObj any) { ks.handleNodeClaimUpdate(newObj) },
		DeleteFunc: ks.handleNodeClaimDelete,
	})
	if err := ks.nodeClaimWatcher.StartAndCacheSync(); err != nil {
		return fmt.Errorf("NodeClaim watcher error: %w", err)
	}
	return nil
}

func (ks *karpenterSpotLatency) startNodeWatcher() error {
	dynamicClient := dynamic.NewForConfigOrDie(ks.RestConfig)
	ks.nodeWatcher = watchers.NewWatcher(
		dynamicClient,
		"karpenterNodeWatcher",
		karpenterNodeGVR,
		"",
		nil,
		nil,
	)
	if err := ks.nodeWatcher.Informer.SetTransform(karpenterNodeTransformFunc()); err != nil {
		log.Warnf("failed to set transform for karpenterNodeWatcher: %v", err)
	}
	ks.nodeWatcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ks.handleNodeAdd,
		UpdateFunc: func(_, newObj any) { ks.handleNodeUpdate(newObj) },
		DeleteFunc: ks.handleNodeDelete,
	})
	if err := ks.nodeWatcher.StartAndCacheSync(); err != nil {
		return fmt.Errorf("Node watcher error: %w", err)
	}
	return nil
}

func (ks *karpenterSpotLatency) startPodWatcher() error {
	dynamicClient := dynamic.NewForConfigOrDie(ks.RestConfig)
	ks.podWatcher = watchers.NewWatcher(
		dynamicClient,
		"karpenterPodWatcher",
		karpenterPodGVR,
		"",
		nil,
		nil,
	)
	if err := ks.podWatcher.Informer.SetTransform(karpenterPodTransformFunc()); err != nil {
		log.Warnf("failed to set transform for karpenterPodWatcher: %v", err)
	}
	ks.podWatcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: ks.handlePodDelete,
	})
	if err := ks.podWatcher.StartAndCacheSync(); err != nil {
		return fmt.Errorf("Pod watcher error: %w", err)
	}
	return nil
}

func (ks *karpenterSpotLatency) snapshotPodsOnNodes() {
	if ks.podWatcher == nil {
		return
	}
	for _, item := range ks.podWatcher.Informer.GetStore().List() {
		u, ok := item.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		nodeName, _, _ := unstructured.NestedString(u.Object, "spec", "nodeName")
		if _, isTarget := ks.nodeToInstance[nodeName]; !isTarget {
			continue
		}
		if isFilteredPod(u) {
			continue
		}
		ks.atomicAdd(&ks.podsOnNodes, nodeName)
		ks.atomicAdd(&ks.drainablePodsLeft, nodeName)
	}
	ks.podsOnNodes.Range(func(key, val any) bool {
		log.Infof("Karpenter: %d app pods on spot node %s at injection time", atomic.LoadInt32(val.(*int32)), key)
		return true
	})
}

func (ks *karpenterSpotLatency) startInstanceTerminationPoller() {
	go func() {
		sess, err := awssession.NewSession(&awsconfig.Config{Region: awsconfig.String(ks.region)})
		if err != nil {
			log.Warnf("Instance termination poller: failed to create AWS session: %v", err)
			return
		}
		ec2Client := ec2svc.New(sess)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		ids := make([]*string, 0, len(ks.instances))
		for _, inst := range ks.instances {
			ids = append(ids, awsconfig.String(inst.InstanceID))
		}
		pending := make(map[string]bool, len(ks.instances))
		for _, inst := range ks.instances {
			pending[inst.InstanceID] = true
		}

		for {
			select {
			case <-ticker.C:
				out, err := ec2Client.DescribeInstances(&ec2svc.DescribeInstancesInput{InstanceIds: ids})
				if err != nil {
					log.Debugf("Instance termination poller: DescribeInstances error: %v", err)
					continue
				}
				for _, res := range out.Reservations {
					for _, inst := range res.Instances {
						id := awsconfig.StringValue(inst.InstanceId)
						if !pending[id] {
							continue
						}
						state := awsconfig.StringValue(inst.State.Name)
						if state == "terminated" || state == "shutting-down" {
							now := time.Now().UTC()
							if value, exists := ks.Metrics.Load(id); exists {
								m := value.(karpenterSpotMetric)
								if m.InstanceTerminatedTime.IsZero() {
									if m.Timestamp.IsZero() {
										delete(pending, id)
										continue
									}
									m.InstanceTerminatedTime = now
									ks.Metrics.Store(id, m)
									log.Infof("Karpenter: %s instance terminated (state=%s, %vs after FIS trigger)",
										id, state, int(now.Sub(m.Timestamp).Seconds()))
								}
							}
							delete(pending, id)
						}
					}
				}
				if len(pending) == 0 {
					return
				}
			case <-ks.doneCh:
				return
			}
		}
	}()
}

func (ks *karpenterSpotLatency) startReplacementReadyPoller() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		guestClient := dynamic.NewForConfigOrDie(ks.RestConfig)
		for {
			select {
			case <-ticker.C:
				if int(atomic.LoadInt32(&ks.readyCount)) >= len(ks.instances) {
					return
				}
				ks.pollReplacementNodesReady(guestClient)
			case <-ks.doneCh:
				return
			}
		}
	}()
}

func (ks *karpenterSpotLatency) pollReplacementNodesReady(guestClient dynamic.Interface) {
	pending := make(map[string]string) // instanceID → replNodeClaimName
	ks.instanceToReplClaim.Range(func(key, val any) bool {
		instanceID := key.(string)
		if value, exists := ks.Metrics.Load(instanceID); exists {
			m := value.(karpenterSpotMetric)
			if m.ReplacementReadyTime.IsZero() {
				pending[instanceID] = val.(string)
			}
		}
		return true
	})
	if len(pending) == 0 {
		return
	}

	// Get NodeClaim → nodeName mapping
	replNodeToInstance := make(map[string]string)
	ncList, err := guestClient.Resource(nodeClaimGVR).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "karpenter.sh/capacity-type=spot",
	})
	if err != nil {
		return
	}
	existingClaims := make(map[string]bool)
	for _, nc := range ncList.Items {
		existingClaims[nc.GetName()] = true
	}
	for _, nc := range ncList.Items {
		ncName := nc.GetName()
		for instanceID, replName := range pending {
			if ncName != replName {
				continue
			}
			nodeName, found, _ := unstructured.NestedString(nc.Object, "status", "nodeName")
			if found && nodeName != "" {
				replNodeToInstance[nodeName] = instanceID
			}
		}
	}

	for instanceID, replName := range pending {
		if !existingClaims[replName] {
			log.Infof("Karpenter: %s replacement NodeClaim %s no longer exists, clearing for re-match", instanceID, replName)
			ks.instanceToReplClaim.Delete(instanceID)
			ks.matchedReplacements.Delete(replName)
		}
	}

	nodeList, err := guestClient.Resource(karpenterNodeGVR).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, node := range nodeList.Items {
		nodeName := node.GetName()
		instanceID, ok := replNodeToInstance[nodeName]
		if !ok {
			continue
		}
		conditions, found, _ := unstructured.NestedSlice(node.Object, "status", "conditions")
		if !found {
			continue
		}
		for _, cRaw := range conditions {
			c, ok := cRaw.(map[string]any)
			if !ok {
				continue
			}
			condType, _ := c["type"].(string)
			status, _ := c["status"].(string)
			if condType == "Ready" && status == "True" {
				if value, exists := ks.Metrics.Load(instanceID); exists {
					m := value.(karpenterSpotMetric)
					if m.ReplacementReadyTime.IsZero() {
						m.ReplacementReadyTime = time.Now().UTC()
						ks.Metrics.Store(instanceID, m)
						count := atomic.AddInt32(&ks.readyCount, 1)
						log.Infof("Karpenter: %s replacement node %s Ready (%d/%d) [resync]", instanceID, nodeName, count, len(ks.instances))
						if int(count) == len(ks.instances) {
							close(ks.doneCh)
						}
					}
				}
			}
		}
	}
}

// --- NodeClaim event handlers ---

func (ks *karpenterSpotLatency) handleNodeClaimAdd(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	name := u.GetName()
	if _, preExisting := ks.preExistingNodeClaims.Load(name); preExisting {
		return
	}
	ks.tryMatchReplacementClaim(u)
}

func (ks *karpenterSpotLatency) handleNodeClaimUpdate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	name := u.GetName()

	// Check if this is a target NodeClaim getting deleted
	if instanceID, ok := ks.nodeClaimToInstance[name]; ok {
		delTS := u.GetDeletionTimestamp()
		if delTS != nil {
			value, exists := ks.Metrics.Load(instanceID)
			if exists {
				m := value.(karpenterSpotMetric)
				if m.NodeClaimDeleteTime.IsZero() {
					m.NodeClaimDeleteTime = delTS.UTC()
					ks.Metrics.Store(instanceID, m)
					log.Infof("Karpenter: %s NodeClaim %s deletionTimestamp=%s", instanceID, name, delTS.Format(time.RFC3339))
				}
			}
		}
		return
	}

	// Check if this is a new replacement NodeClaim
	if _, preExisting := ks.preExistingNodeClaims.Load(name); !preExisting {
		ks.tryMatchReplacementClaim(u)
	}
}

func (ks *karpenterSpotLatency) handleNodeClaimDelete(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			u, ok = tombstone.Obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
		} else {
			return
		}
	}
	name := u.GetName()
	instanceID, ok := ks.nodeClaimToInstance[name]
	if !ok {
		return
	}
	value, exists := ks.Metrics.Load(instanceID)
	if !exists {
		return
	}
	m := value.(karpenterSpotMetric)
	if m.NodeClaimGoneTime.IsZero() {
		m.NodeClaimGoneTime = time.Now().UTC()
		ks.Metrics.Store(instanceID, m)
		log.Infof("Karpenter: %s NodeClaim %s fully deleted", instanceID, name)
	}
}

func (ks *karpenterSpotLatency) tryMatchReplacementClaim(u *unstructured.Unstructured) {
	name := u.GetName()
	if _, alreadyMatched := ks.matchedReplacements.Load(name); alreadyMatched {
		return
	}

	// Check if NodeClaim is Ready
	conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	isReady := false
	if found {
		for _, cRaw := range conditions {
			c, ok := cRaw.(map[string]any)
			if !ok {
				continue
			}
			condType, _ := c["type"].(string)
			status, _ := c["status"].(string)
			if condType == "Initialized" && status == "True" {
				isReady = true
				break
			}
		}
	}
	if !isReady {
		return
	}

	// Match to the next unmatched interrupted instance. With a single NodePool
	// all NodeClaims share the same prefix, so we can't distinguish by name.
	// Instead we match in order — the per-instance sub-stage breakdown may not
	// reflect the exact pairing, but aggregate stats (avg/P99) are correct
	// because it's a 1:1 mapping of N deletions to N replacements.
	for _, inst := range ks.instances {
		if _, hasRepl := ks.instanceToReplClaim.Load(inst.InstanceID); hasRepl {
			continue
		}
		ks.matchedReplacements.Store(name, inst.InstanceID)
		ks.instanceToReplClaim.Store(inst.InstanceID, name)

		replNode, _, _ := unstructured.NestedString(u.Object, "status", "nodeName")
		log.Infof("Karpenter: %s replacement NodeClaim %s Initialized → node=%s", inst.InstanceID, name, replNode)
		break
	}
}

// --- Node event handlers ---

func (ks *karpenterSpotLatency) handleNodeUpdate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	nodeName := u.GetName()

	if instanceID, isTarget := ks.nodeToInstance[nodeName]; isTarget {
		ks.checkTargetNodeSignals(u, instanceID)
		return
	}

	ks.checkReplacementNodeReady(u)
}

func (ks *karpenterSpotLatency) handleNodeAdd(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ks.checkReplacementNodeReady(u)
}

func (ks *karpenterSpotLatency) handleNodeDelete(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			u, ok = tombstone.Obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
		} else {
			return
		}
	}
	nodeName := u.GetName()
	instanceID, ok := ks.nodeToInstance[nodeName]
	if !ok {
		return
	}
	value, exists := ks.Metrics.Load(instanceID)
	if !exists {
		return
	}
	m := value.(karpenterSpotMetric)
	if m.NodeDeleteTime.IsZero() {
		m.NodeDeleteTime = time.Now().UTC()
		ks.deletedNodes.Store(nodeName, struct{}{})
		log.Infof("Karpenter: %s node %s removed from guest cluster", instanceID, nodeName)
		ks.Metrics.Store(instanceID, m)
	}
}

func (ks *karpenterSpotLatency) checkTargetNodeSignals(u *unstructured.Unstructured, instanceID string) {
	value, exists := ks.Metrics.Load(instanceID)
	if !exists {
		return
	}
	m := value.(karpenterSpotMetric)
	updated := false

	// Check for Karpenter disrupted taint
	if m.TaintTime.IsZero() {
		taints, found, _ := unstructured.NestedSlice(u.Object, "spec", "taints")
		if found {
			for _, tRaw := range taints {
				t, ok := tRaw.(map[string]any)
				if !ok {
					continue
				}
				key, _ := t["key"].(string)
				if key == karpenterTaintKey || strings.HasPrefix(key, "karpenter.sh/") {
					m.TaintTime = time.Now().UTC()
					updated = true
					ks.taintedNodes.Store(u.GetName(), struct{}{})
					log.Infof("Karpenter: %s node %s tainted (%s)", instanceID, u.GetName(), key)
					break
				}
			}
		}
	}

	if updated {
		ks.Metrics.Store(instanceID, m)
	}
}

func (ks *karpenterSpotLatency) checkReplacementNodeReady(u *unstructured.Unstructured) {
	nodeName := u.GetName()

	ks.instanceToReplClaim.Range(func(key, val any) bool {
		instanceID := key.(string)
		replClaimName := val.(string)

		value, exists := ks.Metrics.Load(instanceID)
		if !exists {
			return true
		}
		m := value.(karpenterSpotMetric)
		if !m.ReplacementReadyTime.IsZero() {
			return true
		}

		// Check NodeClaim watcher cache for the replacement's nodeName
		if ks.nodeClaimWatcher != nil {
			items := ks.nodeClaimWatcher.Informer.GetStore().List()
			for _, item := range items {
				nc, ok := item.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				if nc.GetName() != replClaimName {
					continue
				}
				ncNodeName, found, _ := unstructured.NestedString(nc.Object, "status", "nodeName")
				if found && ncNodeName == nodeName {
					conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
					if found {
						for _, cRaw := range conditions {
							c, ok := cRaw.(map[string]any)
							if !ok {
								continue
							}
							condType, _ := c["type"].(string)
							status, _ := c["status"].(string)
							if condType == "Ready" && status == "True" {
								m.ReplacementReadyTime = time.Now().UTC()
								ks.Metrics.Store(instanceID, m)
								count := atomic.AddInt32(&ks.readyCount, 1)
								log.Infof("Karpenter: %s replacement node %s Ready (%d/%d)", instanceID, nodeName, count, len(ks.instances))
								if int(count) == len(ks.instances) {
									close(ks.doneCh)
								}
								return false
							}
						}
					}
				}
			}
		}
		return true
	})
}

func (ks *karpenterSpotLatency) handlePodDelete(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			u, ok = tombstone.Obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
		} else {
			return
		}
	}
	nodeName, _, _ := unstructured.NestedString(u.Object, "spec", "nodeName")
	if _, isTarget := ks.nodeToInstance[nodeName]; !isTarget {
		return
	}
	if _, tainted := ks.taintedNodes.Load(nodeName); !tainted {
		return
	}
	if isFilteredPod(u) {
		return
	}
	if _, nodeGone := ks.deletedNodes.Load(nodeName); !nodeGone {
		if val, ok := ks.drainablePodsLeft.Load(nodeName); ok {
			remaining := atomic.AddInt32(val.(*int32), -1)
			if remaining == 0 {
				instanceID := ks.nodeToInstance[nodeName]
				if value, exists := ks.Metrics.Load(instanceID); exists {
					m := value.(karpenterSpotMetric)
					if m.DrainCompleteTime.IsZero() {
						m.DrainCompleteTime = time.Now().UTC()
						ks.Metrics.Store(instanceID, m)
						log.Infof("Karpenter: %s drain complete on node %s", instanceID, nodeName)
					}
				}
			}
		}
	}
}

func (ks *karpenterSpotLatency) atomicAdd(m *sync.Map, key string) {
	val, loaded := m.LoadOrStore(key, new(int32))
	if !loaded {
		atomic.StoreInt32(val.(*int32), 1)
	} else {
		atomic.AddInt32(val.(*int32), 1)
	}
}

func (ks *karpenterSpotLatency) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
}

func (ks *karpenterSpotLatency) Stop() error {
	select {
	case <-ks.doneCh:
		log.Infof("Karpenter: all %d replacements Ready", len(ks.instances))
	case <-time.After(ks.timeout):
		log.Warnf("Karpenter: timeout after %v, %d/%d replacements Ready", ks.timeout, atomic.LoadInt32(&ks.readyCount), len(ks.instances))
	}

	if ks.sqsCancel != nil {
		ks.sqsCancel()
	}
	if ks.nodeClaimWatcher != nil {
		ks.nodeClaimWatcher.StopWatcher()
	}
	if ks.nodeWatcher != nil {
		ks.nodeWatcher.StopWatcher()
	}
	if ks.podWatcher != nil {
		ks.podWatcher.StopWatcher()
	}

	CleanupFISTemplates(ks.region, ks.fisTemplateIDs)

	ks.sqsMu.Lock()
	snapshots := ks.sqsSnapshots
	ks.sqsMu.Unlock()
	var maxBacklog, maxInFlight int
	for _, s := range snapshots {
		if s.MessagesAvailable > maxBacklog {
			maxBacklog = s.MessagesAvailable
		}
		if s.MessagesInFlight > maxInFlight {
			maxInFlight = s.MessagesInFlight
		}
	}
	log.Infof("Karpenter: SQS queue stats: %d snapshots, peak backlog=%d, peak in-flight=%d", len(snapshots), maxBacklog, maxInFlight)

	return ks.StopMeasurement(ks.normalizeLatencies, ks.getLatency)
}

func (ks *karpenterSpotLatency) normalizeLatencies() float64 {
	// Collect all valid metrics first
	var validMetrics []karpenterSpotMetric
	ks.Metrics.Range(func(key, value any) bool {
		m := value.(karpenterSpotMetric)
		if m.Timestamp.IsZero() {
			return true
		}
		if !m.TaintTime.IsZero() && m.TaintTime.Before(m.Timestamp) {
			log.Warnf("Skipping %s: tainted at %v before FIS trigger at %v (already being disrupted)", m.InstanceID, m.TaintTime, m.Timestamp)
			return true
		}
		validMetrics = append(validMetrics, m)
		return true
	})

	// Collect and sort NodeClaimGoneTimes and ReplacementReadyTimes independently,
	// then pair by rank (fastest deletion ↔ fastest replacement). This avoids the
	// mispairing problem where sequential matching assigns the wrong replacement
	// to the wrong original, which can make NodeClaimDeleteToReady > EndToEnd.
	var deletionTimes []time.Time
	var readyTimes []time.Time
	for _, m := range validMetrics {
		if !m.NodeClaimGoneTime.IsZero() {
			deletionTimes = append(deletionTimes, m.NodeClaimGoneTime)
		}
		if !m.ReplacementReadyTime.IsZero() {
			readyTimes = append(readyTimes, m.ReplacementReadyTime)
		}
	}
	sort.Slice(deletionTimes, func(i, j int) bool { return deletionTimes[i].Before(deletionTimes[j]) })
	sort.Slice(readyTimes, func(i, j int) bool { return readyTimes[i].Before(readyTimes[j]) })

	// Build rank-paired NodeClaimDeleteToReady latencies
	var rankedNCDeleteToReady []int
	pairCount := len(deletionTimes)
	if len(readyTimes) < pairCount {
		pairCount = len(readyTimes)
	}
	for i := 0; i < pairCount; i++ {
		lat := int(readyTimes[i].Sub(deletionTimes[i]).Milliseconds())
		if lat < 0 {
			lat = 0
		}
		rankedNCDeleteToReady = append(rankedNCDeleteToReady, lat)
	}

	// Assign rank-paired values back to metrics (sorted by NodeClaimGoneTime)
	sort.Slice(validMetrics, func(i, j int) bool {
		if validMetrics[i].NodeClaimGoneTime.IsZero() {
			return false
		}
		if validMetrics[j].NodeClaimGoneTime.IsZero() {
			return true
		}
		return validMetrics[i].NodeClaimGoneTime.Before(validMetrics[j].NodeClaimGoneTime)
	})

	rankIdx := 0
	for i := range validMetrics {
		m := &validMetrics[i]
		if m.DrainCompleteTime.IsZero() && !m.NodeDeleteTime.IsZero() {
			m.DrainCompleteTime = m.NodeDeleteTime
		}
		if !m.TaintTime.IsZero() {
			m.InterruptionToTaintLatency = int(m.TaintTime.Sub(m.Timestamp).Milliseconds())
		}
		if !m.TaintTime.IsZero() && !m.DrainCompleteTime.IsZero() {
			m.TaintToDrainCompleteLatency = int(m.DrainCompleteTime.Sub(m.TaintTime).Milliseconds())
		}
		if !m.NodeDeleteTime.IsZero() {
			m.InterruptionToNodeDeleteLatency = int(m.NodeDeleteTime.Sub(m.Timestamp).Milliseconds())
		}
		if !m.InstanceTerminatedTime.IsZero() {
			m.InterruptionToInstanceTerminatedLatency = int(m.InstanceTerminatedTime.Sub(m.Timestamp).Milliseconds())
		}
		if !m.DrainCompleteTime.IsZero() && !m.NodeClaimGoneTime.IsZero() {
			lat := int(m.NodeClaimGoneTime.Sub(m.DrainCompleteTime).Milliseconds())
			if lat < 0 {
				lat = 0
			}
			m.DrainCompleteToNodeClaimDeleteLatency = lat
		}
		if !m.NodeClaimGoneTime.IsZero() && rankIdx < len(rankedNCDeleteToReady) {
			m.NodeClaimDeleteToReadyLatency = rankedNCDeleteToReady[rankIdx]
			rankIdx++
		}
		if !m.NodeClaimDeleteTime.IsZero() {
			m.InterruptionToNodeClaimDeletionLatency = int(m.NodeClaimDeleteTime.Sub(m.Timestamp).Milliseconds())
		}
		if !m.ReplacementReadyTime.IsZero() {
			m.EndToEndLatency = int(m.ReplacementReadyTime.Sub(m.Timestamp).Milliseconds())
		}
		if val, ok := ks.podsOnNodes.Load(m.NodeName); ok {
			m.PodsOnNode = int(atomic.LoadInt32(val.(*int32)))
		}
		ks.NormLatencies = append(ks.NormLatencies, *m)
	}
	return 0
}

func (ks *karpenterSpotLatency) getLatency(normLatency any) map[string]float64 {
	m := normLatency.(karpenterSpotMetric)
	return map[string]float64{
		"Interruption To Taint":               float64(m.InterruptionToTaintLatency),
		"Taint To Drain Complete":             float64(m.TaintToDrainCompleteLatency),
		"Interruption To Node Delete":         float64(m.InterruptionToNodeDeleteLatency),
		"Interruption To Instance Terminated": float64(m.InterruptionToInstanceTerminatedLatency),
		"Drain Complete To NodeClaim Delete":   float64(m.DrainCompleteToNodeClaimDeleteLatency),
		"NodeClaim Delete To Ready":           float64(m.NodeClaimDeleteToReadyLatency),
		"Interruption To NodeClaim Deletion":    float64(m.InterruptionToNodeClaimDeletionLatency),
		"End To End":                          float64(m.EndToEndLatency),
	}
}

func (ks *karpenterSpotLatency) Index(jobName string, indexerList map[string]indexers.Indexer) {
	ks.BaseMeasurement.Index(jobName, indexerList)
	ks.sqsMu.Lock()
	snapshots := ks.sqsSnapshots
	ks.sqsMu.Unlock()
	if len(snapshots) == 0 {
		return
	}
	sqsDocs := make([]any, 0, len(snapshots))
	for _, s := range snapshots {
		sqsDocs = append(sqsDocs, karpenterSQSSnapshotMetric{
			Timestamp:         s.Timestamp,
			MessagesAvailable: s.MessagesAvailable,
			MessagesInFlight:  s.MessagesInFlight,
			MetricName:        "nthSpotSQSMeasurement",
			UUID:              ks.Uuid,
			JobName:           ks.JobConfig.Name,
			Metadata:          ks.Metadata,
		})
	}
	metricName := fmt.Sprintf("nthSpotSQSMeasurement-%s", jobName)
	for _, indexer := range indexerList {
		log.Infof("Indexing metric nthSpotSQSMeasurement (karpenter)")
		resp, err := indexer.Index(sqsDocs, indexers.IndexingOpts{MetricName: metricName})
		if err != nil {
			log.Errorf("Error indexing nthSpotSQSMeasurement: %v", err)
		} else {
			log.Info(resp)
		}
	}
}

func (ks *karpenterSpotLatency) IsCompatible() bool {
	return true
}

// nodeClaimTransformFunc preserves fields needed for NodeClaim tracking:
// metadata: name, uid, creationTimestamp, labels, deletionTimestamp
// status: nodeName, providerID, conditions
func nodeClaimTransformFunc() cache.TransformFunc {
	return func(obj any) (any, error) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return obj, nil
		}
		metadata := map[string]any{
			"name":              u.GetName(),
			"uid":               string(u.GetUID()),
			"creationTimestamp":  u.GetCreationTimestamp().Format(time.RFC3339),
			"labels":            u.GetLabels(),
		}
		if delTS := u.GetDeletionTimestamp(); delTS != nil {
			metadata["deletionTimestamp"] = delTS.Format(time.RFC3339)
		}

		minimal := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": u.GetAPIVersion(),
				"kind":       u.GetKind(),
				"metadata":   metadata,
			},
		}
		if nodeName, found, _ := unstructured.NestedString(u.Object, "status", "nodeName"); found {
			_ = unstructured.SetNestedField(minimal.Object, nodeName, "status", "nodeName")
		}
		if providerID, found, _ := unstructured.NestedString(u.Object, "status", "providerID"); found {
			_ = unstructured.SetNestedField(minimal.Object, providerID, "status", "providerID")
		}
		if conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); found {
			_ = unstructured.SetNestedSlice(minimal.Object, conditions, "status", "conditions")
		}
		return minimal, nil
	}
}

func karpenterNodeTransformFunc() cache.TransformFunc {
	return func(obj any) (any, error) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return obj, nil
		}
		metadata := map[string]any{
			"name":              u.GetName(),
			"uid":               string(u.GetUID()),
			"creationTimestamp":  u.GetCreationTimestamp().Format(time.RFC3339),
		}
		minimal := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": u.GetAPIVersion(),
				"kind":       u.GetKind(),
				"metadata":   metadata,
			},
		}
		if taints, found, _ := unstructured.NestedSlice(u.Object, "spec", "taints"); found {
			_ = unstructured.SetNestedSlice(minimal.Object, taints, "spec", "taints")
		}
		if unschedulable, found, _ := unstructured.NestedBool(u.Object, "spec", "unschedulable"); found {
			_ = unstructured.SetNestedField(minimal.Object, unschedulable, "spec", "unschedulable")
		}
		if conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); found {
			_ = unstructured.SetNestedSlice(minimal.Object, conditions, "status", "conditions")
		}
		return minimal, nil
	}
}

func karpenterPodTransformFunc() cache.TransformFunc {
	return func(obj any) (any, error) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return obj, nil
		}
		metadata := map[string]any{
			"name":      u.GetName(),
			"namespace": u.GetNamespace(),
			"uid":       string(u.GetUID()),
		}
		if ownerRefs, found, _ := unstructured.NestedSlice(u.Object, "metadata", "ownerReferences"); found {
			minRefs := make([]any, 0, len(ownerRefs))
			for _, ref := range ownerRefs {
				r, ok := ref.(map[string]any)
				if !ok {
					continue
				}
				kind, _ := r["kind"].(string)
				minRefs = append(minRefs, map[string]any{"kind": kind})
			}
			metadata["ownerReferences"] = minRefs
		}
		minimal := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": u.GetAPIVersion(),
				"kind":       u.GetKind(),
				"metadata":   metadata,
			},
		}
		if nodeName, found, _ := unstructured.NestedString(u.Object, "spec", "nodeName"); found {
			_ = unstructured.SetNestedField(minimal.Object, nodeName, "spec", "nodeName")
		}
		if phase, found, _ := unstructured.NestedString(u.Object, "status", "phase"); found {
			_ = unstructured.SetNestedField(minimal.Object, phase, "status", "phase")
		}
		return minimal, nil
	}
}

func karpenterPodOwnerKind(u *unstructured.Unstructured) string {
	ownerRefs, found, _ := unstructured.NestedSlice(u.Object, "metadata", "ownerReferences")
	if !found {
		return ""
	}
	for _, ref := range ownerRefs {
		r, ok := ref.(map[string]any)
		if !ok {
			continue
		}
		if kind, _ := r["kind"].(string); kind != "" {
			return kind
		}
	}
	return ""
}

func isFilteredPod(u *unstructured.Unstructured) bool {
	if karpenterPodOwnerKind(u) == "DaemonSet" {
		return true
	}
	name := u.GetName()
	return strings.HasPrefix(name, "kube-apiserver-proxy-") || strings.HasPrefix(name, "kube-rbac-proxy-")
}

func machineNamePrefix(name string) string {
	idx := strings.LastIndex(name, "-")
	if idx < 0 {
		return name
	}
	return name[:idx+1]
}
