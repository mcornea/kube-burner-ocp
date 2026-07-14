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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	nthSpotLatencyMeasurement          = "nthSpotLatencyMeasurement"
	nthSpotLatencyQuantilesMeasurement = "nthSpotLatencyQuantilesMeasurement"
	nthTaintKeyPrefix                  = "aws-node-termination-handler/"
)

var nodeGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "nodes",
}

var podGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "pods",
}

type nthSpotMetric struct {
	Timestamp   time.Time         `json:"timestamp"`
	InstanceID  string            `json:"instanceId"`
	NodeName    string            `json:"nodeName"`
	MachineName string            `json:"machineName"`
	BatchIndex  int               `json:"batchIndex"`

	TaintTime              time.Time `json:"-"`
	CordonTime             time.Time `json:"-"`
	MachineDeleteTime      time.Time `json:"-"`
	MachineGoneTime        time.Time `json:"-"`
	NodeDeleteTime         time.Time `json:"-"`
	DrainCompleteTime      time.Time `json:"-"`
	InstanceTerminatedTime time.Time `json:"-"`
	ReplacementRunningTime time.Time `json:"-"`
	ReplacementReadyTime   time.Time `json:"-"`

	InterruptionToCordonLatency              int `json:"interruptionToCordonLatency"`
	CordonToDrainCompleteLatency             int `json:"cordonToDrainCompleteLatency"`
	InterruptionToNodeDeleteLatency          int `json:"interruptionToNodeDeleteLatency"`
	InterruptionToInstanceTerminatedLatency  int `json:"interruptionToInstanceTerminatedLatency"`
	DrainCompleteToMachineDeleteLatency      int `json:"drainCompleteToMachineDeleteLatency"`
	MachineDeleteToReadyLatency              int `json:"machineDeleteToReadyLatency"`
	EndToEndLatency                          int `json:"endToEndLatency"`

	PodsOnNode             int `json:"podsOnNode"`
	MetricName string            `json:"metricName"`
	UUID       string            `json:"uuid"`
	JobName    string            `json:"jobName,omitempty"`
	Labels     map[string]string `json:"labels"`
	Metadata   any               `json:"metadata,omitempty"`
}

type sqsSnapshotMetric struct {
	Timestamp         time.Time `json:"timestamp"`
	MessagesAvailable int       `json:"messagesAvailable"`
	MessagesInFlight  int       `json:"messagesInFlight"`
	MetricName        string    `json:"metricName"`
	UUID              string    `json:"uuid"`
	JobName           string    `json:"jobName,omitempty"`
	Metadata          any       `json:"metadata,omitempty"`
}

type nthSpotLatency struct {
	measurements.BaseMeasurement
	mcRestConfig  *rest.Config
	hcpNamespace  string
	queueURL      string
	region        string
	fisRoleARN    string
	clusterName   string
	fisTemplateIDs []string
	instances     []SpotInstance

	machineWatcher *watchers.Watcher
	nodeWatcher    *watchers.Watcher
	podWatcher     *watchers.Watcher

	// Maps for correlating events across objects
	nodeToInstance    map[string]string // nodeName → instanceID
	machineToInstance map[string]string // machineName → instanceID

	// Replacement tracking
	preExistingMachines   sync.Map // machineName → struct{}
	matchedReplacements   sync.Map // replacementMachineName → instanceID
	instanceToReplMachine sync.Map // instanceID → replacementMachineName

	// Completion signaling
	doneCh           chan struct{}
	readyCount       int32
	timeout          time.Duration
	machineGoneDone  chan struct{}
	replReadyDone    chan struct{}

	// SQS queue monitoring
	sqsSnapshots []SQSQueueSnapshot
	sqsMu        sync.Mutex
	sqsCancel    context.CancelFunc

	// FIS batching config
	fisBatchSize     int
	fisBatchInterval time.Duration

	// Pod termination tracking
	podsOnNodes       sync.Map // nodeName → *int32 (non-DaemonSet count at injection time)
	drainablePodsLeft sync.Map // nodeName → *int32 (remaining non-DaemonSet pods, decremented on delete)
	taintedNodes      sync.Map // nodeName → struct{} (set once taint detected)
	deletedNodes      sync.Map // nodeName → struct{} (set once node removed from guest cluster)
}

type nthSpotLatencyFactory struct {
	measurements.BaseMeasurementFactory
	mcRestConfig     *rest.Config
	hcpNamespace     string
	queueURL         string
	region           string
	fisRoleARN       string
	clusterName      string
	fisTemplateIDs   []string
	instances        []SpotInstance
	timeout          time.Duration
	fisBatchSize     int
	fisBatchInterval time.Duration
}

// NewNthSpotLatencyFactory creates a measurement factory for NTH spot interruption
// pipeline latency. It reads cluster details and instance targets from environment
// variables set by the nth-spot workload command.
func NewNthSpotLatencyFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, _ string) (measurements.MeasurementFactory, error) {
	mcKubeconfigPath := os.Getenv("MC_KUBECONFIG")
	if mcKubeconfigPath == "" {
		log.Warn("MC_KUBECONFIG not set, nthSpotLatency measurement will be disabled")
		return nil, nil
	}
	hcpNamespace := os.Getenv("HCP_NAMESPACE")
	if hcpNamespace == "" {
		log.Warn("HCP_NAMESPACE not set, nthSpotLatency measurement will be disabled")
		return nil, nil
	}
	queueURL := os.Getenv("NTH_QUEUE_URL")
	if queueURL == "" {
		return nil, fmt.Errorf("NTH_QUEUE_URL env var is required for nthSpotLatency measurement")
	}
	region := os.Getenv("NTH_REGION")
	if region == "" {
		return nil, fmt.Errorf("NTH_REGION env var is required for nthSpotLatency measurement")
	}
	fisRoleARN := os.Getenv("NTH_FIS_ROLE_ARN")
	if fisRoleARN == "" {
		return nil, fmt.Errorf("NTH_FIS_ROLE_ARN env var is required for nthSpotLatency measurement")
	}
	clusterName := os.Getenv("NTH_CLUSTER_NAME")
	if clusterName == "" {
		return nil, fmt.Errorf("NTH_CLUSTER_NAME env var is required for nthSpotLatency measurement")
	}
	instancesJSON := os.Getenv("NTH_SPOT_INSTANCES")
	if instancesJSON == "" {
		return nil, fmt.Errorf("NTH_SPOT_INSTANCES env var is required for nthSpotLatency measurement")
	}
	var instances []SpotInstance
	if err := json.Unmarshal([]byte(instancesJSON), &instances); err != nil {
		return nil, fmt.Errorf("failed to parse NTH_SPOT_INSTANCES: %w", err)
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("NTH_SPOT_INSTANCES is empty")
	}

	mcRestConfig, err := clientcmd.BuildConfigFromFlags("", mcKubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to build MC rest config from %s: %w", mcKubeconfigPath, err)
	}
	mcNamespace, err := DiscoverHCPNamespace(mcRestConfig, hcpNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to discover HCP namespace on MC: %w", err)
	}
	timeout := 4 * time.Hour
	if t := os.Getenv("NTH_TIMEOUT"); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
		}
	}

	var fisBatchSize int
	if bs := os.Getenv("NTH_FIS_BATCH_SIZE"); bs != "" {
		if v, err := strconv.Atoi(bs); err == nil {
			fisBatchSize = v
		}
	}
	var fisBatchInterval time.Duration
	if bi := os.Getenv("NTH_FIS_BATCH_INTERVAL"); bi != "" {
		if d, err := time.ParseDuration(bi); err == nil {
			fisBatchInterval = d
		}
	}

	log.Infof("NTH spot latency: MC namespace=%s, %d target instances, fisRoleARN=%s, timeout=%v, batchSize=%d, batchInterval=%v",
		mcNamespace, len(instances), fisRoleARN, timeout, fisBatchSize, fisBatchInterval)

	return nthSpotLatencyFactory{
		BaseMeasurementFactory: measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, ""),
		mcRestConfig:           mcRestConfig,
		hcpNamespace:           mcNamespace,
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

func (f nthSpotLatencyFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &nthSpotLatency{
		BaseMeasurement:  f.NewBaseLatency(jobConfig, clientSet, restConfig, nthSpotLatencyMeasurement, nthSpotLatencyQuantilesMeasurement, embedCfg),
		mcRestConfig:     f.mcRestConfig,
		hcpNamespace:     f.hcpNamespace,
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

func (ns *nthSpotLatency) Start(measurementWg *sync.WaitGroup) error {
	ns.LatencyQuantiles, ns.NormLatencies = nil, nil
	ns.Metrics = sync.Map{}
	ns.doneCh = make(chan struct{})
	SetNthDone(ns.doneCh, ns.timeout)
	ns.readyCount = 0
	defer measurementWg.Done()

	ns.nodeToInstance = make(map[string]string, len(ns.instances))
	ns.machineToInstance = make(map[string]string, len(ns.instances))
	for _, inst := range ns.instances {
		ns.nodeToInstance[inst.NodeName] = inst.InstanceID
		ns.machineToInstance[inst.MachineName] = inst.InstanceID
	}

	// Collect pre-existing Machine names for replacement detection
	ns.collectPreExistingMachines()

	// Start Machine informer on MC
	if err := ns.startMachineWatcher(); err != nil {
		return fmt.Errorf("failed to start Machine watcher: %w", err)
	}

	// Start polling for old Machine removal from informer store.
	// The informer's DeleteFunc is unreliable for label-selected Machines,
	// so we poll the store to detect when they disappear.
	ns.startMachineGonePoller()

	// Start Node informer on guest cluster for taint/cordon detection
	if err := ns.startNodeWatcher(); err != nil {
		return fmt.Errorf("failed to start Node watcher: %w", err)
	}

	// Periodically re-check replacement node readiness via API to compensate
	// for missed informer events (resyncPeriod=0 at high scale).
	ns.startReplacementReadyPoller()

	// Pre-initialize metrics so pollers can record events during batched FIS injection
	effectiveBatchSize := ns.fisBatchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = len(ns.instances)
	}
	for i, inst := range ns.instances {
		ns.Metrics.Store(inst.InstanceID, nthSpotMetric{
			InstanceID:  inst.InstanceID,
			NodeName:    inst.NodeName,
			MachineName: inst.MachineName,
			BatchIndex:  i / effectiveBatchSize,
			MetricName:  nthSpotLatencyMeasurement,
			UUID:        ns.Uuid,
			JobName:     ns.JobConfig.Name,
			Metadata:    ns.Metadata,
		})
	}

	// Start Pod informer on guest cluster for drain tracking
	if err := ns.startPodWatcher(); err != nil {
		log.Warnf("Failed to start Pod watcher (drain tracking disabled): %v", err)
	}

	// Snapshot pod counts on each target node at injection time
	ns.snapshotPodsOnNodes()

	// Start EC2 termination poller before FIS so it catches terminations
	// from earlier batches while later batches are still waiting
	ns.startInstanceTerminationPoller()

	// Tag instances, create experiment templates (batched), and trigger.
	// The callback updates each instance's T0 in the metrics store immediately
	// after its batch is triggered, so pollers see the correct timestamp.
	onBatchTriggered := func(batchInstances []SpotInstance, t0 time.Time) {
		for _, inst := range batchInstances {
			if value, ok := ns.Metrics.Load(inst.InstanceID); ok {
				m := value.(nthSpotMetric)
				m.Timestamp = t0
				ns.Metrics.Store(inst.InstanceID, m)
			}
		}
	}
	templateIDs, _, err := RunFISInterruption(ns.region, ns.clusterName, ns.fisRoleARN, ns.instances, ns.fisBatchSize, ns.fisBatchInterval, onBatchTriggered)
	if err != nil {
		return fmt.Errorf("FIS interruption failed: %w", err)
	}
	ns.fisTemplateIDs = templateIDs
	log.Infof("FIS experiments started (%d templates, batchSize=%d, batchInterval=%v)", len(templateIDs), ns.fisBatchSize, ns.fisBatchInterval)

	// Start SQS queue monitor
	sqsCtx, sqsCancel := context.WithCancel(context.Background())
	ns.sqsCancel = sqsCancel
	sqsCh := MonitorSQSQueue(sqsCtx, ns.queueURL, ns.region, 2*time.Second)
	go func() {
		for snap := range sqsCh {
			ns.sqsMu.Lock()
			ns.sqsSnapshots = append(ns.sqsSnapshots, snap)
			ns.sqsMu.Unlock()
		}
	}()

	log.Infof("NTH spot latency measurement started: %d instances, FIS experiment triggered", len(ns.instances))
	return nil
}

func (ns *nthSpotLatency) collectPreExistingMachines() {
	dynamicClient := dynamic.NewForConfigOrDie(ns.mcRestConfig)
	list, err := dynamicClient.Resource(machineGVR).Namespace(ns.hcpNamespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "hypershift.openshift.io/interruptible-instance",
	})
	if err != nil {
		log.Errorf("error listing pre-existing Machines: %v", err)
		return
	}
	for _, item := range list.Items {
		ns.preExistingMachines.Store(item.GetName(), struct{}{})
	}
	log.Infof("Recorded %d pre-existing spot Machines", len(list.Items))
}

func (ns *nthSpotLatency) startMachineWatcher() error {
	dynamicClient := dynamic.NewForConfigOrDie(ns.mcRestConfig)
	ns.machineWatcher = watchers.NewWatcher(
		dynamicClient,
		"nthMachineWatcher",
		machineGVR,
		ns.hcpNamespace,
		func(options *metav1.ListOptions) {
			options.LabelSelector = "hypershift.openshift.io/interruptible-instance"
		},
		nil,
	)
	if err := ns.machineWatcher.Informer.SetTransform(nthMachineTransformFunc()); err != nil {
		log.Warnf("failed to set transform for nthMachineWatcher: %v", err)
	}
	ns.machineWatcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ns.handleMachineAdd,
		UpdateFunc: func(_, newObj any) { ns.handleMachineUpdate(newObj) },
		DeleteFunc: ns.handleMachineDelete,
	})
	if err := ns.machineWatcher.StartAndCacheSync(); err != nil {
		return fmt.Errorf("Machine watcher error: %w", err)
	}
	return nil
}

// startMachineGonePoller polls the MC API directly to detect when old target
// Machines have been fully deleted. The label-filtered informer drops Machines
// from its store when the label is removed during deletion, before the object
// is actually gone from the API.
func (ns *nthSpotLatency) startMachineGonePoller() {
	ns.machineGoneDone = make(chan struct{})
	dynClient := dynamic.NewForConfigOrDie(ns.mcRestConfig)
	go func() {
		defer close(ns.machineGoneDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		pending := make(map[string]string, len(ns.machineToInstance))
		for name, id := range ns.machineToInstance {
			pending[name] = id
		}
		for {
			select {
			case <-ticker.C:
				for machineName, instanceID := range pending {
					_, err := dynClient.Resource(machineGVR).Namespace(ns.hcpNamespace).Get(
						context.TODO(), machineName, metav1.GetOptions{},
					)
					if err == nil || !apierrors.IsNotFound(err) {
						continue
					}
					value, ok := ns.Metrics.Load(instanceID)
					if !ok {
						continue
					}
					m := value.(nthSpotMetric)
					if m.MachineDeleteTime.IsZero() {
						continue
					}
					if m.MachineGoneTime.IsZero() {
						m.MachineGoneTime = time.Now().UTC()
						ns.Metrics.Store(instanceID, m)
						log.Infof("NTH: %s Machine %s fully deleted from API", instanceID, machineName)
					}
					delete(pending, machineName)
				}
				if len(pending) == 0 {
					return
				}
			case <-ns.doneCh:
				return
			}
		}
	}()
}

// startReplacementReadyPoller periodically lists nodes from the guest cluster API
// and checks if any replacement nodes became Ready without the informer noticing.
// With resyncPeriod=0, the informer can miss events under high churn at scale.
func (ns *nthSpotLatency) startReplacementReadyPoller() {
	ns.replReadyDone = make(chan struct{})
	go func() {
		defer close(ns.replReadyDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		guestClient := dynamic.NewForConfigOrDie(ns.RestConfig)
		mcClient := dynamic.NewForConfigOrDie(ns.mcRestConfig)
		for {
			select {
			case <-ticker.C:
				if int(atomic.LoadInt32(&ns.readyCount)) >= len(ns.instances) {
					return
				}
				ns.pollReplacementNodesReady(guestClient, mcClient)
			case <-ns.doneCh:
				return
			}
		}
	}()
}

func (ns *nthSpotLatency) pollReplacementNodesReady(guestClient, mcClient dynamic.Interface) {
	// Build a set of instanceIDs still waiting for Ready
	pending := make(map[string]string) // instanceID → replMachineName
	ns.instanceToReplMachine.Range(func(key, val any) bool {
		instanceID := key.(string)
		if value, exists := ns.Metrics.Load(instanceID); exists {
			m := value.(nthSpotMetric)
			if m.ReplacementReadyTime.IsZero() {
				pending[instanceID] = val.(string)
			}
		}
		return true
	})
	if len(pending) == 0 {
		return
	}

	// Build nodeRef map from MC API directly (not from cache)
	replNodeToInstance := make(map[string]string) // nodeName → instanceID
	machineList, err := mcClient.Resource(machineGVR).Namespace(ns.hcpNamespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "hypershift.openshift.io/interruptible-instance",
	})
	if err != nil {
		log.Debugf("Replacement poller: error listing Machines: %v", err)
		return
	}
	existingMachines := make(map[string]bool, len(machineList.Items))
	for _, mach := range machineList.Items {
		existingMachines[mach.GetName()] = true
	}
	for _, mach := range machineList.Items {
		machName := mach.GetName()
		for instanceID, replName := range pending {
			if machName != replName {
				continue
			}
			nodeRef, found, _ := unstructured.NestedString(mach.Object, "status", "nodeRef", "name")
			if found && nodeRef != "" {
				replNodeToInstance[nodeRef] = instanceID
			}
		}
	}

	// If a tracked replacement Machine no longer exists, clear the mapping
	// so tryMatchReplacement can pick up the new replacement Machine.
	for instanceID, replName := range pending {
		if !existingMachines[replName] {
			log.Infof("NTH: %s replacement Machine %s no longer exists, clearing mapping for re-match", instanceID, replName)
			ns.instanceToReplMachine.Delete(instanceID)
			ns.matchedReplacements.Delete(replName)
		}
	}

	// List all guest nodes and check Ready condition
	nodeList, err := guestClient.Resource(nodeGVR).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Debugf("Replacement poller: error listing nodes: %v", err)
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
				if value, exists := ns.Metrics.Load(instanceID); exists {
					m := value.(nthSpotMetric)
					if m.ReplacementReadyTime.IsZero() {
						m.ReplacementReadyTime = time.Now().UTC()
						ns.Metrics.Store(instanceID, m)
						count := atomic.AddInt32(&ns.readyCount, 1)
						log.Infof("NTH: %s replacement node %s Ready (%d/%d) [resync]", instanceID, nodeName, count, len(ns.instances))
						if int(count) == len(ns.instances) {
							close(ns.doneCh)
						}
					}
				}
			}
		}
	}
}

func (ns *nthSpotLatency) startNodeWatcher() error {
	dynamicClient := dynamic.NewForConfigOrDie(ns.RestConfig)
	ns.nodeWatcher = watchers.NewWatcher(
		dynamicClient,
		"nthNodeWatcher",
		nodeGVR,
		"",
		nil,
		nil,
	)
	if err := ns.nodeWatcher.Informer.SetTransform(nthNodeTransformFunc()); err != nil {
		log.Warnf("failed to set transform for nthNodeWatcher: %v", err)
	}
	ns.nodeWatcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ns.handleNodeAdd,
		UpdateFunc: func(_, newObj any) { ns.handleNodeUpdate(newObj) },
		DeleteFunc: ns.handleNodeDelete,
	})
	if err := ns.nodeWatcher.StartAndCacheSync(); err != nil {
		return fmt.Errorf("Node watcher error: %w", err)
	}
	return nil
}

func (ns *nthSpotLatency) startPodWatcher() error {
	dynamicClient := dynamic.NewForConfigOrDie(ns.RestConfig)
	ns.podWatcher = watchers.NewWatcher(
		dynamicClient,
		"nthPodWatcher",
		podGVR,
		"",
		nil,
		nil,
	)
	if err := ns.podWatcher.Informer.SetTransform(nthPodTransformFunc()); err != nil {
		log.Warnf("failed to set transform for nthPodWatcher: %v", err)
	}
	ns.podWatcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: ns.handlePodDelete,
	})
	if err := ns.podWatcher.StartAndCacheSync(); err != nil {
		return fmt.Errorf("Pod watcher error: %w", err)
	}
	return nil
}

func (ns *nthSpotLatency) snapshotPodsOnNodes() {
	if ns.podWatcher == nil {
		return
	}
	for _, item := range ns.podWatcher.Informer.GetStore().List() {
		u, ok := item.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		nodeName, _, _ := unstructured.NestedString(u.Object, "spec", "nodeName")
		if _, isTarget := ns.nodeToInstance[nodeName]; !isTarget {
			continue
		}
		if isFilteredPod(u) {
			continue
		}
		ns.atomicAdd(&ns.podsOnNodes, nodeName)
		ns.atomicAdd(&ns.drainablePodsLeft, nodeName)
	}
	ns.podsOnNodes.Range(func(key, val any) bool {
		log.Infof("NTH: %d app pods on spot node %s at injection time", atomic.LoadInt32(val.(*int32)), key)
		return true
	})
}

// startInstanceTerminationPoller polls EC2 to detect when AWS actually terminates
// the spot instances. This distinguishes between node removal driven by NTH/CAPI
// vs node removal caused by AWS killing the instance at the 2-minute mark.
func (ns *nthSpotLatency) startInstanceTerminationPoller() {
	go func() {
		sess, err := awssession.NewSession(&awsconfig.Config{Region: awsconfig.String(ns.region)})
		if err != nil {
			log.Warnf("Instance termination poller: failed to create AWS session: %v", err)
			return
		}
		ec2Client := ec2svc.New(sess)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		ids := make([]*string, 0, len(ns.instances))
		for _, inst := range ns.instances {
			ids = append(ids, awsconfig.String(inst.InstanceID))
		}
		pending := make(map[string]bool, len(ns.instances))
		for _, inst := range ns.instances {
			pending[inst.InstanceID] = true
		}

		for {
			select {
			case <-ticker.C:
				out, err := ec2Client.DescribeInstances(&ec2svc.DescribeInstancesInput{
					InstanceIds: ids,
				})
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
							if value, exists := ns.Metrics.Load(id); exists {
								m := value.(nthSpotMetric)
								if m.InstanceTerminatedTime.IsZero() {
									if m.Timestamp.IsZero() {
										log.Warnf("NTH: %s instance terminated (state=%s) before batch triggered — likely AWS spot reclaim", id, state)
										delete(pending, id)
										continue
									}
									m.InstanceTerminatedTime = now
									ns.Metrics.Store(id, m)
									log.Infof("NTH: %s instance terminated (state=%s, %vs after FIS trigger)",
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
			case <-ns.doneCh:
				return
			}
		}
	}()
}

func (ns *nthSpotLatency) handlePodDelete(obj any) {
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
	if _, isTarget := ns.nodeToInstance[nodeName]; !isTarget {
		return
	}
	if _, tainted := ns.taintedNodes.Load(nodeName); !tainted {
		return
	}
	if isFilteredPod(u) {
		return
	}

	// Decrement drainable counter; record drain complete when 0.
	// Skip once node is removed — those are GC events, not drain.
	if _, nodeGone := ns.deletedNodes.Load(nodeName); !nodeGone {
		if val, ok := ns.drainablePodsLeft.Load(nodeName); ok {
			remaining := atomic.AddInt32(val.(*int32), -1)
			if remaining == 0 {
				instanceID := ns.nodeToInstance[nodeName]
				if value, exists := ns.Metrics.Load(instanceID); exists {
					m := value.(nthSpotMetric)
					if m.DrainCompleteTime.IsZero() {
						m.DrainCompleteTime = time.Now().UTC()
						ns.Metrics.Store(instanceID, m)
						log.Infof("NTH: %s drain complete on node %s", instanceID, nodeName)
					}
				}
			}
		}
	}

}

func (ns *nthSpotLatency) atomicAdd(m *sync.Map, key string) {
	val, loaded := m.LoadOrStore(key, new(int32))
	if !loaded {
		atomic.StoreInt32(val.(*int32), 1)
	} else {
		atomic.AddInt32(val.(*int32), 1)
	}
}

// handleMachineAdd detects new replacement Machines (not in pre-existing set).
func (ns *nthSpotLatency) handleMachineAdd(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	name := u.GetName()
	if _, preExisting := ns.preExistingMachines.Load(name); preExisting {
		return
	}
	ns.tryMatchReplacement(u)
}

// handleMachineUpdate detects deletionTimestamp (T3) and replacement Machines reaching Running (T6).
func (ns *nthSpotLatency) handleMachineUpdate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	name := u.GetName()

	// Check if this is a target Machine getting a deletionTimestamp (T3)
	if instanceID, ok := ns.machineToInstance[name]; ok {
		delTS, found, _ := unstructured.NestedString(u.Object, "metadata", "deletionTimestamp")
		if found && delTS != "" {
			value, exists := ns.Metrics.Load(instanceID)
			if exists {
				m := value.(nthSpotMetric)
				if m.MachineDeleteTime.IsZero() {
					t, err := time.Parse(time.RFC3339, delTS)
					if err == nil {
						m.MachineDeleteTime = t.UTC()
						ns.Metrics.Store(instanceID, m)
						log.Infof("NTH: %s Machine %s deletionTimestamp=%s", instanceID, name, delTS)
					}
				}
			}
		}
		return
	}

	// Check if this is a replacement Machine reaching Running (T6)
	if _, preExisting := ns.preExistingMachines.Load(name); !preExisting {
		ns.tryMatchReplacement(u)
	}
}

// handleMachineDelete detects Machine fully removed (T4).
func (ns *nthSpotLatency) handleMachineDelete(obj any) {
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
	instanceID, ok := ns.machineToInstance[name]
	if !ok {
		return
	}
	value, exists := ns.Metrics.Load(instanceID)
	if !exists {
		return
	}
	m := value.(nthSpotMetric)
	if m.MachineGoneTime.IsZero() {
		m.MachineGoneTime = time.Now().UTC()
		ns.Metrics.Store(instanceID, m)
		log.Infof("NTH: %s Machine %s fully deleted", instanceID, name)
	}
}

// tryMatchReplacement checks if a new Machine belongs to the same NodePool as a
// deleted target and has reached Running phase.
func (ns *nthSpotLatency) tryMatchReplacement(u *unstructured.Unstructured) {
	name := u.GetName()
	if _, alreadyMatched := ns.matchedReplacements.Load(name); alreadyMatched {
		return
	}

	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	if phase != "Running" {
		return
	}

	for _, inst := range ns.instances {
		// Skip if this instance already has a replacement
		if _, hasRepl := ns.instanceToReplMachine.Load(inst.InstanceID); hasRepl {
			continue
		}
		// Match by NodePool prefix: original machine name minus the last random suffix
		origPrefix := machineNamePrefix(inst.MachineName)
		if strings.HasPrefix(name, origPrefix) {
			now := time.Now().UTC()
			ns.matchedReplacements.Store(name, inst.InstanceID)
			ns.instanceToReplMachine.Store(inst.InstanceID, name)

			value, exists := ns.Metrics.Load(inst.InstanceID)
			if exists {
				m := value.(nthSpotMetric)
				m.ReplacementRunningTime = now
				ns.Metrics.Store(inst.InstanceID, m)
			}

			replNode, _, _ := unstructured.NestedString(u.Object, "status", "nodeRef", "name")
			log.Infof("NTH: %s replacement Machine %s Running → node=%s", inst.InstanceID, name, replNode)
			break
		}
	}
}

// handleNodeUpdate detects NTH taint (T1) and cordon (T1.5) on target nodes,
// and Ready condition on replacement nodes (T7).
func (ns *nthSpotLatency) handleNodeUpdate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	nodeName := u.GetName()

	// Check if this is a target node being tainted/cordoned
	if instanceID, isTarget := ns.nodeToInstance[nodeName]; isTarget {
		ns.checkTargetNodeSignals(u, instanceID)
		return
	}

	// Check if this is a replacement node reaching Ready (T7)
	ns.checkReplacementNodeReady(u)
}

func (ns *nthSpotLatency) handleNodeAdd(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ns.checkReplacementNodeReady(u)
}

// handleNodeDelete detects target node removal from guest cluster (T5).
func (ns *nthSpotLatency) handleNodeDelete(obj any) {
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
	instanceID, ok := ns.nodeToInstance[nodeName]
	if !ok {
		return
	}
	value, exists := ns.Metrics.Load(instanceID)
	if !exists {
		return
	}
	m := value.(nthSpotMetric)
	if m.NodeDeleteTime.IsZero() {
		m.NodeDeleteTime = time.Now().UTC()
		ns.deletedNodes.Store(nodeName, struct{}{})
		log.Infof("NTH: %s node %s removed from guest cluster", instanceID, nodeName)
		ns.Metrics.Store(instanceID, m)
	}
}

func (ns *nthSpotLatency) checkTargetNodeSignals(u *unstructured.Unstructured, instanceID string) {
	value, exists := ns.Metrics.Load(instanceID)
	if !exists {
		return
	}
	m := value.(nthSpotMetric)
	updated := false

	// Check for NTH taint (T1)
	if m.TaintTime.IsZero() {
		taints, found, _ := unstructured.NestedSlice(u.Object, "spec", "taints")
		if found {
			for _, tRaw := range taints {
				t, ok := tRaw.(map[string]any)
				if !ok {
					continue
				}
				key, _ := t["key"].(string)
				if strings.HasPrefix(key, nthTaintKeyPrefix) {
					m.TaintTime = time.Now().UTC()
					updated = true
					ns.taintedNodes.Store(u.GetName(), struct{}{})
					log.Infof("NTH: %s node %s tainted (%s)", instanceID, u.GetName(), key)
					break
				}
			}
		}
	}

	// Check for cordon (spec.unschedulable)
	if m.CordonTime.IsZero() {
		unschedulable, found, _ := unstructured.NestedBool(u.Object, "spec", "unschedulable")
		if found && unschedulable {
			m.CordonTime = time.Now().UTC()
			updated = true
			log.Infof("NTH: %s node %s cordoned", instanceID, u.GetName())
		}
	}

	if updated {
		ns.Metrics.Store(instanceID, m)
	}
}

func (ns *nthSpotLatency) checkReplacementNodeReady(u *unstructured.Unstructured) {
	nodeName := u.GetName()

	// Find which instance this replacement node belongs to by checking
	// all instances that have a replacement Machine with a matching nodeRef
	ns.instanceToReplMachine.Range(func(key, val any) bool {
		instanceID := key.(string)
		replMachineName := val.(string)

		value, exists := ns.Metrics.Load(instanceID)
		if !exists {
			return true
		}
		m := value.(nthSpotMetric)
		if !m.ReplacementReadyTime.IsZero() {
			return true
		}

		// Check if this node is the replacement's nodeRef by looking at the machine watcher's cache
		if ns.machineWatcher != nil {
			items := ns.machineWatcher.Informer.GetStore().List()
			for _, item := range items {
				mach, ok := item.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				if mach.GetName() != replMachineName {
					continue
				}
				nodeRef, found, _ := unstructured.NestedString(mach.Object, "status", "nodeRef", "name")
				if found && nodeRef == nodeName {
					// This node belongs to the replacement Machine — check Ready condition
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
								ns.Metrics.Store(instanceID, m)
								count := atomic.AddInt32(&ns.readyCount, 1)
								log.Infof("NTH: %s replacement node %s Ready (%d/%d)", instanceID, nodeName, count, len(ns.instances))
								if int(count) == len(ns.instances) {
									close(ns.doneCh)
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

func (ns *nthSpotLatency) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
}

func (ns *nthSpotLatency) Stop() error {
	// Block until all replacements are Ready or timeout.
	// With metricsClosing=afterMeasurements, the Prometheus scrape window
	// extends until Stop() returns, covering the full observation period.
	select {
	case <-ns.doneCh:
		log.Infof("NTH: all %d replacements Ready", len(ns.instances))
	case <-time.After(ns.timeout):
		log.Warnf("NTH: timeout after %v, %d/%d replacements Ready", ns.timeout, atomic.LoadInt32(&ns.readyCount), len(ns.instances))
	}

	if ns.sqsCancel != nil {
		ns.sqsCancel()
	}
	if ns.machineWatcher != nil {
		ns.machineWatcher.StopWatcher()
	}
	if ns.nodeWatcher != nil {
		ns.nodeWatcher.StopWatcher()
	}
	if ns.podWatcher != nil {
		ns.podWatcher.StopWatcher()
	}

	// Clean up FIS experiment templates
	CleanupFISTemplates(ns.region, ns.fisTemplateIDs)

	// Log SQS queue summary
	ns.sqsMu.Lock()
	snapshots := ns.sqsSnapshots
	ns.sqsMu.Unlock()
	var maxBacklog, maxInFlight int
	for _, s := range snapshots {
		if s.MessagesAvailable > maxBacklog {
			maxBacklog = s.MessagesAvailable
		}
		if s.MessagesInFlight > maxInFlight {
			maxInFlight = s.MessagesInFlight
		}
	}
	log.Infof("NTH: SQS queue stats: %d snapshots, peak backlog=%d, peak in-flight=%d", len(snapshots), maxBacklog, maxInFlight)

	// Index SQS snapshots
	ns.indexSQSSnapshots(snapshots)

	return ns.StopMeasurement(ns.normalizeLatencies, ns.getLatency)
}

func (ns *nthSpotLatency) normalizeLatencies() float64 {
	ns.Metrics.Range(func(key, value any) bool {
		m := value.(nthSpotMetric)
		if m.Timestamp.IsZero() {
			return true
		}
		if !m.CordonTime.IsZero() {
			m.InterruptionToCordonLatency = int(m.CordonTime.Sub(m.Timestamp).Milliseconds())
		}
		// If drain didn't complete before node removal, use NodeDeleteTime
		if m.DrainCompleteTime.IsZero() && !m.NodeDeleteTime.IsZero() {
			m.DrainCompleteTime = m.NodeDeleteTime
		}
		if !m.CordonTime.IsZero() && !m.DrainCompleteTime.IsZero() {
			m.CordonToDrainCompleteLatency = int(m.DrainCompleteTime.Sub(m.CordonTime).Milliseconds())
		}
		if !m.NodeDeleteTime.IsZero() {
			m.InterruptionToNodeDeleteLatency = int(m.NodeDeleteTime.Sub(m.Timestamp).Milliseconds())
		}
		if !m.InstanceTerminatedTime.IsZero() {
			m.InterruptionToInstanceTerminatedLatency = int(m.InstanceTerminatedTime.Sub(m.Timestamp).Milliseconds())
		}
		if !m.DrainCompleteTime.IsZero() && !m.MachineGoneTime.IsZero() {
			lat := int(m.MachineGoneTime.Sub(m.DrainCompleteTime).Milliseconds())
			if lat < 0 {
				lat = 0
			}
			m.DrainCompleteToMachineDeleteLatency = lat
		}
		if !m.MachineGoneTime.IsZero() && !m.ReplacementReadyTime.IsZero() {
			m.MachineDeleteToReadyLatency = int(m.ReplacementReadyTime.Sub(m.MachineGoneTime).Milliseconds())
		}
		if !m.ReplacementReadyTime.IsZero() {
			m.EndToEndLatency = int(m.ReplacementReadyTime.Sub(m.Timestamp).Milliseconds())
		}
		if val, ok := ns.podsOnNodes.Load(m.NodeName); ok {
			m.PodsOnNode = int(atomic.LoadInt32(val.(*int32)))
		}
		ns.NormLatencies = append(ns.NormLatencies, m)
		return true
	})
	return 0
}

func (ns *nthSpotLatency) getLatency(normLatency any) map[string]float64 {
	m := normLatency.(nthSpotMetric)
	return map[string]float64{
		"InterruptionToCordon":         float64(m.InterruptionToCordonLatency),
		"CordonToDrainComplete":        float64(m.CordonToDrainCompleteLatency),
		"InterruptionToNodeDelete":             float64(m.InterruptionToNodeDeleteLatency),
		"InterruptionToInstanceTerminated":     float64(m.InterruptionToInstanceTerminatedLatency),
		"DrainCompleteToMachineDelete": float64(m.DrainCompleteToMachineDeleteLatency),
		"MachineDeleteToReady":         float64(m.MachineDeleteToReadyLatency),
		"EndToEnd":                     float64(m.EndToEndLatency),
	}
}

func (ns *nthSpotLatency) Index(jobName string, indexerList map[string]indexers.Indexer) {
	ns.BaseMeasurement.Index(jobName, indexerList)
	ns.sqsMu.Lock()
	snapshots := ns.sqsSnapshots
	ns.sqsMu.Unlock()
	if len(snapshots) == 0 {
		return
	}
	sqsDocs := make([]any, 0, len(snapshots))
	for _, s := range snapshots {
		sqsDocs = append(sqsDocs, sqsSnapshotMetric{
			Timestamp:         s.Timestamp,
			MessagesAvailable: s.MessagesAvailable,
			MessagesInFlight:  s.MessagesInFlight,
			MetricName:        "nthSpotSQSMeasurement",
			UUID:              ns.Uuid,
			JobName:           ns.JobConfig.Name,
			Metadata:          ns.Metadata,
		})
	}
	metricName := fmt.Sprintf("nthSpotSQSMeasurement-%s", jobName)
	for _, indexer := range indexerList {
		log.Infof("Indexing metric nthSpotSQSMeasurement")
		resp, err := indexer.Index(sqsDocs, indexers.IndexingOpts{MetricName: metricName})
		if err != nil {
			log.Errorf("Error indexing nthSpotSQSMeasurement: %v", err)
		} else {
			log.Info(resp)
		}
	}
}

func (ns *nthSpotLatency) indexSQSSnapshots(snapshots []SQSQueueSnapshot) {
	if len(snapshots) == 0 {
		return
	}
	log.Infof("NTH: collected %d SQS queue snapshots for indexing", len(snapshots))
}

func (ns *nthSpotLatency) IsCompatible() bool {
	return true
}

// machineNamePrefix strips the last hyphen-separated segment (random suffix) from
// a Machine name to get the MachineSet/NodePool prefix for replacement matching.
func machineNamePrefix(name string) string {
	idx := strings.LastIndex(name, "-")
	if idx < 0 {
		return name
	}
	return name[:idx+1]
}

// nthMachineTransformFunc preserves fields needed for NTH tracking:
// metadata: name, uid, creationTimestamp, labels, deletionTimestamp
// status: phase, nodeRef, conditions
// spec: providerID
func nthMachineTransformFunc() cache.TransformFunc {
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
		if phase, found, _ := unstructured.NestedString(u.Object, "status", "phase"); found {
			_ = unstructured.SetNestedField(minimal.Object, phase, "status", "phase")
		}
		if nodeRef, found, _ := unstructured.NestedMap(u.Object, "status", "nodeRef"); found {
			_ = unstructured.SetNestedField(minimal.Object, nodeRef, "status", "nodeRef")
		}
		if conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); found {
			_ = unstructured.SetNestedSlice(minimal.Object, conditions, "status", "conditions")
		}
		if providerID, found, _ := unstructured.NestedString(u.Object, "spec", "providerID"); found {
			_ = unstructured.SetNestedField(minimal.Object, providerID, "spec", "providerID")
		}
		return minimal, nil
	}
}

// nthNodeTransformFunc preserves fields needed for taint/cordon/Ready detection:
// metadata: name, uid, creationTimestamp
// spec: taints, unschedulable
// status: conditions
func nthNodeTransformFunc() cache.TransformFunc {
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

func nthPodTransformFunc() cache.TransformFunc {
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

func podOwnerKind(u *unstructured.Unstructured) string {
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

func isDaemonSetPod(u *unstructured.Unstructured) bool {
	return podOwnerKind(u) == "DaemonSet"
}

func isInfraPoxyPod(u *unstructured.Unstructured) bool {
	name := u.GetName()
	return strings.HasPrefix(name, "kube-apiserver-proxy-") || strings.HasPrefix(name, "kube-rbac-proxy-")
}

func isFilteredPod(u *unstructured.Unstructured) bool {
	return isDaemonSetPod(u) || isInfraPoxyPod(u)
}

