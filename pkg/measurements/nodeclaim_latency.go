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
	"sync"
	"time"

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
	nodeClaimLatencyMeasurement          = "nodeClaimLatencyMeasurement"
	nodeClaimLatencyQuantilesMeasurement = "nodeClaimLatencyQuantilesMeasurement"
)

var nodeClaimGVR = schema.GroupVersionResource{
	Group:    "karpenter.sh",
	Version:  "v1",
	Resource: "nodeclaims",
}

// NodeClaim condition types tracked for latency
const (
	nodeClaimLaunched    = "Launched"
	nodeClaimRegistered  = "Registered"
	nodeClaimInitialized = "Initialized"
	nodeClaimReady       = "Ready"
)

type nodeClaimMetric struct {
	Timestamp            time.Time         `json:"timestamp"`
	Launched             time.Time         `json:"-"`
	LaunchedLatency      int               `json:"launchedLatency"`
	Registered           time.Time         `json:"-"`
	RegisteredLatency    int               `json:"registeredLatency"`
	Initialized          time.Time         `json:"-"`
	InitializedLatency   int               `json:"initializedLatency"`
	Ready                time.Time         `json:"-"`
	ReadyLatency         int               `json:"readyLatency"`
	MetricName           string            `json:"metricName"`
	UUID                 string            `json:"uuid"`
	JobName              string            `json:"jobName,omitempty"`
	Name                 string            `json:"nodeClaimName"`
	NodeName             string            `json:"nodeName,omitempty"`
	Labels               map[string]string `json:"labels"`
	Metadata             any               `json:"metadata,omitempty"`
}

type nodeClaimLatency struct {
	measurements.BaseMeasurement
	watcher *watchers.Watcher
}

type nodeClaimLatencyFactory struct {
	measurements.BaseMeasurementFactory
}

// NewNodeClaimLatencyFactory creates a nodeClaimLatency measurement factory that watches
// NodeClaim resources (karpenter.sh/v1) with the "autonode=true" label selector.
func NewNodeClaimLatencyFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, _ string) (measurements.MeasurementFactory, error) {
	return nodeClaimLatencyFactory{
		BaseMeasurementFactory: measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, "autonode=true"),
	}, nil
}

func (f nodeClaimLatencyFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &nodeClaimLatency{
		BaseMeasurement: f.NewBaseLatency(jobConfig, clientSet, restConfig, nodeClaimLatencyMeasurement, nodeClaimLatencyQuantilesMeasurement, embedCfg),
	}
}

func (nc *nodeClaimLatency) handleCreate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Error("failed to convert to Unstructured in nodeClaimLatency handleCreate")
		return
	}
	name := u.GetName()
	uid := string(u.GetUID())
	nc.Metrics.LoadOrStore(uid, nodeClaimMetric{
		Timestamp:  u.GetCreationTimestamp().UTC(),
		Name:       name,
		MetricName: nodeClaimLatencyMeasurement,
		UUID:       nc.Uuid,
		JobName:    nc.JobConfig.Name,
		Labels:     normalizeLabels(u.GetLabels()),
		Metadata:   nc.Metadata,
	})
	log.Debugf("NodeClaim %s created at %v (uid=%s)", name, u.GetCreationTimestamp().UTC(), uid)
}

func (nc *nodeClaimLatency) handleUpdate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Error("failed to convert to Unstructured in nodeClaimLatency handleUpdate")
		return
	}
	uid := string(u.GetUID())
	value, exists := nc.Metrics.Load(uid)
	if !exists {
		return
	}
	m := value.(nodeClaimMetric)

	// Extract nodeName from status
	if nodeName, found, _ := unstructured.NestedString(u.Object, "status", "nodeName"); found && nodeName != "" {
		m.NodeName = nodeName
	}

	// Extract conditions from status
	conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return
	}
	for _, cRaw := range conditions {
		c, ok := cRaw.(map[string]any)
		if !ok {
			continue
		}
		condType, _ := c["type"].(string)
		status, _ := c["status"].(string)
		if status != "True" {
			continue
		}
		tsStr, _ := c["lastTransitionTime"].(string)
		if tsStr == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, tsStr)
		if err != nil {
			continue
		}
		switch condType {
		case nodeClaimLaunched:
			m.Launched = t.UTC()
		case nodeClaimRegistered:
			m.Registered = t.UTC()
		case nodeClaimInitialized:
			m.Initialized = t.UTC()
		case nodeClaimReady:
			log.Debugf("NodeClaim %s is ready", m.Name)
			m.Ready = t.UTC()
		}
	}
	nc.Metrics.Store(uid, m)
}

func (nc *nodeClaimLatency) Start(measurementWg *sync.WaitGroup) error {
	nc.LatencyQuantiles, nc.NormLatencies = nil, nil
	nc.Metrics = sync.Map{}
	defer measurementWg.Done()

	// Pre-populate existing NodeClaims
	nc.collectExisting()

	log.Infof("Creating NodeClaim latency watcher for %s with selector %s", nc.JobConfig.Name, nc.LabelSelector)
	dynamicClient := dynamic.NewForConfigOrDie(nc.RestConfig)
	nc.watcher = watchers.NewWatcher(
		dynamicClient,
		"nodeClaimWatcher",
		nodeClaimGVR,
		metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			if nc.LabelSelector != "" {
				options.LabelSelector = nc.LabelSelector
			}
		},
		nil,
	)
	if err := nc.watcher.Informer.SetTransform(nodeClaimTransformFunc()); err != nil {
		log.Warnf("failed to set transform for nodeClaimWatcher: %v", err)
	}
	nc.watcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: nc.handleCreate,
		UpdateFunc: func(_, newObj any) {
			nc.handleUpdate(newObj)
		},
	})
	if err := nc.watcher.StartAndCacheSync(); err != nil {
		log.Errorf("NodeClaim latency measurement error: %v", err)
	}
	return nil
}

func (nc *nodeClaimLatency) collectExisting() {
	dynamicClient := dynamic.NewForConfigOrDie(nc.RestConfig)
	listOpts := metav1.ListOptions{}
	if nc.LabelSelector != "" {
		listOpts.LabelSelector = nc.LabelSelector
	}
	list, err := dynamicClient.Resource(nodeClaimGVR).List(context.TODO(), listOpts)
	if err != nil {
		log.Errorf("error listing NodeClaims: %v", err)
		return
	}
	for _, item := range list.Items {
		m := nodeClaimMetric{
			Timestamp:  item.GetCreationTimestamp().UTC(),
			Name:       item.GetName(),
			MetricName: nodeClaimLatencyMeasurement,
			UUID:       nc.Uuid,
			JobName:    nc.JobConfig.Name,
			Labels:     normalizeLabels(item.GetLabels()),
			Metadata:   nc.Metadata,
		}
		// Extract nodeName
		if nodeName, found, _ := unstructured.NestedString(item.Object, "status", "nodeName"); found {
			m.NodeName = nodeName
		}
		// Extract conditions
		conditions, found, _ := unstructured.NestedSlice(item.Object, "status", "conditions")
		if found {
			for _, cRaw := range conditions {
				c, ok := cRaw.(map[string]any)
				if !ok {
					continue
				}
				condType, _ := c["type"].(string)
				status, _ := c["status"].(string)
				if status != "True" {
					continue
				}
				tsStr, _ := c["lastTransitionTime"].(string)
				if tsStr == "" {
					continue
				}
				t, err := time.Parse(time.RFC3339, tsStr)
				if err != nil {
					continue
				}
				switch condType {
				case nodeClaimLaunched:
					m.Launched = t.UTC()
				case nodeClaimRegistered:
					m.Registered = t.UTC()
				case nodeClaimInitialized:
					m.Initialized = t.UTC()
				case nodeClaimReady:
					m.Ready = t.UTC()
				}
			}
		}
		nc.Metrics.Store(string(item.GetUID()), m)
	}
}

func (nc *nodeClaimLatency) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
	// NodeClaims are transitory and may be deleted before Collect runs,
	// so we merge API data into existing watcher data rather than replacing it.
	dynamicClient := dynamic.NewForConfigOrDie(nc.RestConfig)
	listOpts := metav1.ListOptions{}
	if nc.LabelSelector != "" {
		listOpts.LabelSelector = nc.LabelSelector
	}
	list, err := dynamicClient.Resource(nodeClaimGVR).List(context.TODO(), listOpts)
	if err != nil {
		log.Debugf("error listing NodeClaims during collect (may already be deleted): %v", err)
		return
	}
	log.Debugf("NodeClaim Collect: found %d NodeClaims from API", len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		uid := string(item.GetUID())
		m := nodeClaimMetric{
			Timestamp:  item.GetCreationTimestamp().UTC(),
			Name:       item.GetName(),
			MetricName: nodeClaimLatencyMeasurement,
			UUID:       nc.Uuid,
			JobName:    nc.JobConfig.Name,
			Labels:     normalizeLabels(item.GetLabels()),
			Metadata:   nc.Metadata,
		}
		if nodeName, found, _ := unstructured.NestedString(item.Object, "status", "nodeName"); found {
			m.NodeName = nodeName
		}
		conditions, found, _ := unstructured.NestedSlice(item.Object, "status", "conditions")
		if found {
			for _, cRaw := range conditions {
				c, ok := cRaw.(map[string]any)
				if !ok {
					continue
				}
				condType, _ := c["type"].(string)
				status, _ := c["status"].(string)
				if status != "True" {
					continue
				}
				tsStr, _ := c["lastTransitionTime"].(string)
				if tsStr == "" {
					continue
				}
				t, err := time.Parse(time.RFC3339, tsStr)
				if err != nil {
					continue
				}
				switch condType {
				case nodeClaimLaunched:
					m.Launched = t.UTC()
				case nodeClaimRegistered:
					m.Registered = t.UTC()
				case nodeClaimInitialized:
					m.Initialized = t.UTC()
				case nodeClaimReady:
					m.Ready = t.UTC()
				}
			}
		}
		nc.Metrics.Store(uid, m)
	}
}

func (nc *nodeClaimLatency) Stop() error {
	if nc.watcher != nil {
		nc.watcher.StopWatcher()
	}
	return nc.StopMeasurement(nc.normalizeLatencies, nc.getLatency)
}

func (nc *nodeClaimLatency) normalizeLatencies() float64 {
	nc.Metrics.Range(func(key, value any) bool {
		m := value.(nodeClaimMetric)
		if m.Ready.IsZero() {
			log.Tracef("NodeClaim %v latency ignored as it did not reach Ready state", m.Name)
			return true
		}
		m.LaunchedLatency = int(m.Launched.Sub(m.Timestamp).Milliseconds())
		m.RegisteredLatency = int(m.Registered.Sub(m.Timestamp).Milliseconds())
		m.InitializedLatency = int(m.Initialized.Sub(m.Timestamp).Milliseconds())
		m.ReadyLatency = int(m.Ready.Sub(m.Timestamp).Milliseconds())
		nc.NormLatencies = append(nc.NormLatencies, m)
		return true
	})
	return 0
}

func (nc *nodeClaimLatency) getLatency(normLatency any) map[string]float64 {
	m := normLatency.(nodeClaimMetric)
	return map[string]float64{
		nodeClaimLaunched:    float64(m.LaunchedLatency),
		nodeClaimRegistered:  float64(m.RegisteredLatency),
		nodeClaimInitialized: float64(m.InitializedLatency),
		nodeClaimReady:       float64(m.ReadyLatency),
	}
}

func (nc *nodeClaimLatency) IsCompatible() bool {
	return true
}

// normalizeLabels converts nil map to empty map
func normalizeLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{}
	}
	return labels
}

// nodeClaimTransformFunc preserves only the fields needed for latency measurements:
// - metadata: name, uid, creationTimestamp, labels
// - status: conditions, nodeName
func nodeClaimTransformFunc() cache.TransformFunc {
	return func(obj any) (any, error) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return obj, nil
		}
		metadata := map[string]any{
			"name":              u.GetName(),
			"uid":               string(u.GetUID()),
			"creationTimestamp": u.GetCreationTimestamp().Format(time.RFC3339),
			"labels":            u.GetLabels(),
		}
		minimal := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": u.GetAPIVersion(),
				"kind":       u.GetKind(),
				"metadata":   metadata,
			},
		}
		if conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); found {
			_ = unstructured.SetNestedSlice(minimal.Object, conditions, "status", "conditions")
		}
		if nodeName, found, _ := unstructured.NestedString(u.Object, "status", "nodeName"); found {
			_ = unstructured.SetNestedField(minimal.Object, nodeName, "status", "nodeName")
		}
		return minimal, nil
	}
}
