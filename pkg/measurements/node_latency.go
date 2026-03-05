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
	"sync"
	"time"

	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/measurements/types"
	"github.com/kube-burner/kube-burner/v2/pkg/util"
	"github.com/kube-burner/kube-burner/v2/pkg/util/fileutils"
	"github.com/kube-burner/kube-burner/v2/pkg/watchers"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	nodeLatencyMeasurement          = "nodeLatencyMeasurement"
	nodeLatencyQuantilesMeasurement = "nodeLatencyQuantilesMeasurement"
)

type autoNodeLatency struct {
	measurements.BaseMeasurement
	watcher *watchers.Watcher
}

type autoNodeLatencyFactory struct {
	measurements.BaseMeasurementFactory
}

// NewAutoNodeLatencyFactory creates a nodeLatency measurement factory that watches nodes
// with the "autonode=true" label instead of the kube-burner.io/runid label used by the built-in
// nodeLatency watcher. This is needed because Karpenter-provisioned nodes don't carry kube-burner labels.
func NewAutoNodeLatencyFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, _ string) (measurements.MeasurementFactory, error) {
	return autoNodeLatencyFactory{
		BaseMeasurementFactory: measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, "autonode=true"),
	}, nil
}

func (f autoNodeLatencyFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &autoNodeLatency{
		BaseMeasurement: f.NewBaseLatency(jobConfig, clientSet, restConfig, nodeLatencyMeasurement, nodeLatencyQuantilesMeasurement, embedCfg),
	}
}

func (n *autoNodeLatency) handleCreateNode(obj any) {
	node, err := util.ConvertAnyToTyped[corev1.Node](obj)
	if err != nil {
		log.Errorf("failed to convert to Node: %v", err)
		return
	}
	n.Metrics.LoadOrStore(string(node.UID), measurements.NodeMetric{
		Timestamp:  node.CreationTimestamp.UTC(),
		Name:       node.Name,
		MetricName: nodeLatencyMeasurement,
		UUID:       n.Uuid,
		JobName:    n.JobConfig.Name,
		Labels:     util.NormalizeLabels(node.Labels),
		Metadata:   n.Metadata,
	})
}

func (n *autoNodeLatency) handleUpdateNode(obj any) {
	node, err := util.ConvertAnyToTyped[corev1.Node](obj)
	if err != nil {
		log.Errorf("failed to convert to Node: %v", err)
		return
	}
	if value, exists := n.Metrics.Load(string(node.UID)); exists {
		nm := value.(measurements.NodeMetric)
		for _, c := range node.Status.Conditions {
			switch c.Type {
			case corev1.NodeMemoryPressure:
				if c.Status == corev1.ConditionFalse {
					nm.NodeMemoryPressure = c.LastTransitionTime.UTC()
				}
			case corev1.NodeDiskPressure:
				if c.Status == corev1.ConditionFalse {
					nm.NodeDiskPressure = c.LastTransitionTime.UTC()
				}
			case corev1.NodePIDPressure:
				if c.Status == corev1.ConditionFalse {
					nm.NodePIDPressure = c.LastTransitionTime.UTC()
				}
			case corev1.NodeReady:
				if c.Status == corev1.ConditionTrue {
					log.Debugf("Node %s is ready", node.Name)
					nm.NodeReady = c.LastTransitionTime.UTC()
				}
			}
		}
		n.Metrics.Store(string(node.UID), nm)
	}
}

func (n *autoNodeLatency) Start(measurementWg *sync.WaitGroup) error {
	n.LatencyQuantiles, n.NormLatencies = nil, nil
	defer measurementWg.Done()
	var wg sync.WaitGroup
	wg.Add(1)
	n.Collect(&wg)
	wg.Wait()
	gvr, err := util.ResourceToGVR(n.RestConfig, "Node", "v1")
	if err != nil {
		return fmt.Errorf("error getting GVR for Node: %w", err)
	}
	log.Infof("Creating autonode latency watcher for %s with selector %s", n.JobConfig.Name, n.LabelSelector)
	n.watcher = watchers.NewWatcher(
		dynamic.NewForConfigOrDie(n.RestConfig),
		"nodeWatcher",
		gvr,
		corev1.NamespaceAll,
		func(options *metav1.ListOptions) {
			if n.LabelSelector != "" {
				options.LabelSelector = n.LabelSelector
			}
		},
		nil,
	)
	if err := n.watcher.Informer.SetTransform(nodeTransformFunc()); err != nil {
		log.Warnf("failed to set transform for nodeWatcher: %v", err)
	}
	n.watcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: n.handleCreateNode,
		UpdateFunc: func(_, newObj any) {
			n.handleUpdateNode(newObj)
		},
	})
	if err := n.watcher.StartAndCacheSync(); err != nil {
		log.Errorf("Node latency measurement error: %v", err)
	}
	return nil
}

func (n *autoNodeLatency) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
	var nodes []corev1.Node
	nodeList, err := n.ClientSet.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{LabelSelector: n.LabelSelector})
	if err != nil {
		log.Errorf("error listing nodes: %v", err)
	}
	nodes = append(nodes, nodeList.Items...)

	n.Metrics = sync.Map{}
	for _, node := range nodes {
		var nodeMemoryPressure, nodeDiskPressure, nodePIDPressure, nodeReady time.Time
		for _, c := range node.Status.Conditions {
			switch c.Type {
			case corev1.NodeMemoryPressure:
				nodeMemoryPressure = c.LastTransitionTime.UTC()
			case corev1.NodeDiskPressure:
				nodeDiskPressure = c.LastTransitionTime.UTC()
			case corev1.NodePIDPressure:
				nodePIDPressure = c.LastTransitionTime.UTC()
			case corev1.NodeReady:
				nodeReady = c.LastTransitionTime.UTC()
			}
		}
		n.Metrics.Store(string(node.UID), measurements.NodeMetric{
			Timestamp:          node.CreationTimestamp.UTC(),
			Name:               node.Name,
			MetricName:         nodeLatencyMeasurement,
			UUID:               n.Uuid,
			NodeMemoryPressure: nodeMemoryPressure,
			NodeDiskPressure:   nodeDiskPressure,
			NodePIDPressure:    nodePIDPressure,
			NodeReady:          nodeReady,
			JobName:            n.JobConfig.Name,
			Labels:             util.NormalizeLabels(node.Labels),
		})
	}
}

func (n *autoNodeLatency) Stop() error {
	if n.watcher != nil {
		n.watcher.StopWatcher()
	}
	return n.StopMeasurement(n.normalizeLatencies, n.getLatency)
}

func (n *autoNodeLatency) normalizeLatencies() float64 {
	n.Metrics.Range(func(key, value any) bool {
		m := value.(measurements.NodeMetric)
		if m.NodeReady.IsZero() {
			log.Tracef("Node %v latency ignored as it did not reach Ready state", m.Name)
			return true
		}
		earliest := m.Timestamp
		if m.NodeMemoryPressure.Before(earliest) {
			earliest = m.NodeMemoryPressure
		}
		if m.NodeDiskPressure.Before(earliest) {
			earliest = m.NodeDiskPressure
		}
		if m.NodePIDPressure.Before(earliest) {
			earliest = m.NodePIDPressure
		}
		m.NodeMemoryPressureLatency = int(m.NodeMemoryPressure.Sub(earliest).Milliseconds())
		m.NodeDiskPressureLatency = int(m.NodeDiskPressure.Sub(earliest).Milliseconds())
		m.NodePIDPressureLatency = int(m.NodePIDPressure.Sub(earliest).Milliseconds())
		m.NodeReadyLatency = int(m.NodeReady.Sub(earliest).Milliseconds())
		n.NormLatencies = append(n.NormLatencies, m)
		return true
	})
	return 0
}

func (n *autoNodeLatency) getLatency(normLatency any) map[string]float64 {
	nodeMetric := normLatency.(measurements.NodeMetric)
	return map[string]float64{
		string(corev1.NodeMemoryPressure): float64(nodeMetric.NodeMemoryPressureLatency),
		string(corev1.NodeDiskPressure):   float64(nodeMetric.NodeDiskPressureLatency),
		string(corev1.NodePIDPressure):    float64(nodeMetric.NodePIDPressureLatency),
		string(corev1.NodeReady):          float64(nodeMetric.NodeReadyLatency),
	}
}

func (n *autoNodeLatency) IsCompatible() bool {
	return true
}

// nodeTransformFunc preserves only the fields needed for latency measurements:
// - metadata: name, uid, creationTimestamp, labels
// - status: conditions
func nodeTransformFunc() cache.TransformFunc {
	return func(obj interface{}) (interface{}, error) {
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
		return minimal, nil
	}
}
