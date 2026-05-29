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
	"os"
	"strings"
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
	"k8s.io/client-go/tools/clientcmd"
)

const (
	machineLatencyMeasurement          = "machineLatencyMeasurement"
	machineLatencyQuantilesMeasurement = "machineLatencyQuantilesMeasurement"
)

var machineGVR = schema.GroupVersionResource{
	Group:    "cluster.x-k8s.io",
	Version:  "v1beta1",
	Resource: "machines",
}

// nthDoneCh is closed by nthSpotLatency when all replacements are Ready.
// machineLatency waits on it so its watcher stays alive during the observation window.
var (
	nthDoneCh   chan struct{}
	nthDoneMu   sync.Mutex
	nthTimeout  time.Duration
)

func SetNthDone(ch chan struct{}, timeout time.Duration) {
	nthDoneMu.Lock()
	nthDoneCh = ch
	nthTimeout = timeout
	nthDoneMu.Unlock()
}

// CAPI Machine condition types tracked for latency
const (
	machineInfrastructureReady = "InfrastructureReady"
	machineReady               = "Ready"
	machineNodeHealthy         = "NodeHealthy"
)

type machineMetric struct {
	Timestamp                  time.Time         `json:"timestamp"`
	InfrastructureReady        time.Time         `json:"-"`
	InfrastructureReadyLatency int               `json:"infrastructureReadyLatency"`
	Ready                      time.Time         `json:"-"`
	ReadyLatency               int               `json:"readyLatency"`
	NodeHealthy                time.Time         `json:"-"`
	NodeHealthyLatency         int               `json:"nodeHealthyLatency"`
	MetricName                 string            `json:"metricName"`
	UUID                       string            `json:"uuid"`
	JobName                    string            `json:"jobName,omitempty"`
	Name                       string            `json:"machineName"`
	NodeName                   string            `json:"nodeName,omitempty"`
	Phase                      string            `json:"phase,omitempty"`
	Labels                     map[string]string `json:"labels"`
	Metadata                   any               `json:"metadata,omitempty"`
}

type machineLatency struct {
	measurements.BaseMeasurement
	watcher      *watchers.Watcher
	mcRestConfig *rest.Config
	hcpNamespace string
	startTime    time.Time
}

type machineLatencyFactory struct {
	measurements.BaseMeasurementFactory
	mcRestConfig *rest.Config
	hcpNamespace string
}

// NewMachineLatencyFactory creates a machineLatency measurement factory that watches
// CAPI Machine resources (cluster.x-k8s.io/v1beta1) on the management cluster.
// It reads MC_KUBECONFIG for the MC rest config and HCP_NAMESPACE for the target namespace.
func NewMachineLatencyFactory(configSpec config.Spec, measurement types.Measurement, metadata map[string]any, _ string) (measurements.MeasurementFactory, error) {
	mcKubeconfigPath := os.Getenv("MC_KUBECONFIG")
	if mcKubeconfigPath == "" {
		log.Warn("MC_KUBECONFIG not set, machineLatency measurement will be disabled")
		return nil, nil
	}
	hcpNamespace := os.Getenv("HCP_NAMESPACE")
	if hcpNamespace == "" {
		log.Warn("HCP_NAMESPACE not set, machineLatency measurement will be disabled")
		return nil, nil
	}
	mcRestConfig, err := clientcmd.BuildConfigFromFlags("", mcKubeconfigPath)
	if err != nil {
		log.Errorf("Failed to build MC rest config from %s: %v", mcKubeconfigPath, err)
		return nil, err
	}
	// HCP_NAMESPACE (e.g. "sub_id-cluster_name") may not be the full MC namespace.
	// The actual MC namespace has a prefix like "ocm-staging-". Discover it by
	// listing namespaces and finding the one whose name ends with HCP_NAMESPACE.
	mcNamespace, err := DiscoverHCPNamespace(mcRestConfig, hcpNamespace)
	if err != nil {
		log.Errorf("Failed to discover HCP namespace on MC: %v", err)
		return nil, err
	}
	log.Infof("Discovered MC namespace for machineLatency: %s", mcNamespace)
	return machineLatencyFactory{
		BaseMeasurementFactory: measurements.NewBaseMeasurementFactory(configSpec, measurement, metadata, ""),
		mcRestConfig:           mcRestConfig,
		hcpNamespace:           mcNamespace,
	}, nil
}

// DiscoverHCPNamespace finds the actual namespace on the MC that ends with the
// given hcpNamespace suffix. Falls back to the exact value if no suffixed match is found.
func DiscoverHCPNamespace(restConfig *rest.Config, hcpNamespace string) (string, error) {
	mcClientSet, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create MC clientset: %w", err)
	}
	nsList, err := mcClientSet.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list namespaces on MC: %w", err)
	}
	for _, ns := range nsList.Items {
		if strings.HasSuffix(ns.Name, hcpNamespace) {
			return ns.Name, nil
		}
	}
	// Fall back to exact match
	log.Warnf("No namespace ending with %q found on MC, using as-is", hcpNamespace)
	return hcpNamespace, nil
}

func (f machineLatencyFactory) NewMeasurement(jobConfig *config.Job, clientSet kubernetes.Interface, restConfig *rest.Config, embedCfg *fileutils.EmbedConfiguration) measurements.Measurement {
	return &machineLatency{
		BaseMeasurement: f.NewBaseLatency(jobConfig, clientSet, restConfig, machineLatencyMeasurement, machineLatencyQuantilesMeasurement, embedCfg),
		mcRestConfig:    f.mcRestConfig,
		hcpNamespace:    f.hcpNamespace,
	}
}

func (ml *machineLatency) handleCreate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Error("failed to convert to Unstructured in machineLatency handleCreate")
		return
	}
	name := u.GetName()
	uid := string(u.GetUID())
	ml.Metrics.LoadOrStore(uid, machineMetric{
		Timestamp:  u.GetCreationTimestamp().UTC(),
		Name:       name,
		MetricName: machineLatencyMeasurement,
		UUID:       ml.Uuid,
		JobName:    ml.JobConfig.Name,
		Labels:     normalizeLabels(u.GetLabels()),
		Metadata:   ml.Metadata,
	})
	log.Debugf("Machine %s created at %v (uid=%s)", name, u.GetCreationTimestamp().UTC(), uid)
}

func (ml *machineLatency) handleUpdate(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		log.Error("failed to convert to Unstructured in machineLatency handleUpdate")
		return
	}
	uid := string(u.GetUID())
	value, exists := ml.Metrics.Load(uid)
	if !exists {
		return
	}
	m := value.(machineMetric)

	// Extract phase from status
	if phase, found, _ := unstructured.NestedString(u.Object, "status", "phase"); found {
		m.Phase = phase
	}

	// Extract nodeName from status.nodeRef
	if nodeName, found, _ := unstructured.NestedString(u.Object, "status", "nodeRef", "name"); found && nodeName != "" {
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
		case machineInfrastructureReady:
			m.InfrastructureReady = t.UTC()
		case machineReady:
			log.Debugf("Machine %s is ready", m.Name)
			m.Ready = t.UTC()
		case machineNodeHealthy:
			m.NodeHealthy = t.UTC()
		}
	}
	ml.Metrics.Store(uid, m)
}

func (ml *machineLatency) Start(measurementWg *sync.WaitGroup) error {
	ml.LatencyQuantiles, ml.NormLatencies = nil, nil
	ml.Metrics = sync.Map{}
	ml.startTime = time.Now().UTC()
	defer measurementWg.Done()

	// Pre-populate existing Machines so the watcher can track updates to
	// machines created between listing and watcher start.
	ml.collectExisting()

	log.Infof("Creating Machine latency watcher for %s in namespace %s", ml.JobConfig.Name, ml.hcpNamespace)
	dynamicClient := dynamic.NewForConfigOrDie(ml.mcRestConfig)
	ml.watcher = watchers.NewWatcher(
		dynamicClient,
		"machineWatcher",
		machineGVR,
		ml.hcpNamespace,
		nil,
		nil,
	)
	if err := ml.watcher.Informer.SetTransform(machineTransformFunc()); err != nil {
		log.Warnf("failed to set transform for machineWatcher: %v", err)
	}
	ml.watcher.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: ml.handleCreate,
		UpdateFunc: func(_, newObj any) {
			ml.handleUpdate(newObj)
		},
	})
	if err := ml.watcher.StartAndCacheSync(); err != nil {
		log.Errorf("Machine latency measurement error: %v", err)
	}
	return nil
}

func (ml *machineLatency) collectExisting() {
	dynamicClient := dynamic.NewForConfigOrDie(ml.mcRestConfig)
	list, err := dynamicClient.Resource(machineGVR).Namespace(ml.hcpNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Errorf("error listing Machines in %s: %v", ml.hcpNamespace, err)
		return
	}
	for _, item := range list.Items {
		ml.populateMetric(&item)
	}
}

func (ml *machineLatency) populateMetric(item *unstructured.Unstructured) {
	m := machineMetric{
		Timestamp:  item.GetCreationTimestamp().UTC(),
		Name:       item.GetName(),
		MetricName: machineLatencyMeasurement,
		UUID:       ml.Uuid,
		JobName:    ml.JobConfig.Name,
		Labels:     normalizeLabels(item.GetLabels()),
		Metadata:   ml.Metadata,
	}
	if phase, found, _ := unstructured.NestedString(item.Object, "status", "phase"); found {
		m.Phase = phase
	}
	if nodeName, found, _ := unstructured.NestedString(item.Object, "status", "nodeRef", "name"); found {
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
			case machineInfrastructureReady:
				m.InfrastructureReady = t.UTC()
			case machineReady:
				m.Ready = t.UTC()
			case machineNodeHealthy:
				m.NodeHealthy = t.UTC()
			}
		}
	}
	ml.Metrics.Store(string(item.GetUID()), m)
}

func (ml *machineLatency) Collect(measurementWg *sync.WaitGroup) {
	defer measurementWg.Done()
	dynamicClient := dynamic.NewForConfigOrDie(ml.mcRestConfig)
	list, err := dynamicClient.Resource(machineGVR).Namespace(ml.hcpNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Debugf("error listing Machines during collect: %v", err)
		return
	}
	log.Debugf("Machine Collect: found %d Machines from API", len(list.Items))
	for i := range list.Items {
		ml.populateMetric(&list.Items[i])
	}
}

func (ml *machineLatency) Stop() error {
	nthDoneMu.Lock()
	ch := nthDoneCh
	t := nthTimeout
	nthDoneMu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		case <-time.After(t):
		}
	}
	if ml.watcher != nil {
		ml.watcher.StopWatcher()
	}
	return ml.StopMeasurement(ml.normalizeLatencies, ml.getLatency)
}

func (ml *machineLatency) normalizeLatencies() float64 {
	ml.Metrics.Range(func(key, value any) bool {
		m := value.(machineMetric)
		if m.Timestamp.Before(ml.startTime) {
			log.Tracef("Machine %v latency ignored as it was created before measurement started", m.Name)
			return true
		}
		if m.Ready.IsZero() {
			log.Tracef("Machine %v latency ignored as it did not reach Ready state", m.Name)
			return true
		}
		m.InfrastructureReadyLatency = int(m.InfrastructureReady.Sub(m.Timestamp).Milliseconds())
		m.ReadyLatency = int(m.Ready.Sub(m.Timestamp).Milliseconds())
		if !m.NodeHealthy.IsZero() {
			m.NodeHealthyLatency = int(m.NodeHealthy.Sub(m.Timestamp).Milliseconds())
		}
		ml.NormLatencies = append(ml.NormLatencies, m)
		return true
	})
	return 0
}

func (ml *machineLatency) getLatency(normLatency any) map[string]float64 {
	m := normLatency.(machineMetric)
	return map[string]float64{
		machineInfrastructureReady: float64(m.InfrastructureReadyLatency),
		machineReady:              float64(m.ReadyLatency),
		machineNodeHealthy:        float64(m.NodeHealthyLatency),
	}
}

func (ml *machineLatency) IsCompatible() bool {
	return true
}

// machineTransformFunc preserves only the fields needed for latency measurements:
// - metadata: name, uid, creationTimestamp, labels
// - status: conditions, phase, nodeRef
func machineTransformFunc() cache.TransformFunc {
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
		if phase, found, _ := unstructured.NestedString(u.Object, "status", "phase"); found {
			_ = unstructured.SetNestedField(minimal.Object, phase, "status", "phase")
		}
		if nodeRef, found, _ := unstructured.NestedMap(u.Object, "status", "nodeRef"); found {
			_ = unstructured.SetNestedField(minimal.Object, nodeRef, "status", "nodeRef")
		}
		return minimal, nil
	}
}
