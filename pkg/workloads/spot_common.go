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

package workloads

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	ocpMeasurements "github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

var (
	hostedClusterGVR = schema.GroupVersionResource{
		Group:    "hypershift.openshift.io",
		Version:  "v1beta1",
		Resource: "hostedclusters",
	}
	spotInstanceIDPattern = regexp.MustCompile(`i-[a-f0-9]+`)
)

const pausePodNamespace = "spot-test-pods"

func createPausePods(instances []ocpMeasurements.SpotInstance, podsPerNode int, image string) error {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return fmt.Errorf("failed to build guest cluster config: %w", err)
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: pausePodNamespace}}
	if _, err := clientset.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create namespace %s: %w", pausePodNamespace, err)
	}

	var gracePeriod int64
	for _, inst := range instances {
		for i := 0; i < podsPerNode; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("spot-pause-%s-%d", inst.InstanceID, i),
					Namespace: pausePodNamespace,
					Labels:    map[string]string{"app": "spot-pause", "instance": inst.InstanceID},
				},
				Spec: corev1.PodSpec{
					NodeSelector:                  map[string]string{"kubernetes.io/hostname": inst.NodeName},
					TerminationGracePeriodSeconds: &gracePeriod,
					Containers: []corev1.Container{{
						Name:            "pause",
						Image:           image,
						Command:         []string{"sleep", "inf"},
						ImagePullPolicy: corev1.PullIfNotPresent,
					}},
				},
			}
			for retry := 0; retry < 10; retry++ {
				_, err := clientset.CoreV1().Pods(pausePodNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
				if err == nil || errors.IsAlreadyExists(err) {
					break
				}
				if retry == 9 {
					return fmt.Errorf("failed to create pod for %s after 10 attempts: %w", inst.InstanceID, err)
				}
				backoff := time.Duration(retry+1) * 2 * time.Second
				log.Warnf("Retrying pod creation for %s (attempt %d/10, backoff %v): %v", inst.InstanceID, retry+1, backoff, err)
				time.Sleep(backoff)
			}
		}
		log.Infof("Created %d pause pods on node %s (%s)", podsPerNode, inst.NodeName, inst.InstanceID)
	}

	expectedPods := len(instances) * podsPerNode
	log.Infof("Waiting for %d pause pods to be Running...", expectedPods)
	lastReady := 0
	stableCount := 0
	err = wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods, err := clientset.CoreV1().Pods(pausePodNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=spot-pause",
		})
		if err != nil {
			return false, nil
		}
		ready := 0
		pending := 0
		for i := range pods.Items {
			if pods.Items[i].Status.Phase == corev1.PodRunning {
				ready++
			} else if pods.Items[i].Status.Phase == corev1.PodPending {
				pending++
			}
		}
		log.Infof("Pause pods: %d/%d Running (%d Pending)", ready, expectedPods, pending)
		if ready >= expectedPods {
			return true, nil
		}
		if ready == lastReady && ready > 0 && pending > 0 {
			stableCount++
			if stableCount >= 3 {
				log.Warnf("Pause pods stabilized at %d/%d Running with %d Pending (likely spot-reclaimed nodes) — proceeding", ready, expectedPods, pending)
				return true, nil
			}
		} else {
			stableCount = 0
		}
		lastReady = ready
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("timeout waiting for pause pods: %w", err)
	}
	log.Infof("Pause pods ready: %d/%d expected", lastReady, expectedPods)
	return nil
}

func createPausePodPDBs(instances []ocpMeasurements.SpotInstance) error {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return fmt.Errorf("failed to build guest cluster config: %w", err)
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)

	maxUnavailable := intstr.FromInt(0)
	for _, inst := range instances {
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("spot-pause-%s", inst.InstanceID),
				Namespace: pausePodNamespace,
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app":      "spot-pause",
						"instance": inst.InstanceID,
					},
				},
			},
		}
		if _, err := clientset.PolicyV1().PodDisruptionBudgets(pausePodNamespace).Create(context.TODO(), pdb, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create PDB for %s: %w", inst.InstanceID, err)
		}
	}
	log.Infof("Created %d PDBs with maxUnavailable=0 (drain will be blocked)", len(instances))
	return nil
}

func cleanupPausePods() {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		log.Warnf("Failed to build guest cluster config for cleanup: %v", err)
		return
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)
	if err := clientset.CoreV1().Namespaces().Delete(context.TODO(), pausePodNamespace, metav1.DeleteOptions{}); err != nil {
		log.Warnf("Failed to delete namespace %s: %v", pausePodNamespace, err)
	} else {
		log.Infof("Cleaned up pause pod namespace %s", pausePodNamespace)
	}
}
