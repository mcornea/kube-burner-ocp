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
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
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
	capiMachineGVR = schema.GroupVersionResource{
		Group:    "cluster.x-k8s.io",
		Version:  "v1beta2",
		Resource: "machines",
	}
	instanceIDPattern = regexp.MustCompile(`i-[a-f0-9]+`)
)

// NewNthSpot creates the nth-spot workload command for NTH spot interruption testing.
const pausePodNamespace = "nth-spot-pods"

func NewNthSpot(wh *workloads.WorkloadHelper) *cobra.Command {
	var rc int
	var metricsProfiles []string
	var spotPodCount, fisTargetCount, fisBatchSize int
	var fisBatchInterval time.Duration
	var containerImage, fisRoleARN string
	var spotPodPDB bool
	cmd := &cobra.Command{
		Use:          "nth-spot",
		Short:        "Runs NTH spot interruption latency test",
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			mcKubeconfigPath := os.Getenv("MC_KUBECONFIG")
			if mcKubeconfigPath == "" {
				log.Fatal("MC_KUBECONFIG must be set to the management cluster kubeconfig path")
			}
			hcpNamespace := os.Getenv("HCP_NAMESPACE")
			if hcpNamespace == "" {
				log.Fatal("HCP_NAMESPACE must be set (e.g. subscription_id-cluster_name)")
			}

			mcRestConfig, err := clientcmd.BuildConfigFromFlags("", mcKubeconfigPath)
			if err != nil {
				log.Fatalf("Failed to build MC rest config: %v", err)
			}
			mcDynClient := dynamic.NewForConfigOrDie(mcRestConfig)

			// Discover cluster details from MC
			region, queueURL, err := discoverNthClusterDetails(mcDynClient, hcpNamespace)
			if err != nil {
				log.Fatalf("Failed to discover cluster details: %v", err)
			}

			if fisRoleARN == "" {
				log.Fatal("--fis-role-arn must be set (from setup-spot-nth.sh output)")
			}

			log.Infof("NTH spot test: region=%s queueURL=%s fisTemplate=%s", region, queueURL, fisRoleARN)

			if err := ocpMeasurements.PurgeSQSQueue(queueURL, region); err != nil {
				log.Warnf("Failed to purge SQS queue (continuing): %v", err)
			}

			// Discover HCP namespace (resolve the full MC namespace with prefix)
			fullHCPNamespace, err := ocpMeasurements.DiscoverHCPNamespace(mcRestConfig, hcpNamespace)
			if err != nil {
				log.Fatalf("Failed to discover HCP namespace: %v", err)
			}

			// List spot instances
			instances, err := discoverSpotInstances(mcDynClient, fullHCPNamespace)
			if err != nil {
				log.Fatalf("Failed to discover spot instances: %v", err)
			}
			if len(instances) == 0 {
				log.Fatal("No running spot Machines found in HCP namespace")
			}
			log.Infof("Found %d running spot instances", len(instances))

			// Limit to fisTargetCount if set (0 means all)
			if fisTargetCount > 0 && fisTargetCount < len(instances) {
				log.Infof("Limiting FIS targets to %d of %d instances", fisTargetCount, len(instances))
				instances = instances[:fisTargetCount]
			}

			for _, inst := range instances {
				log.Infof("  %s → node=%s machine=%s", inst.InstanceID, inst.NodeName, inst.MachineName)
			}

			instancesJSON, err := json.Marshal(instances)
			if err != nil {
				log.Fatalf("Failed to marshal instances: %v", err)
			}

			// Extract cluster name from HCP namespace for FIS tagging
			clusterName := hcpNamespace
			if idx := strings.LastIndex(hcpNamespace, "-"); idx >= 0 {
				clusterName = hcpNamespace[idx+1:]
			}

			// Set env vars for the measurement factory
			os.Setenv("NTH_QUEUE_URL", queueURL)
			os.Setenv("NTH_REGION", region)
			os.Setenv("NTH_FIS_ROLE_ARN", fisRoleARN)
			os.Setenv("NTH_CLUSTER_NAME", clusterName)
			os.Setenv("NTH_SPOT_INSTANCES", string(instancesJSON))
			os.Setenv("NTH_TIMEOUT", wh.Config.Timeout.String())
			if fisBatchSize > 0 {
				os.Setenv("NTH_FIS_BATCH_SIZE", fmt.Sprintf("%d", fisBatchSize))
			}
			if fisBatchInterval > 0 {
				os.Setenv("NTH_FIS_BATCH_INTERVAL", fisBatchInterval.String())
			}

			measurementFactories := map[string]kubeburnermeasurements.NewMeasurementFactory{
				"nthSpotLatency": ocpMeasurements.NewNthSpotLatencyFactory,
				"machineLatency": ocpMeasurements.NewMachineLatencyFactory,
			}

			// Create pause pods on spot nodes before the test
			if spotPodCount > 0 {
				if err := createPausePods(instances, spotPodCount, containerImage); err != nil {
					log.Fatalf("Failed to create pause pods: %v", err)
				}
				if spotPodPDB {
					if err := createPausePodPDBs(instances); err != nil {
						log.Fatalf("Failed to create PDBs: %v", err)
					}
				}
				defer cleanupPausePods()
			}

			setMetrics(cmd, metricsProfiles)
			wh.SetMeasurements(measurementFactories)
			wh.SetVariables(AdditionalVars, SetVars)
			rc = wh.Run(cmd.Name() + ".yml")
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			os.Exit(rc)
		},
	}
	cmd.Flags().StringVar(&fisRoleARN, "fis-role-arn", "", "FIS IAM role ARN (from setup-spot-nth.sh output)")
	cmd.Flags().IntVar(&fisTargetCount, "fis-target-count", 0, "Number of instances to interrupt via FIS (0 = all discovered instances)")
	cmd.Flags().IntVar(&fisBatchSize, "fis-batch-size", 0, "Number of nodes to interrupt per FIS batch (0 = all at once)")
	cmd.Flags().DurationVar(&fisBatchInterval, "fis-batch-interval", 0, "Pause duration between FIS batches (e.g., 2m, 30s)")
	cmd.Flags().IntVar(&spotPodCount, "spot-pod-count", 5, "Number of pause pods per spot node (0 to skip)")
	cmd.Flags().BoolVar(&spotPodPDB, "spot-pod-pdb", false, "Create PDBs with maxUnavailable=0 to block drain")
	cmd.Flags().StringVar(&containerImage, "container-image", "quay.io/prometheus/busybox:latest", "Container image for pause pods")
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	return cmd
}

// discoverNthClusterDetails finds the AWS region and SQS queue URL from a
// HostedCluster whose HCP namespace matches the given hcpNamespace.
func discoverNthClusterDetails(dynClient dynamic.Interface, hcpNamespace string) (region, queueURL string, err error) {
	hcList, err := dynClient.Resource(hostedClusterGVR).Namespace("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to list HostedClusters: %w", err)
	}
	for _, hc := range hcList.Items {
		hcNamespace := hc.GetNamespace()
		hcName := hc.GetName()
		expectedHCPNS := fmt.Sprintf("%s-%s", hcNamespace, hcName)
		if expectedHCPNS == hcpNamespace || hcName == hcpNamespace || strings.HasSuffix(expectedHCPNS, hcpNamespace) {
			r, _, _ := unstructured.NestedString(hc.Object, "spec", "platform", "aws", "region")
			q, _, _ := unstructured.NestedString(hc.Object, "spec", "platform", "aws", "terminationHandlerQueueURL")
			if r == "" {
				return "", "", fmt.Errorf("HostedCluster %s/%s has no region", hcNamespace, hcName)
			}
			if q == "" {
				return "", "", fmt.Errorf("HostedCluster %s/%s has no terminationHandlerQueueURL — run setup-spot-nth.sh first", hcNamespace, hcName)
			}
			return r, q, nil
		}
	}
	return "", "", fmt.Errorf("no HostedCluster found matching HCP namespace %q", hcpNamespace)
}

func discoverSpotInstances(dynClient dynamic.Interface, hcpNamespace string) ([]ocpMeasurements.SpotInstance, error) {
	list, err := dynClient.Resource(capiMachineGVR).Namespace(hcpNamespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "hypershift.openshift.io/interruptible-instance",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list Machines: %w", err)
	}
	var instances []ocpMeasurements.SpotInstance
	for _, item := range list.Items {
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase != "Running" {
			continue
		}
		providerID, _, _ := unstructured.NestedString(item.Object, "spec", "providerID")
		idMatch := instanceIDPattern.FindString(providerID)
		if idMatch == "" {
			continue
		}
		nodeName, _, _ := unstructured.NestedString(item.Object, "status", "nodeRef", "name")
		instances = append(instances, ocpMeasurements.SpotInstance{
			InstanceID:  idMatch,
			NodeName:    nodeName,
			MachineName: item.GetName(),
		})
	}
	return instances, nil
}

func createPausePods(instances []ocpMeasurements.SpotInstance, podsPerNode int, image string) error {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return fmt.Errorf("failed to build guest cluster config: %w", err)
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)

	// Create namespace
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: pausePodNamespace}}
	if _, err := clientset.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create namespace %s: %w", pausePodNamespace, err)
	}

	var gracePeriod int64
	for _, inst := range instances {
		for i := 0; i < podsPerNode; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("nth-pause-%s-%d", inst.InstanceID, i),
					Namespace: pausePodNamespace,
					Labels:    map[string]string{"app": "nth-pause", "instance": inst.InstanceID},
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

	// Wait for all pods to be Running. At large scale, some spot nodes may be
	// reclaimed during pod creation, leaving orphaned Pending pods. Accept when
	// Running count is stable and all non-Pending pods are Running.
	expectedPods := len(instances) * podsPerNode
	log.Infof("Waiting for %d pause pods to be Running...", expectedPods)
	lastReady := 0
	stableCount := 0
	err = wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods, err := clientset.CoreV1().Pods(pausePodNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=nth-pause",
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
				Name:      fmt.Sprintf("nth-pause-%s", inst.InstanceID),
				Namespace: pausePodNamespace,
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app":      "nth-pause",
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

