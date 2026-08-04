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
	"strings"
	"time"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	ocpMeasurements "github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

const (
	scalingNamespace  = "karpenter-spot-scaling"
	scalingDeployment = "karpenter-spot-driver"
)

var nodeClaimGVR = schema.GroupVersionResource{
	Group:    "karpenter.sh",
	Version:  "v1",
	Resource: "nodeclaims",
}

func NewKarpenterSpot(wh *workloads.WorkloadHelper) *cobra.Command {
	var rc int
	var metricsProfiles []string
	var spotPodCount, fisTargetCount, fisBatchSize, spotNodeCount int
	var fisBatchInterval time.Duration
	var containerImage, fisRoleARN, sqsQueueURL, scalingCPURequest string
	var spotPodPDB, skipScaling bool
	cmd := &cobra.Command{
		Use:          "karpenter-spot",
		Short:        "Runs Karpenter spot interruption latency test",
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

			region, err := discoverClusterRegion(mcDynClient, hcpNamespace)
			if err != nil {
				log.Fatalf("Failed to discover cluster region: %v", err)
			}

			if fisRoleARN == "" {
				log.Fatal("--fis-role-arn must be set")
			}
			if sqsQueueURL == "" {
				log.Fatal("--sqs-queue-url must be set to the Karpenter interruption SQS queue URL")
			}

			log.Infof("Karpenter spot test: region=%s sqsQueueURL=%s", region, sqsQueueURL)

			if err := ocpMeasurements.PurgeSQSQueue(sqsQueueURL, region); err != nil {
				log.Warnf("Failed to purge SQS queue (continuing): %v", err)
			}

			// Deploy scaling pods to trigger Karpenter to provision spot nodes
			if !skipScaling && spotNodeCount > 0 {
				if err := deployScalingPods(spotNodeCount, scalingCPURequest, containerImage); err != nil {
					log.Fatalf("Failed to deploy scaling pods: %v", err)
				}
				defer cleanupScalingPods()

				log.Infof("Waiting for %d Karpenter spot nodes to become Ready...", spotNodeCount)
				if err := waitForKarpenterSpotNodes(spotNodeCount, 20*time.Minute); err != nil {
					log.Fatalf("Failed waiting for spot nodes: %v", err)
				}
			}

			// Discover Karpenter spot instances from guest cluster nodes
			instances, err := discoverKarpenterSpotInstances()
			if err != nil {
				log.Fatalf("Failed to discover Karpenter spot instances: %v", err)
			}
			if len(instances) == 0 {
				log.Fatal("No running Karpenter spot instances found")
			}
			log.Infof("Found %d running Karpenter spot instances", len(instances))

			if fisTargetCount > 0 && fisTargetCount < len(instances) {
				log.Infof("Limiting FIS targets to %d of %d instances", fisTargetCount, len(instances))
				instances = instances[:fisTargetCount]
			}

			for _, inst := range instances {
				log.Infof("  %s → node=%s nodeclaim=%s", inst.InstanceID, inst.NodeName, inst.MachineName)
			}

			instancesJSON, err := json.Marshal(instances)
			if err != nil {
				log.Fatalf("Failed to marshal instances: %v", err)
			}

			clusterName := hcpNamespace
			if idx := strings.LastIndex(hcpNamespace, "-"); idx >= 0 {
				clusterName = hcpNamespace[idx+1:]
			}

			os.Setenv("KARPENTER_QUEUE_URL", sqsQueueURL)
			os.Setenv("KARPENTER_REGION", region)
			os.Setenv("KARPENTER_FIS_ROLE_ARN", fisRoleARN)
			os.Setenv("KARPENTER_CLUSTER_NAME", clusterName)
			os.Setenv("KARPENTER_SPOT_INSTANCES", string(instancesJSON))
			os.Setenv("KARPENTER_TIMEOUT", wh.Config.Timeout.String())
			if fisBatchSize > 0 {
				os.Setenv("KARPENTER_FIS_BATCH_SIZE", fmt.Sprintf("%d", fisBatchSize))
			}
			if fisBatchInterval > 0 {
				os.Setenv("KARPENTER_FIS_BATCH_INTERVAL", fisBatchInterval.String())
			}

			measurementFactories := map[string]kubeburnermeasurements.NewMeasurementFactory{
				"karpenterSpotLatency": ocpMeasurements.NewKarpenterSpotLatencyFactory,
			}

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
	cmd.Flags().StringVar(&fisRoleARN, "fis-role-arn", "", "FIS IAM role ARN")
	cmd.Flags().StringVar(&sqsQueueURL, "sqs-queue-url", "", "Karpenter interruption SQS queue URL")
	cmd.Flags().IntVar(&fisTargetCount, "fis-target-count", 0, "Number of instances to interrupt via FIS (0 = all discovered instances)")
	cmd.Flags().IntVar(&fisBatchSize, "fis-batch-size", 0, "Number of nodes to interrupt per FIS batch (0 = all at once)")
	cmd.Flags().DurationVar(&fisBatchInterval, "fis-batch-interval", 0, "Pause duration between FIS batches (e.g., 2m, 30s)")
	cmd.Flags().IntVar(&spotPodCount, "spot-pod-count", 5, "Number of pause pods per spot node (0 to skip)")
	cmd.Flags().BoolVar(&spotPodPDB, "spot-pod-pdb", false, "Create PDBs with maxUnavailable=0 to block drain")
	cmd.Flags().StringVar(&containerImage, "container-image", "quay.io/prometheus/busybox:latest", "Container image for pause pods")
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	cmd.Flags().IntVar(&spotNodeCount, "spot-node-count", 0, "Number of spot nodes to provision via Karpenter before the test (0 = use existing nodes)")
	cmd.Flags().StringVar(&scalingCPURequest, "scaling-cpu-request", "300m", "CPU request per scaling pod (10 pods per node, default: 300m × 10 = 3000m per node)")
	cmd.Flags().BoolVar(&skipScaling, "skip-scaling", false, "Skip scaling pod deployment (use pre-existing spot nodes)")
	return cmd
}

// deployScalingPods creates a Deployment with one pod per desired node, each requesting
// enough CPU to force Karpenter to provision a separate spot node.
func deployScalingPods(nodeCount int, cpuRequest, image string) error {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return fmt.Errorf("failed to build guest cluster config: %w", err)
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: scalingNamespace,
			Labels: map[string]string{
				"security.openshift.io/scc.podSecurityLabelSync": "false",
				"pod-security.kubernetes.io/enforce":              "privileged",
				"pod-security.kubernetes.io/audit":                "privileged",
				"pod-security.kubernetes.io/warn":                 "privileged",
			},
		},
	}
	if _, err := clientset.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create namespace %s: %w", scalingNamespace, err)
	}

	// Deploy multiple small pods per node to let Karpenter bin-pack efficiently.
	// This avoids over-provisioning: the provisioner sees many small pods and
	// packs them tightly, creating close to the exact number of nodes needed.
	podsPerNode := 10
	cpuPerPod := resource.MustParse(cpuRequest)
	totalPods := int32(nodeCount * podsPerNode)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scalingDeployment,
			Namespace: scalingNamespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &totalPods,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": scalingDeployment},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": scalingDeployment},
				},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						"karpenter.sh/capacity-type": "spot",
					},
					Containers: []corev1.Container{
						{
							Name:            "pause",
							Image:           image,
							Command:         []string{"sleep", "inf"},
							ImagePullPolicy: corev1.PullIfNotPresent,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU: cpuPerPod,
								},
							},
						},
					},
				},
			},
		},
	}

	if _, err := clientset.AppsV1().Deployments(scalingNamespace).Create(context.TODO(), deploy, metav1.CreateOptions{}); err != nil {
		if errors.IsAlreadyExists(err) {
			if _, err := clientset.AppsV1().Deployments(scalingNamespace).Update(context.TODO(), deploy, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to update scaling deployment: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create scaling deployment: %w", err)
		}
	}

	log.Infof("Deployed %d scaling pods (%d per node × %d nodes, CPU request: %s each) to trigger Karpenter spot node provisioning", totalPods, podsPerNode, nodeCount, cpuRequest)
	return nil
}

// waitForKarpenterSpotNodes waits until the desired number of Karpenter spot nodes
// are Ready on the guest cluster.
func waitForKarpenterSpotNodes(desiredCount int, timeout time.Duration) error {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return fmt.Errorf("failed to build guest cluster config: %w", err)
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)

	return wait.PollUntilContextTimeout(context.TODO(), 15*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: "karpenter.sh/capacity-type=spot",
		})
		if err != nil {
			log.Warnf("Error listing spot nodes (retrying): %v", err)
			return false, nil
		}
		readyCount := 0
		for _, node := range nodes.Items {
			for _, cond := range node.Status.Conditions {
				if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
					readyCount++
					break
				}
			}
		}
		log.Infof("Karpenter spot nodes: %d/%d Ready", readyCount, desiredCount)
		return readyCount >= desiredCount, nil
	})
}

// cleanupScalingPods deletes the scaling namespace and all its resources.
func cleanupScalingPods() {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		log.Warnf("Failed to build guest cluster config for scaling cleanup: %v", err)
		return
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)
	if err := clientset.CoreV1().Namespaces().Delete(context.TODO(), scalingNamespace, metav1.DeleteOptions{}); err != nil {
		log.Warnf("Failed to delete scaling namespace %s: %v", scalingNamespace, err)
	} else {
		log.Infof("Cleaned up scaling namespace %s", scalingNamespace)
	}
}

// discoverClusterRegion finds the AWS region from a HostedCluster matching the HCP namespace.
func discoverClusterRegion(dynClient dynamic.Interface, hcpNamespace string) (string, error) {
	hcList, err := dynClient.Resource(hostedClusterGVR).Namespace("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list HostedClusters: %w", err)
	}
	for _, hc := range hcList.Items {
		hcNamespace := hc.GetNamespace()
		hcName := hc.GetName()
		expectedHCPNS := fmt.Sprintf("%s-%s", hcNamespace, hcName)
		if expectedHCPNS == hcpNamespace || hcName == hcpNamespace || strings.HasSuffix(expectedHCPNS, hcpNamespace) {
			r, _, _ := unstructured.NestedString(hc.Object, "spec", "platform", "aws", "region")
			if r == "" {
				return "", fmt.Errorf("HostedCluster %s/%s has no region", hcNamespace, hcName)
			}
			return r, nil
		}
	}
	return "", fmt.Errorf("no HostedCluster found matching HCP namespace %q", hcpNamespace)
}

// discoverKarpenterSpotInstances finds Karpenter-managed spot instances by listing
// guest cluster nodes with karpenter.sh/capacity-type=spot label and cross-referencing
// with NodeClaims for the NodeClaim name. Instance IDs are extracted from node providerID.
func discoverKarpenterSpotInstances() ([]ocpMeasurements.SpotInstance, error) {
	guestConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return nil, fmt.Errorf("failed to build guest cluster config: %w", err)
	}
	clientset := kubernetes.NewForConfigOrDie(guestConfig)
	dynClient := dynamic.NewForConfigOrDie(guestConfig)

	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: "karpenter.sh/capacity-type=spot",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list spot nodes: %w", err)
	}

	// Build nodeName → nodeClaimName map from NodeClaims, excluding those already being deleted
	nodeClaimMap := make(map[string]string)
	deletingNodes := make(map[string]bool)
	ncList, err := dynClient.Resource(nodeClaimGVR).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "karpenter.sh/capacity-type=spot",
	})
	if err != nil {
		log.Warnf("Failed to list NodeClaims (will use node names as machine names): %v", err)
	} else {
		for _, nc := range ncList.Items {
			if nc.GetDeletionTimestamp() != nil {
				nodeName, _, _ := unstructured.NestedString(nc.Object, "status", "nodeName")
				if nodeName != "" {
					deletingNodes[nodeName] = true
					log.Infof("Skipping NodeClaim %s (already deleting) → node=%s", nc.GetName(), nodeName)
				}
				continue
			}
			nodeName, _, _ := unstructured.NestedString(nc.Object, "status", "nodeName")
			if nodeName != "" {
				nodeClaimMap[nodeName] = nc.GetName()
			}
		}
	}

	var instances []ocpMeasurements.SpotInstance
	for _, node := range nodes.Items {
		if deletingNodes[node.Name] {
			continue
		}

		// Skip nodes already tainted with karpenter.sh/disrupted
		isDisrupted := false
		for _, taint := range node.Spec.Taints {
			if taint.Key == "karpenter.sh/disrupted" {
				isDisrupted = true
				break
			}
		}
		if isDisrupted {
			log.Infof("Skipping node %s (already disrupted)", node.Name)
			continue
		}

		// Skip nodes that are not Ready
		isReady := false
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				isReady = true
				break
			}
		}
		if !isReady {
			continue
		}

		// Skip unschedulable nodes
		if node.Spec.Unschedulable {
			log.Infof("Skipping node %s (unschedulable)", node.Name)
			continue
		}

		providerID := node.Spec.ProviderID
		idMatch := spotInstanceIDPattern.FindString(providerID)
		if idMatch == "" {
			continue
		}

		nodeClaimName := nodeClaimMap[node.Name]
		if nodeClaimName == "" {
			nodeClaimName = node.Name
		}

		instances = append(instances, ocpMeasurements.SpotInstance{
			InstanceID:  idMatch,
			NodeName:    node.Name,
			MachineName: nodeClaimName,
		})
	}
	return instances, nil
}
