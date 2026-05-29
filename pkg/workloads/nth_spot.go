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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
		Version:  "v1beta1",
		Resource: "machines",
	}
	instanceIDPattern = regexp.MustCompile(`i-[a-f0-9]+`)
)

// NewNthSpot creates the nth-spot workload command for NTH spot interruption testing.
const pausePodNamespace = "nth-spot-pods"

func NewNthSpot(wh *workloads.WorkloadHelper) *cobra.Command {
	var rc int
	var metricsProfiles []string
	var spotReplicas, workersCount, spotPodCount int
	var instanceType, workloadType, eventType, containerImage string
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

			// Get AWS account ID
			accountID, err := getAWSAccountID(region)
			if err != nil {
				log.Fatalf("Failed to get AWS account ID: %v", err)
			}

			log.Infof("NTH spot test: region=%s queueURL=%s accountID=%s", region, queueURL, accountID)

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
			for _, inst := range instances {
				log.Infof("  %s → node=%s machine=%s", inst.InstanceID, inst.NodeName, inst.MachineName)
			}

			instancesJSON, err := json.Marshal(instances)
			if err != nil {
				log.Fatalf("Failed to marshal instances: %v", err)
			}

			// Set env vars for the measurement factory
			os.Setenv("NTH_QUEUE_URL", queueURL)
			os.Setenv("NTH_REGION", region)
			os.Setenv("NTH_AWS_ACCOUNT_ID", accountID)
			os.Setenv("NTH_EVENT_TYPE", eventType)
			os.Setenv("NTH_SPOT_INSTANCES", string(instancesJSON))
			os.Setenv("NTH_TIMEOUT", wh.Config.Timeout.String())
			AdditionalVars["WORKLOAD_TYPE"] = workloadType
			AdditionalVars["SPOT_REPLICAS"] = spotReplicas
			AdditionalVars["INSTANCE_TYPE"] = instanceType
			AdditionalVars["WORKERS_COUNT"] = workersCount

			measurementFactories := map[string]kubeburnermeasurements.NewMeasurementFactory{
				"nthSpotLatency": ocpMeasurements.NewNthSpotLatencyFactory,
				"machineLatency": ocpMeasurements.NewMachineLatencyFactory,
			}

			// Create pause pods on spot nodes before the test
			if spotPodCount > 0 {
				if err := createPausePods(instances, spotPodCount, containerImage); err != nil {
					log.Fatalf("Failed to create pause pods: %v", err)
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
	cmd.Flags().IntVar(&spotReplicas, "spot-replicas", 5, "Number of spot nodes (metadata only)")
	cmd.Flags().StringVar(&instanceType, "instance-type", "c5a.xlarge", "Spot instance type (metadata only)")
	cmd.Flags().IntVar(&workersCount, "workers-count", 10, "NTH WORKERS value (metadata only)")
	cmd.Flags().StringVar(&workloadType, "workload-type", "empty", "Workload type: empty, standard, pdb-protected")
	cmd.Flags().StringVar(&eventType, "event-type", "spot-itn", "Event type: spot-itn, rebalance, mixed")
	cmd.Flags().IntVar(&spotPodCount, "spot-pod-count", 5, "Number of pause pods per spot node (0 to skip)")
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
			if _, err := clientset.CoreV1().Pods(pausePodNamespace).Create(context.TODO(), pod, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create pod for %s: %w", inst.InstanceID, err)
			}
		}
		log.Infof("Created %d pause pods on node %s (%s)", podsPerNode, inst.NodeName, inst.InstanceID)
	}

	// Wait for all pods to be Running
	expectedPods := len(instances) * podsPerNode
	log.Infof("Waiting for %d pause pods to be Running...", expectedPods)
	err = wait.PollUntilContextTimeout(context.TODO(), 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods, err := clientset.CoreV1().Pods(pausePodNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=nth-pause",
		})
		if err != nil {
			return false, nil
		}
		ready := 0
		for i := range pods.Items {
			if pods.Items[i].Status.Phase == corev1.PodRunning {
				ready++
			}
		}
		log.Infof("Pause pods: %d/%d Running", ready, expectedPods)
		return ready >= expectedPods, nil
	})
	if err != nil {
		return fmt.Errorf("timeout waiting for pause pods: %w", err)
	}
	log.Infof("All %d pause pods Running", expectedPods)
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

func getAWSAccountID(region string) (string, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return "", fmt.Errorf("failed to create AWS session: %w", err)
	}
	out, err := sts.New(sess).GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("failed to get caller identity: %w", err)
	}
	return aws.StringValue(out.Account), nil
}
