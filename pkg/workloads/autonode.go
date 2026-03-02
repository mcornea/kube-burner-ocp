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
	"os"
	"time"

	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	"github.com/spf13/cobra"

	ocpMeasurements "github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

// NewAutoNode holds autonode workload
func NewAutoNode(wh *workloads.WorkloadHelper) *cobra.Command {
	var rc int
	var metricsProfiles []string
	var pods, churnCycles, churnPercent int
	var cpuRequest, memoryRequest, containerImage string
	var podReadyThreshold, jobPause, churnDuration, churnDelay, churnDeleteDelay time.Duration
	var deletionStrategy, churnMode string
	cmd := &cobra.Command{
		Use:          "autonode",
		Short:        "Runs autonode workload",
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			AdditionalVars["JOB_ITERATIONS"] = pods / 4
			AdditionalVars["DELETION_STRATEGY"] = deletionStrategy
			AdditionalVars["POD_READY_THRESHOLD"] = podReadyThreshold
			AdditionalVars["JOB_PAUSE"] = jobPause
			AdditionalVars["CHURN_CYCLES"] = churnCycles
			AdditionalVars["CHURN_DURATION"] = churnDuration
			AdditionalVars["CHURN_DELAY"] = churnDelay
			AdditionalVars["CHURN_PERCENT"] = churnPercent
			AdditionalVars["CHURN_DELETE_DELAY"] = churnDeleteDelay
			AdditionalVars["CHURN_MODE"] = churnMode
			AdditionalVars["CPU_REQUEST"] = cpuRequest
			AdditionalVars["MEMORY_REQUEST"] = memoryRequest
			AdditionalVars["CONTAINER_IMAGE"] = containerImage
			setMetrics(cmd, metricsProfiles)
			wh.SetMeasurements(map[string]kubeburnermeasurements.NewMeasurementFactory{
				"autoNodeLatency": ocpMeasurements.NewAutoNodeLatencyFactory,
				"nodeClaimLatency":      ocpMeasurements.NewNodeClaimLatencyFactory,
			})
			wh.SetVariables(AdditionalVars, SetVars)
			rc = wh.Run(cmd.Name() + ".yml")
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			os.Exit(rc)
		},
	}
	cmd.Flags().IntVar(&pods, "pods", 100, "Total number of pods distributed evenly across 4 instance types (must be divisible by 4)")
	cmd.Flags().StringVar(&cpuRequest, "cpu-request", "500m", "CPU request per pod")
	cmd.Flags().StringVar(&memoryRequest, "memory-request", "128Mi", "Memory request per pod")
	cmd.Flags().StringVar(&containerImage, "container-image", "registry.k8s.io/pause:3.9", "Pod container image")
	cmd.Flags().DurationVar(&podReadyThreshold, "pod-ready-threshold", 5*time.Minute, "Pod ready timeout threshold")
	cmd.Flags().DurationVar(&jobPause, "job-pause", 0, "Steady-state hold duration after pod creation")
	cmd.Flags().IntVar(&churnCycles, "churn-cycles", 0, "Churn cycles to execute")
	cmd.Flags().DurationVar(&churnDuration, "churn-duration", 0, "Churn duration")
	cmd.Flags().DurationVar(&churnDelay, "churn-delay", 2*time.Minute, "Time to wait between each churn")
	cmd.Flags().DurationVar(&churnDeleteDelay, "churn-delete-delay", 0, "Time to wait after object deletion before recreation during churn")
	cmd.Flags().IntVar(&churnPercent, "churn-percent", 10, "Percentage of job iterations that kube-burner will churn each round")
	cmd.Flags().StringVar(&churnMode, "churn-mode", string(config.ChurnObjects), "Either namespaces, to churn entire namespaces or objects, to churn individual objects")
	cmd.Flags().StringVar(&deletionStrategy, "deletion-strategy", config.GVRDeletionStrategy, "GC deletion mode")
	cmd.Flags().StringSliceVar(&metricsProfiles, "metrics-profile", []string{"metrics.yml"}, "Comma separated list of metrics profiles to use")
	return cmd
}
