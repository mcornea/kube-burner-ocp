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
	"embed"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
	kubeburnermeasurements "github.com/kube-burner/kube-burner/v2/pkg/measurements"
	"github.com/kube-burner/kube-burner/v2/pkg/config"
	"github.com/kube-burner/kube-burner/v2/pkg/workloads"
	"github.com/spf13/cobra"

	ocpMeasurements "github.com/kube-burner/kube-burner-ocp/pkg/measurements"
)

// WavesConfig represents the top-level waves configuration
type WavesConfig struct {
	Delay time.Duration `yaml:"delay"`
	Waves []Wave        `yaml:"waves"`
}

// Wave represents a single wave of pods with a specific CPU request
type Wave struct {
	Pods       int    `yaml:"pods"`
	CPURequest string `yaml:"cpuRequest"`
}

const wavesConfigDir = "config/autonode"

// NewAutoNode holds autonode workload
func NewAutoNode(wh *workloads.WorkloadHelper, embedFS embed.FS) *cobra.Command {
	var rc int
	var metricsProfiles []string
	var wavesConfigFile string
	var churnCycles, churnPercent int
	var memoryRequest, containerImage string
	var podReadyThreshold, jobPause, churnDuration, churnDelay, churnDeleteDelay time.Duration
	var deletionStrategy, churnMode string
	cmd := &cobra.Command{
		Use:          "autonode",
		Short:        "Runs autonode workload",
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			wavesConfigPath := wavesConfigDir + "/" + wavesConfigFile + ".yml"
			data, err := embedFS.ReadFile(wavesConfigPath)
			if err != nil {
				log.Fatalf("Error reading embedded waves config: %v", err)
			}
			var wavesConfig WavesConfig
			if err := yaml.Unmarshal(data, &wavesConfig); err != nil {
				log.Fatalf("Error parsing embedded waves config: %v", err)
			}
			waves := wavesConfig.Waves
			if len(waves) == 0 {
				log.Fatal("Waves config file must contain at least one wave")
			}
			var cpuRequests []string
			var podCounts []int
			for i, w := range waves {
				if w.Pods <= 0 {
					log.Fatalf("Wave %d has invalid pods count: %d", i, w.Pods)
				}
				if w.CPURequest == "" {
					log.Fatalf("Wave %d has empty cpuRequest", i)
				}
				cpuRequests = append(cpuRequests, w.CPURequest)
				podCounts = append(podCounts, w.Pods)
			}
			log.Infof("Configured %d waves", len(waves))
			var grandTotal resource.Quantity
			totalPods := 0
			for i, w := range waves {
				perPod := resource.MustParse(w.CPURequest)
				waveTotal := perPod.DeepCopy()
				for j := 1; j < w.Pods; j++ {
					waveTotal.Add(perPod)
				}
				grandTotal.Add(waveTotal)
				totalPods += w.Pods
				log.Infof("  Wave %d: %d pods x %s cores = %s total cores", i, w.Pods, w.CPURequest, &waveTotal)
			}
			log.Infof("Total across all waves: %d pods, %s cores", totalPods, &grandTotal)
			AdditionalVars["JOB_ITERATIONS"] = len(waves)
			AdditionalVars["WAVE_DELAY"] = wavesConfig.Delay
			AdditionalVars["DELETION_STRATEGY"] = deletionStrategy
			AdditionalVars["POD_READY_THRESHOLD"] = podReadyThreshold
			AdditionalVars["JOB_PAUSE"] = jobPause
			AdditionalVars["CHURN_CYCLES"] = churnCycles
			AdditionalVars["CHURN_DURATION"] = churnDuration
			AdditionalVars["CHURN_DELAY"] = churnDelay
			AdditionalVars["CHURN_PERCENT"] = churnPercent
			AdditionalVars["CHURN_DELETE_DELAY"] = churnDeleteDelay
			AdditionalVars["CHURN_MODE"] = churnMode
			AdditionalVars["CPU_REQUESTS"] = cpuRequests
			AdditionalVars["POD_COUNTS"] = podCounts
			AdditionalVars["MEMORY_REQUEST"] = memoryRequest
			AdditionalVars["CONTAINER_IMAGE"] = containerImage
			setMetrics(cmd, metricsProfiles)
			wh.SetMeasurements(map[string]kubeburnermeasurements.NewMeasurementFactory{
				"autoNodeLatency":  ocpMeasurements.NewAutoNodeLatencyFactory,
				"nodeClaimLatency": ocpMeasurements.NewNodeClaimLatencyFactory,
			})
			wh.SetVariables(AdditionalVars, SetVars)
			rc = wh.Run(cmd.Name() + ".yml")
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			os.Exit(rc)
		},
	}
	cmd.Flags().StringVar(&wavesConfigFile, "waves-config", "waves", "Name of the waves config file (without .yml extension)")
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
