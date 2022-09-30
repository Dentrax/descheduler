/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package descheduler

import (
	"context"

	"sigs.k8s.io/descheduler/pkg/tracing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/framework"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/podlifetime"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removeduplicates"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removefailedpods"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodshavingtoomanyrestarts"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatinginterpodantiaffinity"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingnodeaffinity"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingnodetaints"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingtopologyspreadconstraint"
)

// Once all strategies are migrated the arguments get read from the configuration file
// without any wiring. Keeping the wiring here so the descheduler can still use
// the v1alpha1 configuration during the strategy migration to plugins.

var pluginsMap = map[string]func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl){
	"RemovePodsViolatingNodeTaints": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		args := &removepodsviolatingnodetaints.RemovePodsViolatingNodeTaintsArgs{
			Namespaces:              params.Namespaces,
			LabelSelector:           params.LabelSelector,
			IncludePreferNoSchedule: params.IncludePreferNoSchedule,
			ExcludedTaints:          params.ExcludedTaints,
		}
		if err := removepodsviolatingnodetaints.ValidateRemovePodsViolatingNodeTaintsArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", removepodsviolatingnodetaints.PluginName)
			return
		}
		pg, err := removepodsviolatingnodetaints.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", removepodsviolatingnodetaints.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.DeschedulePlugin).Deschedule, tracing.DescheduleOperation, removepodsviolatingnodetaints.PluginName)(ctx, nodes)
	},
	"RemoveFailedPods": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		failedPodsParams := params.FailedPods
		if failedPodsParams == nil {
			failedPodsParams = &api.FailedPods{}
		}
		args := &removefailedpods.RemoveFailedPodsArgs{
			Namespaces:              params.Namespaces,
			LabelSelector:           params.LabelSelector,
			IncludingInitContainers: failedPodsParams.IncludingInitContainers,
			MinPodLifetimeSeconds:   failedPodsParams.MinPodLifetimeSeconds,
			ExcludeOwnerKinds:       failedPodsParams.ExcludeOwnerKinds,
			Reasons:                 failedPodsParams.Reasons,
		}
		if err := removefailedpods.ValidateRemoveFailedPodsArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", removefailedpods.PluginName)
			return
		}
		pg, err := removefailedpods.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", removefailedpods.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.DeschedulePlugin).Deschedule, tracing.DescheduleOperation, removefailedpods.PluginName)(ctx, nodes)
	},
	"RemovePodsViolatingNodeAffinity": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		args := &removepodsviolatingnodeaffinity.RemovePodsViolatingNodeAffinityArgs{
			Namespaces:       params.Namespaces,
			LabelSelector:    params.LabelSelector,
			NodeAffinityType: params.NodeAffinityType,
		}
		if err := removepodsviolatingnodeaffinity.ValidateRemovePodsViolatingNodeAffinityArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", removepodsviolatingnodeaffinity.PluginName)
			return
		}
		pg, err := removepodsviolatingnodeaffinity.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", removepodsviolatingnodeaffinity.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.DeschedulePlugin).Deschedule, tracing.DescheduleOperation, removepodsviolatingnodeaffinity.PluginName)(ctx, nodes)
	},
	"RemovePodsViolatingInterPodAntiAffinity": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		args := &removepodsviolatinginterpodantiaffinity.RemovePodsViolatingInterPodAntiAffinityArgs{
			Namespaces:    params.Namespaces,
			LabelSelector: params.LabelSelector,
		}
		if err := removepodsviolatinginterpodantiaffinity.ValidateRemovePodsViolatingInterPodAntiAffinityArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", removepodsviolatinginterpodantiaffinity.PluginName)
			return
		}
		pg, err := removepodsviolatinginterpodantiaffinity.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", removepodsviolatinginterpodantiaffinity.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.DeschedulePlugin).Deschedule, tracing.DescheduleOperation, removepodsviolatinginterpodantiaffinity.PluginName)(ctx, nodes)
	},
	"RemovePodsHavingTooManyRestarts": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		tooManyRestartsParams := params.PodsHavingTooManyRestarts
		if tooManyRestartsParams == nil {
			tooManyRestartsParams = &api.PodsHavingTooManyRestarts{}
		}
		args := &removepodshavingtoomanyrestarts.RemovePodsHavingTooManyRestartsArgs{
			Namespaces:              params.Namespaces,
			LabelSelector:           params.LabelSelector,
			PodRestartThreshold:     tooManyRestartsParams.PodRestartThreshold,
			IncludingInitContainers: tooManyRestartsParams.IncludingInitContainers,
		}
		if err := removepodshavingtoomanyrestarts.ValidateRemovePodsHavingTooManyRestartsArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", removepodshavingtoomanyrestarts.PluginName)
			return
		}
		pg, err := removepodshavingtoomanyrestarts.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", removepodshavingtoomanyrestarts.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.DeschedulePlugin).Deschedule, tracing.DescheduleOperation, removepodshavingtoomanyrestarts.PluginName)(ctx, nodes)
	},
	"PodLifeTime": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		podLifeTimeParams := params.PodLifeTime
		if podLifeTimeParams == nil {
			podLifeTimeParams = &api.PodLifeTime{}
		}

		var states []string
		if podLifeTimeParams.PodStatusPhases != nil {
			states = append(states, podLifeTimeParams.PodStatusPhases...)
		}
		if podLifeTimeParams.States != nil {
			states = append(states, podLifeTimeParams.States...)
		}

		args := &podlifetime.PodLifeTimeArgs{
			Namespaces:            params.Namespaces,
			LabelSelector:         params.LabelSelector,
			MaxPodLifeTimeSeconds: podLifeTimeParams.MaxPodLifeTimeSeconds,
			States:                states,
		}
		if err := podlifetime.ValidatePodLifeTimeArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", podlifetime.PluginName)
			return
		}
		pg, err := podlifetime.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", podlifetime.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.DeschedulePlugin).Deschedule, tracing.DescheduleOperation, podlifetime.PluginName)(ctx, nodes)
	},
	"RemoveDuplicates": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		args := &removeduplicates.RemoveDuplicatesArgs{
			Namespaces: params.Namespaces,
		}
		if params.RemoveDuplicates != nil {
			args.ExcludeOwnerKinds = params.RemoveDuplicates.ExcludeOwnerKinds
		}
		if err := removeduplicates.ValidateRemoveDuplicatesArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", removeduplicates.PluginName)
			return
		}
		pg, err := removeduplicates.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", removeduplicates.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.BalancePlugin).Balance, tracing.BalanceOperation, removeduplicates.PluginName)(ctx, nodes)
	},
	"RemovePodsViolatingTopologySpreadConstraint": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		args := &removepodsviolatingtopologyspreadconstraint.RemovePodsViolatingTopologySpreadConstraintArgs{
			Namespaces:             params.Namespaces,
			LabelSelector:          params.LabelSelector,
			IncludeSoftConstraints: params.IncludeSoftConstraints,
		}
		if err := removepodsviolatingtopologyspreadconstraint.ValidateRemovePodsViolatingTopologySpreadConstraintArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", removepodsviolatingtopologyspreadconstraint.PluginName)
			return
		}
		pg, err := removepodsviolatingtopologyspreadconstraint.New(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", removepodsviolatingtopologyspreadconstraint.PluginName)
			return
		}
		RunPlugin(pg, pg.(framework.BalancePlugin).Balance, tracing.BalanceOperation, removepodsviolatingtopologyspreadconstraint.PluginName)(ctx, nodes)
	},
	"HighNodeUtilization": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		args := &nodeutilization.HighNodeUtilizationArgs{
			Thresholds:    params.NodeResourceUtilizationThresholds.Thresholds,
			NumberOfNodes: params.NodeResourceUtilizationThresholds.NumberOfNodes,
		}

		if err := nodeutilization.ValidateHighNodeUtilizationArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", nodeutilization.HighNodeUtilizationPluginName)
			return
		}
		pg, err := nodeutilization.NewHighNodeUtilization(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", nodeutilization.HighNodeUtilizationPluginName)
			return
		}
		RunPlugin(pg, pg.(framework.BalancePlugin).Balance, tracing.BalanceOperation, nodeutilization.HighNodeUtilizationPluginName)(ctx, nodes)
	},
	"LowNodeUtilization": func(ctx context.Context, nodes []*v1.Node, params *api.StrategyParameters, handle *handleImpl) {
		args := &nodeutilization.LowNodeUtilizationArgs{
			Thresholds:             params.NodeResourceUtilizationThresholds.Thresholds,
			TargetThresholds:       params.NodeResourceUtilizationThresholds.TargetThresholds,
			UseDeviationThresholds: params.NodeResourceUtilizationThresholds.UseDeviationThresholds,
			NumberOfNodes:          params.NodeResourceUtilizationThresholds.NumberOfNodes,
		}

		if err := nodeutilization.ValidateLowNodeUtilizationArgs(args); err != nil {
			klog.V(1).ErrorS(err, "unable to validate plugin arguments", "pluginName", nodeutilization.LowNodeUtilizationPluginName)
			return
		}
		pg, err := nodeutilization.NewLowNodeUtilization(args, handle)
		if err != nil {
			klog.V(1).ErrorS(err, "unable to initialize a plugin", "pluginName", nodeutilization.LowNodeUtilizationPluginName)
			return
		}
		RunPlugin(pg, pg.(framework.BalancePlugin).Balance, tracing.BalanceOperation, nodeutilization.LowNodeUtilizationPluginName)(ctx, nodes)
	},
}

// RunFunc represents a framework.Plugin function.
// - (framework.BalancePlugin).Balance
// - (framework.DeschedulePlugin).Deschedule
type RunFunc func(ctx context.Context, nodes []*v1.Node) *framework.Status

// RunPlugin runs a plugin.
func RunPlugin(plugin framework.Plugin, runFn RunFunc, operationName, pluginName string) func(ctx context.Context, nodes []*v1.Node) {
	return func(ctx context.Context, nodes []*v1.Node) {
		spanCtx, span, closer := tracing.StartSpan(ctx, plugin.Name(), operationName)
		defer closer()
		status := runFn(spanCtx, nodes)
		if status != nil && status.Err != nil {
			span.RecordError(status.Err)
			klog.V(1).ErrorS(status.Err, "plugin finished with error", "pluginName", pluginName)
		}
	}
}
