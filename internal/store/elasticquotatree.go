/*
Copyright 2024 The Kubernetes Authors All rights reserved.

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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	basemetrics "k8s.io/component-base/metrics"

	"k8s.io/kube-state-metrics/v2/pkg/metric"
	generator "k8s.io/kube-state-metrics/v2/pkg/metric_generator"
)

var (
	descElasticQuotaTreeLabelsDefaultLabels = []string{"namespace", "elasticquotatree"}
)

func elasticQuotaTreeMetricFamilies(allowAnnotationsList, allowLabelsList []string) []generator.FamilyGenerator {
	return []generator.FamilyGenerator{
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_annotations",
			"Kubernetes annotations converted to Prometheus labels.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				if len(allowAnnotationsList) == 0 {
					return &metric.Family{}
				}
				annotations := e.GetAnnotations()
				annotationKeys, annotationValues := createPrometheusLabelKeysValues("annotation", annotations, allowAnnotationsList)
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							LabelKeys:   annotationKeys,
							LabelValues: annotationValues,
							Value:       1,
						},
					},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_labels",
			"Kubernetes labels converted to Prometheus labels.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				if len(allowLabelsList) == 0 {
					return &metric.Family{}
				}
				labels := e.GetLabels()
				labelKeys, labelValues := createPrometheusLabelKeysValues("label", labels, allowLabelsList)
				return &metric.Family{
					Metrics: []*metric.Metric{
						{
							LabelKeys:   labelKeys,
							LabelValues: labelValues,
							Value:       1,
						},
					},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_info",
			"Information about elasticquotatree.",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return &metric.Family{
					Metrics: []*metric.Metric{{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       1,
					}},
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_created",
			"Unix creation timestamp",
			metric.Gauge,
			basemetrics.STABLE,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				ms := []*metric.Metric{}

				creationTimestamp := e.GetCreationTimestamp()
				if !creationTimestamp.IsZero() {
					ms = append(ms, &metric.Metric{
						LabelKeys:   []string{},
						LabelValues: []string{},
						Value:       float64(creationTimestamp.Unix()),
					})
				}

				return &metric.Family{
					Metrics: ms,
				}
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_metadata_resource_version",
			"Resource version representing a specific version of the elasticquotatree.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return &metric.Family{
					Metrics: resourceVersionMetric(e.GetResourceVersion()),
				}
			}),
		),
		// 队列基本信息指标
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_info",
			"Information about ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueInfoMetrics(e)
			}),
		),
		// 队列资源配额指标 - Spec
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_spec_max_cpu",
			"Maximum CPU quota for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueSpecMaxCPUMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_spec_max_memory",
			"Maximum memory quota for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueSpecMaxMemoryMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_spec_min_cpu",
			"Minimum CPU quota for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueSpecMinCPUMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_spec_min_memory",
			"Minimum memory quota for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueSpecMinMemoryMetrics(e)
			}),
		),
		// 队列资源使用指标 - Status
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_used_cpu",
			"Used CPU for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusUsedCPUMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_used_memory",
			"Used memory for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusUsedMemoryMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_request_cpu",
			"Requested CPU for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusRequestCPUMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_request_memory",
			"Requested memory for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusRequestMemoryMetrics(e)
			}),
		),
		// 非抢占式资源指标
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_nonpreemptible_used_cpu",
			"Non-preemptible used CPU for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusNonPreemptibleUsedCPUMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_nonpreemptible_used_memory",
			"Non-preemptible used memory for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusNonPreemptibleUsedMemoryMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_nonpreemptible_request_cpu",
			"Non-preemptible requested CPU for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusNonPreemptibleRequestCPUMetrics(e)
			}),
		),
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_status_nonpreemptible_request_memory",
			"Non-preemptible requested memory for ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueStatusNonPreemptibleRequestMemoryMetrics(e)
			}),
		),
		// 队列命名空间数量指标
		*generator.NewFamilyGeneratorWithStability(
			"kube_elasticquotatree_queue_namespace_count",
			"Number of namespaces in ElasticQuotaTree queue.",
			metric.Gauge,
			basemetrics.ALPHA,
			"",
			wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
				return generateQueueNamespaceCountMetrics(e)
			}),
		),
	}
}

func createElasticQuotaTreeListWatch(dynamicClient dynamic.Interface, ns string, fieldSelector string) cache.ListerWatcher {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.FieldSelector = fieldSelector
			return dynamicClient.Resource(schema.GroupVersionResource{
				Group:    "scheduling.sigs.k8s.io",
				Version:  "v1beta1",
				Resource: "elasticquotatrees",
			}).Namespace(ns).List(context.TODO(), opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.FieldSelector = fieldSelector
			return dynamicClient.Resource(schema.GroupVersionResource{
				Group:    "scheduling.sigs.k8s.io",
				Version:  "v1beta1",
				Resource: "elasticquotatrees",
			}).Namespace(ns).Watch(context.TODO(), opts)
		},
	}
}

func wrapElasticQuotaTreeFunc(f func(*unstructured.Unstructured) *metric.Family) func(interface{}) *metric.Family {
	return func(obj interface{}) *metric.Family {
		elasticQuotaTree := obj.(*unstructured.Unstructured)

		metricFamily := f(elasticQuotaTree)

		for _, m := range metricFamily.Metrics {
			m.LabelKeys, m.LabelValues = mergeKeyValues(descElasticQuotaTreeLabelsDefaultLabels, []string{elasticQuotaTree.GetNamespace(), elasticQuotaTree.GetName()}, m.LabelKeys, m.LabelValues)
		}

		return metricFamily
	}
}

// 队列节点结构
type queueNode struct {
	Name                  string                 `json:"name"`
	Max                   map[string]interface{} `json:"max,omitempty"`
	Min                   map[string]interface{} `json:"min,omitempty"`
	Children              []queueNode            `json:"children,omitempty"`
	Namespaces            []string               `json:"namespaces,omitempty"`
	NonPreemptibleRequest map[string]interface{} `json:"nonPreemptibleRequest,omitempty"`
	NonPreemptibleUsed    map[string]interface{} `json:"nonPreemptibleUsed,omitempty"`
	Request               map[string]interface{} `json:"request,omitempty"`
	Used                  map[string]interface{} `json:"used,omitempty"`
}

// 递归解析队列树并生成指标
func parseQueueTree(node queueNode, parentPath string, metrics []*metric.Metric, metricType string) []*metric.Metric {
	currentPath := node.Name
	if parentPath != "" {
		currentPath = parentPath + "/" + node.Name
	}

	// 添加队列信息指标
	metrics = append(metrics, &metric.Metric{
		LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
		LabelValues: []string{node.Name, currentPath, parentPath},
		Value:       1,
	})

	// 根据指标类型添加相应的资源指标
	switch metricType {
	case "spec_max_cpu":
		if cpu, found := node.Max["cpu"]; found {
			if cpuStr, ok := cpu.(string); ok {
				if cpuValue, err := parseResourceValue(cpuStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       cpuValue,
					})
				}
			}
		}
	case "spec_max_memory":
		if memory, found := node.Max["memory"]; found {
			if memoryStr, ok := memory.(string); ok {
				if memoryValue, err := parseMemoryValue(memoryStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       memoryValue,
					})
				}
			}
		}
	case "spec_min_cpu":
		if cpu, found := node.Min["cpu"]; found {
			if cpuStr, ok := cpu.(string); ok {
				if cpuValue, err := parseResourceValue(cpuStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       cpuValue,
					})
				}
			}
		}
	case "spec_min_memory":
		if memory, found := node.Min["memory"]; found {
			if memoryStr, ok := memory.(string); ok {
				if memoryValue, err := parseMemoryValue(memoryStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       memoryValue,
					})
				}
			}
		}
	case "status_used_cpu":
		if cpu, found := node.Used["cpu"]; found {
			if cpuStr, ok := cpu.(string); ok {
				if cpuValue, err := parseResourceValue(cpuStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       cpuValue,
					})
				}
			}
		}
	case "status_used_memory":
		if memory, found := node.Used["memory"]; found {
			if memoryStr, ok := memory.(string); ok {
				if memoryValue, err := parseMemoryValue(memoryStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       memoryValue,
					})
				}
			}
		}
	case "status_request_cpu":
		if cpu, found := node.Request["cpu"]; found {
			if cpuStr, ok := cpu.(string); ok {
				if cpuValue, err := parseResourceValue(cpuStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       cpuValue,
					})
				}
			}
		}
	case "status_request_memory":
		if memory, found := node.Request["memory"]; found {
			if memoryStr, ok := memory.(string); ok {
				if memoryValue, err := parseMemoryValue(memoryStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       memoryValue,
					})
				}
			}
		}
	case "status_nonpreemptible_used_cpu":
		if cpu, found := node.NonPreemptibleUsed["cpu"]; found {
			if cpuStr, ok := cpu.(string); ok {
				if cpuValue, err := parseResourceValue(cpuStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       cpuValue,
					})
				}
			}
		}
	case "status_nonpreemptible_used_memory":
		if memory, found := node.NonPreemptibleUsed["memory"]; found {
			if memoryStr, ok := memory.(string); ok {
				if memoryValue, err := parseMemoryValue(memoryStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       memoryValue,
					})
				}
			}
		}
	case "status_nonpreemptible_request_cpu":
		if cpu, found := node.NonPreemptibleRequest["cpu"]; found {
			if cpuStr, ok := cpu.(string); ok {
				if cpuValue, err := parseResourceValue(cpuStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       cpuValue,
					})
				}
			}
		}
	case "status_nonpreemptible_request_memory":
		if memory, found := node.NonPreemptibleRequest["memory"]; found {
			if memoryStr, ok := memory.(string); ok {
				if memoryValue, err := parseMemoryValue(memoryStr); err == nil {
					metrics = append(metrics, &metric.Metric{
						LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
						LabelValues: []string{node.Name, currentPath, parentPath},
						Value:       memoryValue,
					})
				}
			}
		}
	case "namespace_count":
		metrics = append(metrics, &metric.Metric{
			LabelKeys:   []string{"queue_name", "queue_path", "parent_queue"},
			LabelValues: []string{node.Name, currentPath, parentPath},
			Value:       float64(len(node.Namespaces)),
		})
	}

	// 递归处理子队列
	for _, child := range node.Children {
		metrics = parseQueueTree(child, currentPath, metrics, metricType)
	}

	return metrics
}

// 解析资源值（CPU）
func parseResourceValue(value string) (float64, error) {
	// 移除引号
	value = strings.Trim(value, "\"")
	return strconv.ParseFloat(value, 64)
}

// 解析内存值
func parseMemoryValue(value string) (float64, error) {
	// 移除引号
	value = strings.Trim(value, "\"")

	// 处理不同的内存单位
	if strings.HasSuffix(value, "Gi") {
		value = strings.TrimSuffix(value, "Gi")
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			return num * 1024 * 1024 * 1024, nil // 转换为字节
		}
	} else if strings.HasSuffix(value, "Mi") {
		value = strings.TrimSuffix(value, "Mi")
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			return num * 1024 * 1024, nil // 转换为字节
		}
	} else if strings.HasSuffix(value, "Ki") {
		value = strings.TrimSuffix(value, "Ki")
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			return num * 1024, nil // 转换为字节
		}
	}

	// 尝试直接解析为数字
	return strconv.ParseFloat(value, 64)
}

// 从 ElasticQuotaTree 对象中提取队列树
func extractQueueTree(e *unstructured.Unstructured, path string) (queueNode, error) {
	var node queueNode

	data, found, err := unstructured.NestedMap(e.Object, strings.Split(path, ".")...)
	if !found || err != nil {
		return node, fmt.Errorf("failed to extract queue tree from path %s: %v", path, err)
	}

	// 将 map 转换为 JSON 再解析为结构体
	jsonData, err := json.Marshal(data)
	if err != nil {
		return node, fmt.Errorf("failed to marshal queue tree data: %v", err)
	}

	err = json.Unmarshal(jsonData, &node)
	if err != nil {
		return node, fmt.Errorf("failed to unmarshal queue tree data: %v", err)
	}

	return node, nil
}

// 指标生成函数
func generateQueueInfoMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "spec.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "info")
	return &metric.Family{Metrics: metrics}
}

func generateQueueSpecMaxCPUMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "spec.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "spec_max_cpu")
	return &metric.Family{Metrics: metrics}
}

func generateQueueSpecMaxMemoryMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "spec.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "spec_max_memory")
	return &metric.Family{Metrics: metrics}
}

func generateQueueSpecMinCPUMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "spec.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "spec_min_cpu")
	return &metric.Family{Metrics: metrics}
}

func generateQueueSpecMinMemoryMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "spec.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "spec_min_memory")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusUsedCPUMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_used_cpu")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusUsedMemoryMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_used_memory")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusRequestCPUMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_request_cpu")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusRequestMemoryMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_request_memory")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusNonPreemptibleUsedCPUMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_nonpreemptible_used_cpu")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusNonPreemptibleUsedMemoryMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_nonpreemptible_used_memory")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusNonPreemptibleRequestCPUMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_nonpreemptible_request_cpu")
	return &metric.Family{Metrics: metrics}
}

func generateQueueStatusNonPreemptibleRequestMemoryMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "status.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "status_nonpreemptible_request_memory")
	return &metric.Family{Metrics: metrics}
}

func generateQueueNamespaceCountMetrics(e *unstructured.Unstructured) *metric.Family {
	root, err := extractQueueTree(e, "spec.root")
	if err != nil {
		return &metric.Family{}
	}

	metrics := parseQueueTree(root, "", []*metric.Metric{}, "namespace_count")
	return &metric.Family{Metrics: metrics}
}
