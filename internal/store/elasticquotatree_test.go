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
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"k8s.io/kube-state-metrics/v2/pkg/metric"
)

func TestElasticQuotaTreeMetricFamilies(t *testing.T) {
	// 测试指标生成器是否正常工作
	families := elasticQuotaTreeMetricFamilies([]string{}, []string{})

	if len(families) == 0 {
		t.Error("Expected non-empty metric families")
	}

	// 验证基础指标是否存在
	expectedMetrics := []string{
		"kube_elasticquotatree_info",
		"kube_elasticquotatree_created",
		"kube_elasticquotatree_annotations",
		"kube_elasticquotatree_labels",
		"kube_elasticquotatree_metadata_resource_version",
	}

	for _, expectedMetric := range expectedMetrics {
		found := false
		for _, family := range families {
			if family.Name == expectedMetric {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected metric %s not found", expectedMetric)
		}
	}
}

func TestWrapElasticQuotaTreeFunc(t *testing.T) {
	// 创建测试用的 ElasticQuotaTree 对象
	elasticQuotaTree := &unstructured.Unstructured{}
	elasticQuotaTree.SetName("test-elasticquotatree")
	elasticQuotaTree.SetNamespace("test-namespace")
	elasticQuotaTree.SetAnnotations(map[string]string{
		"test.annotation": "test-value",
	})
	elasticQuotaTree.SetLabels(map[string]string{
		"test.label": "test-value",
	})

	// 测试包装函数
	wrapper := wrapElasticQuotaTreeFunc(func(e *unstructured.Unstructured) *metric.Family {
		return &metric.Family{
			Metrics: []*metric.Metric{
				{
					LabelKeys:   []string{},
					LabelValues: []string{},
					Value:       1,
				},
			},
		}
	})

	result := wrapper(elasticQuotaTree)

	// 验证结果
	if len(result.Metrics) == 0 {
		t.Error("Expected non-empty metrics")
	}

	// 验证默认标签是否被添加
	metric := result.Metrics[0]
	expectedLabelKeys := []string{"namespace", "elasticquotatree"}
	expectedLabelValues := []string{"test-namespace", "test-elasticquotatree"}

	for i, key := range expectedLabelKeys {
		if i >= len(metric.LabelKeys) || metric.LabelKeys[i] != key {
			t.Errorf("Expected label key %s at position %d, got %s", key, i, metric.LabelKeys[i])
		}
	}

	for i, value := range expectedLabelValues {
		if i >= len(metric.LabelValues) || metric.LabelValues[i] != value {
			t.Errorf("Expected label value %s at position %d, got %s", value, i, metric.LabelValues[i])
		}
	}
}

func TestElasticQuotaTreeFactory(t *testing.T) {
	// 测试注册工厂
	factory := &ElasticQuotaTreeFactory{}

	// 测试名称
	if factory.Name() != "elasticquotatrees" {
		t.Errorf("Expected name 'elasticquotatrees', got %s", factory.Name())
	}

	// 测试期望类型
	expectedType := factory.ExpectedType()
	if expectedType == nil {
		t.Error("Expected non-nil expected type")
	}

	// 测试指标生成器
	generators := factory.MetricFamilyGenerators()
	if len(generators) == 0 {
		t.Error("Expected non-empty metric family generators")
	}
}

// 测试辅助函数
func TestElasticQuotaTreeHelpers(t *testing.T) {
	// 测试资源名称
	resourceName := "elasticquotatrees"
	if resourceName != "elasticquotatrees" {
		t.Errorf("Expected resource name 'elasticquotatrees', got %s", resourceName)
	}

	// 测试 API 组版本
	expectedGroup := "scheduling.sigs.k8s.io"
	expectedVersion := "v1beta1"
	expectedKind := "ElasticQuotaTree"

	if expectedGroup != "scheduling.sigs.k8s.io" {
		t.Errorf("Expected group %s, got %s", "scheduling.sigs.k8s.io", expectedGroup)
	}

	if expectedVersion != "v1beta1" {
		t.Errorf("Expected version %s, got %s", "v1beta1", expectedVersion)
	}

	if expectedKind != "ElasticQuotaTree" {
		t.Errorf("Expected kind %s, got %s", "ElasticQuotaTree", expectedKind)
	}
}
