package workflow

import (
	"context"
	"errors"
	"testing"
)

func noopHandler(context.Context) error { return nil }

func TestValidateDuplicateStep(t *testing.T) {
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: noopHandler},
		{Name: "a", Handler: noopHandler},
	}}
	err := def.Validate()
	t.Logf("输入: 两个同名步骤 a; 判定依据: errors.Is(err, ErrDuplicateStep)")
	if !errors.Is(err, ErrDuplicateStep) {
		t.Fatalf("期望 ErrDuplicateStep，得到 %v", err)
	}
	if errors.Is(err, ErrUnknownDependency) || errors.Is(err, ErrCyclicDependency) {
		t.Fatalf("错误原因不可区分: %v", err)
	}
}

func TestValidateUnknownDependency(t *testing.T) {
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: noopHandler, DependsOn: []string{"ghost"}},
	}}
	err := def.Validate()
	t.Logf("输入: 步骤 a 依赖不存在的 ghost; 判定依据: errors.Is(err, ErrUnknownDependency)")
	if !errors.Is(err, ErrUnknownDependency) {
		t.Fatalf("期望 ErrUnknownDependency，得到 %v", err)
	}
	if errors.Is(err, ErrDuplicateStep) || errors.Is(err, ErrCyclicDependency) {
		t.Fatalf("错误原因不可区分: %v", err)
	}
}

func TestValidateCyclicDependency(t *testing.T) {
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: noopHandler, DependsOn: []string{"c"}},
		{Name: "b", Handler: noopHandler, DependsOn: []string{"a"}},
		{Name: "c", Handler: noopHandler, DependsOn: []string{"b"}},
	}}
	err := def.Validate()
	t.Logf("输入: a->c->b->a 构成环; 判定依据: errors.Is(err, ErrCyclicDependency)")
	if !errors.Is(err, ErrCyclicDependency) {
		t.Fatalf("期望 ErrCyclicDependency，得到 %v", err)
	}
	if errors.Is(err, ErrDuplicateStep) || errors.Is(err, ErrUnknownDependency) {
		t.Fatalf("错误原因不可区分: %v", err)
	}
}

func TestValidateSelfDependency(t *testing.T) {
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: noopHandler, DependsOn: []string{"a"}},
	}}
	err := def.Validate()
	t.Logf("输入: 步骤 a 依赖自身; 判定依据: errors.Is(err, ErrCyclicDependency)")
	if !errors.Is(err, ErrCyclicDependency) {
		t.Fatalf("期望 ErrCyclicDependency，得到 %v", err)
	}
}

func TestValidateOKAndTopoOrder(t *testing.T) {
	def := &Definition{Steps: []StepSpec{
		{Name: "c", Handler: noopHandler, DependsOn: []string{"a", "b"}},
		{Name: "a", Handler: noopHandler},
		{Name: "b", Handler: noopHandler, DependsOn: []string{"a"}},
	}}
	if err := def.Validate(); err != nil {
		t.Fatalf("合法定义被拒绝: %v", err)
	}
	order := def.topoOrder()
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	t.Logf("输入: c 依赖 a,b; b 依赖 a; 拓扑序: %v", order)
	if !(pos["a"] < pos["b"] && pos["b"] < pos["c"]) {
		t.Fatalf("拓扑序违反依赖: %v", order)
	}
}
