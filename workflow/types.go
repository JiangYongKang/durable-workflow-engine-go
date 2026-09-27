// Package workflow 提供本地可运行的持久化工作流编排引擎。
package workflow

import (
	"context"
	"time"
)

// StepStatus 表示单个步骤的终态/中间态。
type StepStatus string

const (
	StatusPending   StepStatus = "pending"
	StatusRunning   StepStatus = "running"
	StatusSucceeded StepStatus = "succeeded"
	StatusFailed    StepStatus = "failed"
	StatusSkipped   StepStatus = "skipped"
	StatusCanceled  StepStatus = "canceled"
)

// Terminal 判断状态是否为终态。
func (s StepStatus) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusSkipped, StatusCanceled:
		return true
	}
	return false
}

// RunStatus 表示整个工作流运行的终态。
type RunStatus string

const (
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCanceled  RunStatus = "canceled"
)

// RetryPolicy 控制单个步骤的重试行为。
type RetryPolicy struct {
	MaxAttempts int           // 最大尝试次数（含首次），<=0 视为 1
	Backoff     time.Duration // 每次重试前的基础退避时间，线性增长
	Timeout     time.Duration // 单次尝试的超时时间，<=0 表示不限制
}

// Step 是工作流中的一个步骤定义。
type Step struct {
	Name    string
	Depends []string
	Retry   RetryPolicy
	// Run 是步骤的业务逻辑；返回 NonRetryable 包裹的错误会立即终止。
	Run func(ctx context.Context) error
}

// StepRecord 记录单个步骤在运行中的持久化状态。
type StepRecord struct {
	Status   StepStatus `json:"status"`
	Attempts int        `json:"attempts"`
	Err      string     `json:"err,omitempty"`
}

// RunRecord 是一次运行的完整持久化状态。
type RunRecord struct {
	RunID  string                 `json:"run_id"`
	Status RunStatus              `json:"status"`
	Steps  map[string]*StepRecord `json:"steps"`
}

// Config 是引擎级配置。
type Config struct {
	MaxParallel int // 同时运行的步骤上限，<=0 视为 1
}
