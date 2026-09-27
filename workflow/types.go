// Package workflow 提供本地可运行的持久化工作流编排引擎。
package workflow

import (
	"context"
	"errors"
	"time"
)

// StepStatus 表示单个步骤在其生命周期内的状态。
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
)

// RunStatus 表示一次工作流运行的终态或进行中状态。
type RunStatus string

const (
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

// Handler 是步骤的业务逻辑。返回 nil 表示成功；
// 返回 PermanentError 包装的错误表示不可重试，立即终止。
type Handler func(ctx context.Context) error

// RetryPolicy 控制步骤失败后的重试行为。
type RetryPolicy struct {
	MaxAttempts int           // 最大尝试次数（含首次），<=0 视为 1
	Backoff     time.Duration // 首次重试前的退避时间，之后按 Multiplier 倍增
	Multiplier  float64       // 退避倍增因子，<=0 视为 1（固定退避）
	Timeout     time.Duration // 单次尝试的超时时间，<=0 表示不限制
}

// StepSpec 描述一个步骤的静态定义。
type StepSpec struct {
	Name      string
	DependsOn []string
	Handler   Handler
	Retry     RetryPolicy
}

// Definition 是一组步骤的集合，加载时需要通过 Validate。
type Definition struct {
	Steps []StepSpec
}

// 加载阶段的校验错误，可用 errors.Is 区分原因。
var (
	ErrDuplicateStep     = errors.New("workflow: duplicate step definition")
	ErrUnknownDependency = errors.New("workflow: dependency references unknown step")
	ErrCyclicDependency  = errors.New("workflow: dependency cycle detected")
	ErrStepFailed        = errors.New("workflow: step failed")
	ErrRunCancelled      = errors.New("workflow: run cancelled")
	ErrDependencyFailed  = errors.New("workflow: dependency did not succeed")
)

// PermanentError 包装一个错误，标记其为不可重试。
func PermanentError(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// IsPermanent 判断错误是否被标记为不可重试。
func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}

// StepRecord 是单个步骤的持久化运行记录。
type StepRecord struct {
	Status   StepStatus `json:"status"`
	Attempts int        `json:"attempts"`
	LastErr  string     `json:"last_err,omitempty"`
}

// RunState 是一次运行的完整持久化状态。
type RunState struct {
	RunID  string                 `json:"run_id"`
	Status RunStatus              `json:"status"`
	Steps  map[string]*StepRecord `json:"steps"`
}
