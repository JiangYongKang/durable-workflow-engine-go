package workflow

import "fmt"

// ErrorKind 区分加载期校验失败的原因类别。
type ErrorKind string

const (
	ErrKindDuplicateStep ErrorKind = "duplicate_step"
	ErrKindMissingDep    ErrorKind = "missing_dependency"
	ErrKindCycle         ErrorKind = "dependency_cycle"
)

// DefinitionError 是工作流定义在加载阶段被拒绝的错误。
type DefinitionError struct {
	Kind   ErrorKind
	Step   string
	Detail string
}

func (e *DefinitionError) Error() string {
	return fmt.Sprintf("workflow definition rejected (%s): step %q: %s", e.Kind, e.Step, e.Detail)
}

// NonRetryable 将错误标记为不可重试，步骤会立即终止并保留原始原因。
func NonRetryable(err error) error {
	return &nonRetryableError{err: err}
}

type nonRetryableError struct{ err error }

func (e *nonRetryableError) Error() string { return e.err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.err }

// IsNonRetryable 判断错误是否被标记为不可重试。
func IsNonRetryable(err error) bool {
	for err != nil {
		if _, ok := err.(*nonRetryableError); ok {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
