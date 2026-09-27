package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Engine 执行工作流并把状态持久化到 Store。
type Engine struct {
	cfg   Config
	store *Store
	steps map[string]Step
	order []string // 拓扑序（依赖在前）

	runMu sync.Mutex // 同一引擎同一时刻只允许一个 Run 在调度
	mu    sync.Mutex // 保护 rec
	rec   *RunRecord
}

// NewEngine 校验定义并创建引擎；校验失败返回 *DefinitionError。
func NewEngine(cfg Config, store *Store, steps []Step) (*Engine, error) {
	if err := Validate(steps); err != nil {
		return nil, err
	}
	order, err := topoOrder(steps)
	if err != nil {
		return nil, err
	}
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = 1
	}
	m := make(map[string]Step, len(steps))
	for _, s := range steps {
		m[s.Name] = s
	}
	return &Engine{cfg: cfg, store: store, steps: m, order: order}, nil
}

// Run 执行工作流直到终态。
// 可重入：同一 runID 重复调用（含进程重启后）会从未完成的步骤继续，
// 已成功生效的步骤不会再次执行。返回的 *RunRecord 是唯一终态。
func (e *Engine) Run(ctx context.Context, runID string) (*RunRecord, error) {
	e.runMu.Lock()
	defer e.runMu.Unlock()

	rec, err := e.store.Load()
	if err != nil {
		return nil, fmt.Errorf("load run state: %w", err)
	}
	if rec != nil && rec.RunID != runID {
		return nil, fmt.Errorf("store already holds run %q, cannot run %q", rec.RunID, runID)
	}
	if rec == nil {
		rec = &RunRecord{RunID: runID, Status: RunRunning, Steps: make(map[string]*StepRecord, len(e.order))}
		for _, name := range e.order {
			rec.Steps[name] = &StepRecord{Status: StatusPending}
		}
		if err := e.store.Save(rec); err != nil {
			return nil, err
		}
	}
	// 崩溃恢复：上次运行时处于 running 的步骤效果未知，重置为 pending 重跑；
	// 已 succeeded 的步骤保持终态，不会再次生效。
	for _, st := range rec.Steps {
		if st.Status == StatusRunning {
			st.Status = StatusPending
		}
	}
	e.rec = rec
	if rec.Status != RunRunning {
		return rec, nil // 已是终态，幂等返回
	}
	if err := e.store.Save(rec); err != nil {
		return nil, err
	}

	e.schedule(ctx)

	// 汇总唯一终态
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case ctx.Err() != nil:
		rec.Status = RunCanceled
	default:
		rec.Status = RunSucceeded
		for _, name := range e.order {
			s := rec.Steps[name].Status
			if s == StatusFailed || s == StatusSkipped {
				rec.Status = RunFailed
				break
			}
		}
	}
	if err := e.store.Save(rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// schedule 是调度主循环：按拓扑序推进状态机，用信号量限制并发。
// ctx 取消后不再调度新步骤，等待在跑步骤结束后把所有 pending 标记为 canceled。
func (e *Engine) schedule(ctx context.Context) {
	sem := make(chan struct{}, e.cfg.MaxParallel)
	doneCh := make(chan string, len(e.order))
	running := 0

	for {
		canceled := ctx.Err() != nil
		for _, name := range e.order {
			e.mu.Lock()
			st := e.rec.Steps[name]
			if st.Status != StatusPending {
				e.mu.Unlock()
				continue
			}
			depsDone, depsOK := true, true
			for _, d := range e.steps[name].Depends {
				ds := e.rec.Steps[d].Status
				if !ds.Terminal() {
					depsDone = false
				}
				if ds != StatusSucceeded {
					depsOK = false
				}
			}
			switch {
			case !depsDone:
				e.mu.Unlock()
			case canceled:
				// 取消后不再调度新步骤，pending 一律标记为 canceled
				st.Status = StatusCanceled
				e.store.Save(e.rec)
				e.mu.Unlock()
			case !depsOK:
				st.Status = StatusSkipped
				e.store.Save(e.rec)
				e.mu.Unlock()
			default:
				select {
				case sem <- struct{}{}:
					st.Status = StatusRunning
					e.store.Save(e.rec)
					e.mu.Unlock()
					running++
					go func(n string) {
						e.executeStep(ctx, n)
						<-sem
						doneCh <- n
					}(name)
				default:
					e.mu.Unlock()
				}
			}
		}
		if running == 0 {
			// 一次完整扫描且没有在跑步骤：按拓扑序归纳可知所有步骤均已终态。
			return
		}
		<-doneCh
		running--
	}
}

// executeStep 执行单个步骤，带重试、退避和单次尝试超时。
// 只在此 goroutine 内写该步骤的记录，终态唯一。
func (e *Engine) executeStep(ctx context.Context, name string) {
	step := e.steps[name]
	p := step.Retry
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}

	var runErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			runErr = ctx.Err()
			break
		}
		e.mu.Lock()
		e.rec.Steps[name].Attempts = attempt
		e.store.Save(e.rec)
		e.mu.Unlock()

		attemptCtx := ctx
		cancel := func() {}
		if p.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, p.Timeout)
		}
		runErr = step.Run(attemptCtx)
		cancel()

		if runErr == nil {
			e.finishStep(name, StatusSucceeded, "")
			return
		}
		if IsNonRetryable(runErr) || ctx.Err() != nil {
			break // 不可重试或整体取消：立即终止，保留原始原因
		}
		if attempt < p.MaxAttempts && p.Backoff > 0 {
			timer := time.NewTimer(p.Backoff * time.Duration(attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}

	if ctx.Err() != nil && !IsNonRetryable(runErr) {
		e.finishStep(name, StatusCanceled, runErr.Error())
		return
	}
	e.finishStep(name, StatusFailed, runErr.Error())
}

func (e *Engine) finishStep(name string, status StepStatus, errMsg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.rec.Steps[name]
	st.Status = status
	st.Err = errMsg
	e.store.Save(e.rec)
}
