package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Engine 按依赖关系调度并执行工作流步骤。
// 对同一 runID 的并发 Run 调用会被串行化；不同 runID 可并行。
type Engine struct {
	def           *Definition
	store         *Store
	maxConcurrent int

	specs map[string]*StepSpec
	order []string

	mu       sync.Mutex
	inflight map[string]chan struct{} // runID -> 执行结束信号，用于串行化同一 run
}

// Option 配置 Engine。
type Option func(*Engine)

// WithMaxConcurrent 限制同时运行的步骤数量，<=0 表示不限制。
func WithMaxConcurrent(n int) Option {
	return func(e *Engine) { e.maxConcurrent = n }
}

// NewEngine 创建工作流引擎；定义非法时在加载阶段即拒绝。
func NewEngine(def *Definition, store *Store, opts ...Option) (*Engine, error) {
	if def == nil {
		return nil, fmt.Errorf("workflow: nil definition")
	}
	if err := def.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("workflow: nil store")
	}
	e := &Engine{
		def:      def,
		store:    store,
		specs:    make(map[string]*StepSpec, len(def.Steps)),
		order:    def.topoOrder(),
		inflight: map[string]chan struct{}{},
	}
	for i := range def.Steps {
		s := &def.Steps[i]
		e.specs[s.Name] = s
	}
	for _, o := range opts {
		o(e)
	}
	return e, nil
}

// Run 执行（或恢复）一次运行，直到到达唯一终态。
//
// 幂等与恢复语义：
//   - 已 Succeeded 的步骤不会再次执行（重复调用或进程重启都一样）；
//   - 上次崩溃时处于 Running 的步骤会被重置为 Pending 重新调度，
//     因此 Handler 需要自身幂等（at-least-once 语义）；
//   - 已终态（succeeded/failed/cancelled）的运行直接返回，不做任何调度。
//
// ctx 取消或超时后不再调度新步骤；已在运行的步骤会跑完并落盘，
// 运行终态为 cancelled。
func (e *Engine) Run(ctx context.Context, runID string) (*RunState, error) {
	unlock := e.lockRun(runID)
	defer unlock()

	state, err := e.store.LoadRun(runID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		state = &RunState{RunID: runID, Status: RunRunning, Steps: map[string]*StepRecord{}}
		for _, name := range e.order {
			state.Steps[name] = &StepRecord{Status: StepPending}
		}
		if err := e.store.SaveRun(state); err != nil {
			return nil, err
		}
	}
	if isTerminalRun(state.Status) {
		return state, terminalErr(state.Status)
	}
	// 崩溃恢复：Running 说明上次进程退出时未落终态，重置为 Pending。
	for _, rec := range state.Steps {
		if rec.Status == StepRunning {
			rec.Status = StepPending
		}
	}
	state.Status = RunRunning
	if err := e.store.SaveRun(state); err != nil {
		return nil, err
	}

	rs := &runSession{engine: e, state: state}
	rs.execute(ctx)
	return state, terminalErr(state.Status)
}

// runSession 持有单次执行的共享状态，所有对 state 的访问都经 mu 保护。
type runSession struct {
	engine *Engine
	mu     sync.Mutex
	state  *RunState
}

func (rs *runSession) saveLocked() {
	_ = rs.engine.store.SaveRun(rs.state)
}

// execute 是调度主循环：将就绪步骤派发给 worker，直到终态。
func (rs *runSession) execute(ctx context.Context) {
	e := rs.engine
	var sem chan struct{}
	if e.maxConcurrent > 0 {
		sem = make(chan struct{}, e.maxConcurrent)
	}
	type result struct{ fail bool }
	results := make(chan result, len(e.order))
	var wg sync.WaitGroup
	running := 0
	stopped := false // 停止调度新步骤（失败、取消或超时）

	for {
		rs.mu.Lock()
		// 1. 传播失败：依赖未成功的步骤标记为 skipped。
		for _, name := range e.order {
			rec := rs.state.Steps[name]
			if rec.Status != StepPending {
				continue
			}
			for _, dep := range e.specs[name].DependsOn {
				ds := rs.state.Steps[dep].Status
				if ds == StepFailed || ds == StepSkipped {
					rec.Status = StepSkipped
					rec.LastErr = fmt.Sprintf("%v: %s", ErrDependencyFailed, dep)
					break
				}
			}
		}

		// 2. 取消/超时后不再调度新步骤。
		if !stopped && ctx.Err() != nil {
			stopped = true
		}

		// 3. 找出就绪步骤（在锁内判定，在锁外获取信号量，避免持锁阻塞）。
		var ready []string
		if !stopped {
			for _, name := range e.order {
				if rs.state.Steps[name].Status == StepPending && rs.readyLocked(name) {
					ready = append(ready, name)
				}
			}
		}
		rs.saveLocked()
		rs.mu.Unlock()

		for _, name := range ready {
			if sem != nil {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					stopped = true
				}
				if stopped {
					break
				}
			}
			rs.mu.Lock()
			// 重新校验：等待信号量期间状态可能已变化。
			if rs.state.Steps[name].Status != StepPending || !rs.readyLocked(name) {
				rs.mu.Unlock()
				if sem != nil {
					<-sem
				}
				continue
			}
			rs.state.Steps[name].Status = StepRunning
			rs.saveLocked()
			rs.mu.Unlock()
			running++
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				fail := rs.runStep(ctx, name)
				if sem != nil {
					<-sem
				}
				results <- result{fail: fail}
			}(name)
		}

		// 4. 判断是否可以退出循环。
		rs.mu.Lock()
		pending := 0
		failed := false
		for _, name := range e.order {
			switch rs.state.Steps[name].Status {
			case StepPending:
				pending++
			case StepFailed:
				failed = true
			}
		}
		if failed {
			stopped = true
		}
		done := (stopped || pending == 0) && running == 0
		rs.mu.Unlock()
		if done {
			break
		}

		// 5. 等待任一运行中的步骤完成。
		res := <-results
		rs.mu.Lock()
		running--
		if res.fail {
			stopped = true
		}
		rs.mu.Unlock()
	}
	wg.Wait()

	rs.mu.Lock()
	defer rs.mu.Unlock()
	// 取消时仍未调度的步骤统一标记 skipped，保证没有半完成状态。
	if ctx.Err() != nil {
		for _, name := range e.order {
			if rs.state.Steps[name].Status == StepPending {
				rs.state.Steps[name].Status = StepSkipped
				rs.state.Steps[name].LastErr = ErrRunCancelled.Error()
			}
		}
	}

	// 计算唯一终态。
	switch {
	case ctx.Err() != nil:
		rs.state.Status = RunCancelled
	default:
		allOK := true
		for _, name := range e.order {
			if rs.state.Steps[name].Status != StepSucceeded {
				allOK = false
				break
			}
		}
		if allOK {
			rs.state.Status = RunSucceeded
		} else {
			rs.state.Status = RunFailed
		}
	}
	rs.saveLocked()
}

// readyLocked 判断步骤的所有依赖是否都已成功。调用方需持有 rs.mu。
func (rs *runSession) readyLocked(name string) bool {
	for _, dep := range rs.engine.specs[name].DependsOn {
		if rs.state.Steps[dep].Status != StepSucceeded {
			return false
		}
	}
	return true
}

// runStep 按重试策略执行单个步骤，返回是否最终失败。
// 每次状态变更都会落盘，保证重启后可恢复。
func (rs *runSession) runStep(ctx context.Context, name string) (failed bool) {
	spec := rs.engine.specs[name]

	maxAttempts := spec.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	backoff := spec.Retry.Backoff
	mult := spec.Retry.Multiplier
	if mult <= 0 {
		mult = 1
	}

	for {
		rs.mu.Lock()
		rec := rs.state.Steps[name]
		if rec.Attempts >= maxAttempts {
			rec.Status = StepFailed
			rs.saveLocked()
			rs.mu.Unlock()
			return true
		}
		if ctx.Err() != nil {
			// 取消/超时：不开始新的尝试；重置为 pending 以便恢复时重新调度。
			rec.Status = StepPending
			rs.saveLocked()
			rs.mu.Unlock()
			return false
		}
		rec.Attempts++
		rs.saveLocked()
		rs.mu.Unlock()

		attemptCtx := ctx
		cancel := func() {}
		if spec.Retry.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, spec.Retry.Timeout)
		}
		err := spec.Handler(attemptCtx)
		cancel()

		rs.mu.Lock()
		if err == nil {
			rec.Status = StepSucceeded
			rec.LastErr = ""
			rs.saveLocked()
			rs.mu.Unlock()
			return false
		}
		rec.LastErr = err.Error()
		if IsPermanent(err) {
			rec.Status = StepFailed
			rs.saveLocked()
			rs.mu.Unlock()
			return true
		}
		rs.saveLocked()
		rs.mu.Unlock()

		if backoff > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			backoff = time.Duration(float64(backoff) * mult)
		}
	}
}

// lockRun 串行化同一 runID 的并发 Run 调用。
func (e *Engine) lockRun(runID string) func() {
	e.mu.Lock()
	ch, ok := e.inflight[runID]
	for ok {
		e.mu.Unlock()
		<-ch
		e.mu.Lock()
		ch, ok = e.inflight[runID]
	}
	done := make(chan struct{})
	e.inflight[runID] = done
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		delete(e.inflight, runID)
		e.mu.Unlock()
		close(done)
	}
}

func isTerminalRun(s RunStatus) bool {
	return s == RunSucceeded || s == RunFailed || s == RunCancelled
}

func terminalErr(s RunStatus) error {
	switch s {
	case RunFailed:
		return ErrStepFailed
	case RunCancelled:
		return ErrRunCancelled
	default:
		return nil
	}
}
