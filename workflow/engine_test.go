package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// logDecision 在单测日志里打印步骤的输入与判定依据。
func logDecision(t *testing.T, step, input, basis string) {
	t.Helper()
	t.Logf("step=%s input=%s decision-basis=%s", step, input, basis)
}

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir() + "/run.json")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

// TestValidate 校验三类加载期拒绝原因可区分。
func TestValidate(t *testing.T) {
	noop := func(context.Context) error { return nil }
	cases := []struct {
		name  string
		steps []Step
		kind  ErrorKind
	}{
		{"duplicate", []Step{{Name: "a", Run: noop}, {Name: "a", Run: noop}}, ErrKindDuplicateStep},
		{"missing dep", []Step{{Name: "a", Depends: []string{"ghost"}, Run: noop}}, ErrKindMissingDep},
		{"self cycle", []Step{{Name: "a", Depends: []string{"a"}, Run: noop}}, ErrKindCycle},
		{"cycle", []Step{
			{Name: "a", Depends: []string{"c"}, Run: noop},
			{Name: "b", Depends: []string{"a"}, Run: noop},
			{Name: "c", Depends: []string{"b"}, Run: noop},
		}, ErrKindCycle},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.steps)
			var de *DefinitionError
			if !errors.As(err, &de) {
				t.Fatalf("expected *DefinitionError, got %v", err)
			}
			logDecision(t, tc.name, fmt.Sprintf("steps=%d", len(tc.steps)), fmt.Sprintf("kind=%s detail=%s", de.Kind, de.Detail))
			if de.Kind != tc.kind {
				t.Fatalf("kind = %s, want %s", de.Kind, tc.kind)
			}
		})
	}
}

// TestDependencyFailure 依赖失败后，下游步骤被跳过，运行终态为 failed。
func TestDependencyFailure(t *testing.T) {
	boom := errors.New("boom")
	var ranB, ranC atomic.Int32
	steps := []Step{
		{Name: "a", Run: func(ctx context.Context) error {
			logDecision(t, "a", "no deps", "returns error -> fail, no retry configured")
			return boom
		}},
		{Name: "b", Depends: []string{"a"}, Run: func(ctx context.Context) error {
			ranB.Add(1)
			return nil
		}},
		{Name: "c", Depends: []string{"b"}, Run: func(ctx context.Context) error {
			ranC.Add(1)
			return nil
		}},
	}
	eng, err := NewEngine(Config{MaxParallel: 2}, newStore(t), steps)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	rec, err := eng.Run(context.Background(), "run-dep-fail")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	logDecision(t, "run", "a failed", fmt.Sprintf("runStatus=%s b=%s c=%s", rec.Status, rec.Steps["b"].Status, rec.Steps["c"].Status))
	if rec.Status != RunFailed {
		t.Fatalf("run status = %s, want failed", rec.Status)
	}
	if rec.Steps["a"].Status != StatusFailed || rec.Steps["a"].Err != "boom" {
		t.Fatalf("step a = %+v, want failed with original cause", rec.Steps["a"])
	}
	if rec.Steps["b"].Status != StatusSkipped || rec.Steps["c"].Status != StatusSkipped {
		t.Fatalf("downstream should be skipped: b=%s c=%s", rec.Steps["b"].Status, rec.Steps["c"].Status)
	}
	if ranB.Load()+ranC.Load() != 0 {
		t.Fatalf("downstream steps must not execute, ran b=%d c=%d", ranB.Load(), ranC.Load())
	}
}

// TestRetryExhausted 重试次数耗尽后步骤失败，且不可重试错误立即终止。
func TestRetryExhausted(t *testing.T) {
	var attempts atomic.Int32
	steps := []Step{
		{Name: "flaky", Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond, Timeout: time.Second},
			Run: func(ctx context.Context) error {
				n := attempts.Add(1)
				logDecision(t, "flaky", fmt.Sprintf("attempt=%d", n), "always fails with retryable error")
				return fmt.Errorf("transient %d", n)
			}},
	}
	eng, err := NewEngine(Config{MaxParallel: 1}, newStore(t), steps)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	rec, err := eng.Run(context.Background(), "run-retry")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	logDecision(t, "run", "flaky exhausted retries", fmt.Sprintf("status=%s attempts=%d err=%q", rec.Status, rec.Steps["flaky"].Attempts, rec.Steps["flaky"].Err))
	if rec.Status != RunFailed {
		t.Fatalf("run status = %s, want failed", rec.Status)
	}
	if got := rec.Steps["flaky"].Attempts; got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if rec.Steps["flaky"].Err != "transient 3" {
		t.Fatalf("err = %q, want last original cause", rec.Steps["flaky"].Err)
	}

	// 不可重试错误：第一次失败即终止，保留原始原因。
	var nrAttempts atomic.Int32
	steps2 := []Step{
		{Name: "fatal", Retry: RetryPolicy{MaxAttempts: 5, Backoff: time.Millisecond},
			Run: func(ctx context.Context) error {
				n := nrAttempts.Add(1)
				logDecision(t, "fatal", fmt.Sprintf("attempt=%d", n), "returns NonRetryable -> stop immediately")
				return NonRetryable(errors.New("fatal: bad input"))
			}},
	}
	eng2, err := NewEngine(Config{MaxParallel: 1}, newStore(t), steps2)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	rec2, err := eng2.Run(context.Background(), "run-nonretryable")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rec2.Steps["fatal"].Attempts != 1 || nrAttempts.Load() != 1 {
		t.Fatalf("non-retryable must stop after 1 attempt, got %d", rec2.Steps["fatal"].Attempts)
	}
	if rec2.Steps["fatal"].Err != "fatal: bad input" {
		t.Fatalf("err = %q, want original cause", rec2.Steps["fatal"].Err)
	}
}

// TestRepeatAndRestart 已成功步骤在重复执行与“进程重启”（新引擎+同一状态文件）后不再次生效。
func TestRepeatAndRestart(t *testing.T) {
	store := newStore(t)
	var effects atomic.Int32
	steps := []Step{
		{Name: "pay", Run: func(ctx context.Context) error {
			n := effects.Add(1)
			logDecision(t, "pay", fmt.Sprintf("effect#%d", n), "side effect applied once")
			return nil
		}},
		{Name: "notify", Depends: []string{"pay"}, Run: func(ctx context.Context) error { return nil }},
	}
	eng, err := NewEngine(Config{MaxParallel: 2}, store, steps)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	rec, err := eng.Run(context.Background(), "run-idem")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rec.Status != RunSucceeded {
		t.Fatalf("run status = %s, want succeeded", rec.Status)
	}

	// 重复执行同一 runID
	rec2, err := eng.Run(context.Background(), "run-idem")
	if err != nil {
		t.Fatalf("re-Run: %v", err)
	}
	logDecision(t, "run", "repeat same runID", fmt.Sprintf("status=%s effects=%d", rec2.Status, effects.Load()))
	if rec2.Status != RunSucceeded || effects.Load() != 1 {
		t.Fatalf("repeat run must not re-apply effects: status=%s effects=%d", rec2.Status, effects.Load())
	}

	// 模拟进程重启：新 Engine 实例 + 同一状态文件
	eng3, err := NewEngine(Config{MaxParallel: 2}, store, steps)
	if err != nil {
		t.Fatalf("NewEngine after restart: %v", err)
	}
	rec3, err := eng3.Run(context.Background(), "run-idem")
	if err != nil {
		t.Fatalf("Run after restart: %v", err)
	}
	logDecision(t, "run", "restart with persisted state", fmt.Sprintf("status=%s effects=%d", rec3.Status, effects.Load()))
	if rec3.Status != RunSucceeded || effects.Load() != 1 {
		t.Fatalf("restart must not re-apply effects: status=%s effects=%d", rec3.Status, effects.Load())
	}
}

// TestConcurrentCancel 并发触发 + 整体取消：取消后不再调度新步骤，在跑步骤结束后终态唯一。
func TestConcurrentCancel(t *testing.T) {
	var started, finished atomic.Int32
	blocking := make(chan struct{})
	steps := []Step{
		{Name: "s1", Run: func(ctx context.Context) error {
			started.Add(1)
			<-ctx.Done() // 模拟在跑的长任务，响应取消
			finished.Add(1)
			return ctx.Err()
		}},
		{Name: "s2", Depends: []string{"s1"}, Run: func(ctx context.Context) error {
			started.Add(1)
			return nil
		}},
		{Name: "s3", Depends: []string{"s2"}, Run: func(ctx context.Context) error {
			started.Add(1)
			return nil
		}},
	}
	close(blocking)
	eng, err := NewEngine(Config{MaxParallel: 1}, newStore(t), steps)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	type result struct {
		rec *RunRecord
		err error
	}
	results := make(chan result, 2)
	// 两个并发触发同一 runID（第二个会阻塞在 runMu 上，等第一个结束后幂等返回）
	go func() {
		rec, err := eng.Run(ctx, "run-cancel")
		results <- result{rec, err}
	}()
	go func() {
		rec, err := eng.Run(ctx, "run-cancel")
		results <- result{rec, err}
	}()

	// 等 s1 开始跑之后取消
	for started.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()

	var rec *RunRecord
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("Run: %v", r.err)
		}
		if rec == nil {
			rec = r.rec
		}
	}
	logDecision(t, "run", "cancel while s1 running, MaxParallel=1",
		fmt.Sprintf("status=%s s1=%s s2=%s s3=%s started=%d finished=%d",
			rec.Status, rec.Steps["s1"].Status, rec.Steps["s2"].Status, rec.Steps["s3"].Status, started.Load(), finished.Load()))

	if rec.Status != RunCanceled {
		t.Fatalf("run status = %s, want canceled", rec.Status)
	}
	if rec.Steps["s1"].Status != StatusCanceled {
		t.Fatalf("s1 = %s, want canceled", rec.Steps["s1"].Status)
	}
	// 取消后不得再调度新步骤
	if rec.Steps["s2"].Status != StatusCanceled || rec.Steps["s3"].Status != StatusCanceled {
		t.Fatalf("pending steps must be canceled, not scheduled: s2=%s s3=%s", rec.Steps["s2"].Status, rec.Steps["s3"].Status)
	}
	if started.Load() != 1 || finished.Load() != 1 {
		t.Fatalf("only s1 may run: started=%d finished=%d", started.Load(), finished.Load())
	}
	// 两个并发触发必须收敛到同一个唯一终态
	for _, name := range []string{"s1", "s2", "s3"} {
		if !rec.Steps[name].Status.Terminal() {
			t.Fatalf("step %s not terminal: %s", name, rec.Steps[name].Status)
		}
	}
}

// TestParallelSuccess 独立步骤并发执行且全部成功。
func TestParallelSuccess(t *testing.T) {
	var inFlight, maxInFlight atomic.Int32
	mk := func(name string, deps ...string) Step {
		return Step{Name: name, Depends: deps, Run: func(ctx context.Context) error {
			n := inFlight.Add(1)
			for {
				m := maxInFlight.Load()
				if n <= m || maxInFlight.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			return nil
		}}
	}
	steps := []Step{mk("a"), mk("b"), mk("c"), mk("join", "a", "b", "c")}
	eng, err := NewEngine(Config{MaxParallel: 2}, newStore(t), steps)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	rec, err := eng.Run(context.Background(), "run-parallel")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	logDecision(t, "run", "3 independent steps, MaxParallel=2", fmt.Sprintf("status=%s maxInFlight=%d", rec.Status, maxInFlight.Load()))
	if rec.Status != RunSucceeded {
		t.Fatalf("run status = %s, want succeeded", rec.Status)
	}
	if maxInFlight.Load() > 2 {
		t.Fatalf("concurrency limit violated: maxInFlight=%d", maxInFlight.Load())
	}
}
