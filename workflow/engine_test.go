package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newEngine(t *testing.T, def *Definition, opts ...Option) *Engine {
	t.Helper()
	store, err := OpenStore(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	eng, err := NewEngine(def, store, opts...)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return eng
}

func logStep(t *testing.T, runID, step, input, basis string) {
	t.Helper()
	t.Logf("run=%s step=%s 输入=%s 判定依据=%s", runID, step, input, basis)
}

// 依赖失败：b 依赖 a，a 不可重试地失败，b 必须被跳过且运行终态为 failed。
func TestDependencyFailureSkipsDependents(t *testing.T) {
	rootErr := errors.New("disk gone")
	var bRuns atomic.Int32
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: func(ctx context.Context) error {
			logStep(t, "run-dep", "a", "无依赖", "返回 PermanentError，应立即终止且不重试")
			return PermanentError(rootErr)
		}},
		{Name: "b", DependsOn: []string{"a"}, Handler: func(ctx context.Context) error {
			bRuns.Add(1)
			return nil
		}},
		{Name: "c", DependsOn: []string{"b"}, Handler: noopHandler},
	}}
	eng := newEngine(t, def)
	state, err := eng.Run(context.Background(), "run-dep")
	logStep(t, "run-dep", "run", "a 永久失败", "终态必须为 failed，b/c 必须 skipped 且 b 从未执行")
	if !errors.Is(err, ErrStepFailed) {
		t.Fatalf("期望 ErrStepFailed，得到 %v", err)
	}
	if state.Status != RunFailed {
		t.Fatalf("终态=%s，期望 failed", state.Status)
	}
	if got := state.Steps["a"].Status; got != StepFailed {
		t.Fatalf("a 状态=%s，期望 failed", got)
	}
	if got := state.Steps["a"].Attempts; got != 1 {
		t.Fatalf("a 尝试次数=%d，不可重试错误必须只执行 1 次", got)
	}
	if !errors.Is(errors.New(state.Steps["a"].LastErr), rootErr) && state.Steps["a"].LastErr != rootErr.Error() {
		t.Fatalf("a 未保留原始原因: %q", state.Steps["a"].LastErr)
	}
	for _, s := range []string{"b", "c"} {
		if got := state.Steps[s].Status; got != StepSkipped {
			t.Fatalf("%s 状态=%s，期望 skipped", s, got)
		}
	}
	if bRuns.Load() != 0 {
		t.Fatalf("b 被执行了 %d 次，期望 0", bRuns.Load())
	}
}

// 重试耗尽：可重试错误按配置次数尝试后失败，尝试次数与退避次数可观测。
func TestRetryExhaustion(t *testing.T) {
	var attempts atomic.Int32
	def := &Definition{Steps: []StepSpec{
		{Name: "flaky", Retry: RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond, Multiplier: 2},
			Handler: func(ctx context.Context) error {
				n := attempts.Add(1)
				logStep(t, "run-retry", "flaky", fmt.Sprintf("第 %d 次尝试", n), "始终返回可重试错误，应在 3 次后耗尽")
				return fmt.Errorf("transient %d", n)
			}},
	}}
	eng := newEngine(t, def)
	state, err := eng.Run(context.Background(), "run-retry")
	if !errors.Is(err, ErrStepFailed) {
		t.Fatalf("期望 ErrStepFailed，得到 %v", err)
	}
	rec := state.Steps["flaky"]
	logStep(t, "run-retry", "flaky", "重试耗尽", fmt.Sprintf("attempts=%d lastErr=%q", rec.Attempts, rec.LastErr))
	if rec.Attempts != 3 {
		t.Fatalf("尝试次数=%d，期望 3", rec.Attempts)
	}
	if rec.Status != StepFailed || state.Status != RunFailed {
		t.Fatalf("状态=%s/%s，期望 failed/failed", rec.Status, state.Status)
	}
	if rec.LastErr != "transient 3" {
		t.Fatalf("应保留最后一次原始错误，得到 %q", rec.LastErr)
	}
}

// 重试后成功：前两次失败第三次成功。
func TestRetryThenSucceed(t *testing.T) {
	var attempts atomic.Int32
	def := &Definition{Steps: []StepSpec{
		{Name: "flaky", Retry: RetryPolicy{MaxAttempts: 5, Backoff: time.Millisecond},
			Handler: func(ctx context.Context) error {
				n := attempts.Add(1)
				if n < 3 {
					return fmt.Errorf("transient %d", n)
				}
				return nil
			}},
	}}
	eng := newEngine(t, def)
	state, err := eng.Run(context.Background(), "run-flaky-ok")
	logStep(t, "run-flaky-ok", "flaky", "前 2 次失败第 3 次成功", "终态应为 succeeded，attempts=3")
	if err != nil {
		t.Fatalf("期望成功，得到 %v", err)
	}
	if state.Status != RunSucceeded || state.Steps["flaky"].Attempts != 3 {
		t.Fatalf("状态=%s attempts=%d", state.Status, state.Steps["flaky"].Attempts)
	}
}

// 重复执行与重启恢复：已成功的步骤在再次 Run 或换引擎（模拟重启）后不得再次生效。
func TestRepeatRunAndRestartAreIdempotent(t *testing.T) {
	var aCount, bCount atomic.Int32
	path := t.TempDir() + "/state.json"
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: func(ctx context.Context) error {
			aCount.Add(1)
			return nil
		}},
		{Name: "b", DependsOn: []string{"a"}, Handler: func(ctx context.Context) error {
			bCount.Add(1)
			return nil
		}},
	}}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := NewEngine(def, store)
	if err != nil {
		t.Fatal(err)
	}
	st1, err := eng.Run(context.Background(), "run-idem")
	if err != nil || st1.Status != RunSucceeded {
		t.Fatalf("首次运行: state=%v err=%v", st1.Status, err)
	}
	logStep(t, "run-idem", "a,b", "首次运行", "各执行 1 次")

	// 同一引擎重复执行。
	st2, err := eng.Run(context.Background(), "run-idem")
	if err != nil || st2.Status != RunSucceeded {
		t.Fatalf("重复运行: state=%v err=%v", st2.Status, err)
	}

	// 模拟进程重启：新 Store + 新 Engine，加载同一状态文件。
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	eng2, err := NewEngine(def, store2)
	if err != nil {
		t.Fatal(err)
	}
	st3, err := eng2.Run(context.Background(), "run-idem")
	if err != nil || st3.Status != RunSucceeded {
		t.Fatalf("重启后运行: state=%v err=%v", st3.Status, err)
	}
	logStep(t, "run-idem", "a,b", "重复执行+重启后共 3 次 Run 调用",
		fmt.Sprintf("a 执行 %d 次, b 执行 %d 次；成功步骤不得再次生效", aCount.Load(), bCount.Load()))
	if aCount.Load() != 1 || bCount.Load() != 1 {
		t.Fatalf("幂等被破坏: a=%d b=%d，期望各 1 次", aCount.Load(), bCount.Load())
	}
}

// 崩溃恢复：状态文件中遗留 running 的步骤会被重置并重新调度，已成功有直接构造状态验证。
func TestCrashRecoveryResumesPendingWork(t *testing.T) {
	path := t.TempDir() + "/state.json"
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃现场：a 已成功，b 崩溃时处于 running，c 未开始。
	if err := store.SaveRun(&RunState{
		RunID:  "run-crash",
		Status: RunRunning,
		Steps: map[string]*StepRecord{
			"a": {Status: StepSucceeded, Attempts: 1},
			"b": {Status: StepRunning, Attempts: 1},
			"c": {Status: StepPending},
		},
	}); err != nil {
		t.Fatal(err)
	}
	var aCount, bCount, cCount atomic.Int32
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: func(ctx context.Context) error { aCount.Add(1); return nil }},
		{Name: "b", DependsOn: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 3},
			Handler: func(ctx context.Context) error { bCount.Add(1); return nil }},
		{Name: "c", DependsOn: []string{"b"}, Handler: func(ctx context.Context) error { cCount.Add(1); return nil }},
	}}
	eng, err := NewEngine(def, store)
	if err != nil {
		t.Fatal(err)
	}
	state, err := eng.Run(context.Background(), "run-crash")
	logStep(t, "run-crash", "b", "崩溃时处于 running", "应重置为 pending 并重新执行；a 已成功不得重跑")
	if err != nil || state.Status != RunSucceeded {
		t.Fatalf("恢复运行: state=%v err=%v", state.Status, err)
	}
	if aCount.Load() != 0 {
		t.Fatalf("a 已持久化成功却被重跑 %d 次", aCount.Load())
	}
	if bCount.Load() != 1 || cCount.Load() != 1 {
		t.Fatalf("b=%d c=%d，期望各执行 1 次", bCount.Load(), cCount.Load())
	}
}

// 并发取消：并发触发多个运行并整体取消，取消后不得调度新步骤，
// 在跑的步骤跑完后终态唯一为 cancelled，无半完成状态。
func TestConcurrentCancel(t *testing.T) {
	var started, finished atomic.Int32
	release := make(chan struct{})
	def := &Definition{Steps: []StepSpec{
		{Name: "s1", Handler: func(ctx context.Context) error {
			started.Add(1)
			<-release // 阻塞直到测试放行，模拟长任务
			finished.Add(1)
			return nil
		}},
		{Name: "s2", DependsOn: []string{"s1"}, Handler: func(ctx context.Context) error {
			started.Add(1)
			finished.Add(1)
			return nil
		}},
	}}
	eng := newEngine(t, def, WithMaxConcurrent(1))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	states := make([]*RunState, 4)
	errs := make([]error, 4)
	// 并发触发 4 个不同 runID 的运行。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := eng.Run(ctx, fmt.Sprintf("run-cancel-%d", i))
			states[i], errs[i] = st, err
		}(i)
	}
	// 等至少一个步骤在跑，然后整体取消。
	deadline := time.Now().Add(2 * time.Second)
	for started.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	time.Sleep(10 * time.Millisecond) // 给调度器机会错误地派发新步骤
	close(release)                    // 放行在跑的步骤
	wg.Wait()

	logStep(t, "run-cancel-*", "s1/s2", "4 个并发运行 + 整体取消",
		fmt.Sprintf("started=%d finished=%d；取消后不得调度新步骤", started.Load(), finished.Load()))
	if started.Load() != finished.Load() {
		t.Fatalf("存在半完成步骤: started=%d finished=%d", started.Load(), finished.Load())
	}
	for i, st := range states {
		if st == nil {
			t.Fatalf("运行 %d 无状态返回", i)
		}
		if st.Status != RunCancelled {
			t.Fatalf("运行 %d 终态=%s，期望唯一终态 cancelled", i, st.Status)
		}
		if !errors.Is(errs[i], ErrRunCancelled) {
			t.Fatalf("运行 %d 错误=%v，期望 ErrRunCancelled", i, errs[i])
		}
		for name, rec := range st.Steps {
			if rec.Status == StepRunning || rec.Status == StepPending {
				t.Fatalf("运行 %d 步骤 %s 残留中间态 %s", i, name, rec.Status)
			}
		}
	}
	// 取消后 s2 不应在任何运行中成功生效。
	for i, st := range states {
		if st.Steps["s2"].Status == StepSucceeded {
			t.Fatalf("运行 %d 在取消后仍调度了 s2", i)
		}
	}
}

// 并发限制：同时运行的步骤数不得超过上限。
func TestMaxConcurrentLimit(t *testing.T) {
	var cur, maxSeen atomic.Int32
	var mu sync.Mutex
	observed := 0
	mkHandler := func() Handler {
		return func(ctx context.Context) error {
			n := cur.Add(1)
			mu.Lock()
			if int(n) > observed {
				observed = int(n)
			}
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			cur.Add(-1)
			return nil
		}
	}
	def := &Definition{Steps: []StepSpec{
		{Name: "a", Handler: mkHandler()},
		{Name: "b", Handler: mkHandler()},
		{Name: "c", Handler: mkHandler()},
		{Name: "d", Handler: mkHandler()},
	}}
	eng := newEngine(t, def, WithMaxConcurrent(2))
	state, err := eng.Run(context.Background(), "run-limit")
	maxSeen.Store(int32(observed))
	logStep(t, "run-limit", "a-d", "4 个无依赖步骤，上限 2", fmt.Sprintf("观测到的最大并发=%d", maxSeen.Load()))
	if err != nil || state.Status != RunSucceeded {
		t.Fatalf("state=%v err=%v", state.Status, err)
	}
	if maxSeen.Load() > 2 {
		t.Fatalf("并发上限被突破: %d", maxSeen.Load())
	}
}

// 同一 runID 并发触发：结果一致且步骤只生效一次。
func TestConcurrentSameRunID(t *testing.T) {
	var count atomic.Int32
	def := &Definition{Steps: []StepSpec{
		{Name: "only", Handler: func(ctx context.Context) error {
			count.Add(1)
			return nil
		}},
	}}
	eng := newEngine(t, def)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := eng.Run(context.Background(), "run-same")
			if err != nil || st.Status != RunSucceeded {
				t.Errorf("state=%v err=%v", st.Status, err)
			}
		}()
	}
	wg.Wait()
	logStep(t, "run-same", "only", "8 个 goroutine 并发触发同一 runID",
		fmt.Sprintf("步骤实际执行 %d 次，期望恰好 1 次", count.Load()))
	if count.Load() != 1 {
		t.Fatalf("步骤生效 %d 次，期望 1 次", count.Load())
	}
}
