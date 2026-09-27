// wfengine 是一个本地演示程序：跑一个带失败重试的多步骤工作流，
// 状态持久化在 ./wfstate/run.json，重复执行可观察幂等恢复。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/highcumontoa/durable-workflow-engine-go/workflow"
)

func main() {
	statePath := flag.String("state", "wfstate/run.json", "本地状态文件路径")
	runID := flag.String("run", "demo-run", "运行 ID（重复执行同一 ID 可验证幂等）")
	flag.Parse()

	store, err := workflow.NewStore(*statePath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}

	var fetchAttempts int
	steps := []workflow.Step{
		{Name: "fetch", Retry: workflow.RetryPolicy{MaxAttempts: 3, Backoff: 200 * time.Millisecond, Timeout: 2 * time.Second},
			Run: func(ctx context.Context) error {
				fetchAttempts++
				fmt.Printf("[fetch] attempt %d\n", fetchAttempts)
				if fetchAttempts < 2 {
					return errors.New("transient network error") // 可重试
				}
				return nil
			}},
		{Name: "transform", Depends: []string{"fetch"}, Run: func(ctx context.Context) error {
			fmt.Println("[transform] done")
			return nil
		}},
		{Name: "load", Depends: []string{"transform"}, Run: func(ctx context.Context) error {
			fmt.Println("[load] done")
			return nil
		}},
	}

	eng, err := workflow.NewEngine(workflow.Config{MaxParallel: 2}, store, steps)
	if err != nil {
		log.Fatalf("invalid workflow: %v", err)
	}
	rec, err := eng.Run(context.Background(), *runID)
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	fmt.Printf("run %s finished: status=%s\n", rec.RunID, rec.Status)
	for name, st := range rec.Steps {
		fmt.Printf("  %-10s %-10s attempts=%d err=%q\n", name, st.Status, st.Attempts, st.Err)
	}
	if rec.Status != workflow.RunSucceeded {
		os.Exit(1)
	}
}
