# durable-workflow-engine-go

本地可运行的持久化工作流编排引擎。用本地 JSON 文件模拟运行状态的持久化，
保证多步骤任务在失败、重启和并发触发下都能得到一致、可解释的终态。

## 执行模型

- **定义（Definition）**：一组 `StepSpec`，每个步骤有名称、依赖列表（`DependsOn`）、
  业务逻辑（`Handler`）和重试策略（`Retry`）。
- **加载期校验**：`NewEngine` 在加载阶段拒绝非法定义，三类原因可区分（`errors.Is`）：
  - `ErrDuplicateStep`：重复定义的步骤；
  - `ErrUnknownDependency`：依赖引用了不存在的步骤；
  - `ErrCyclicDependency`：依赖成环（含自依赖）。
- **调度**：引擎按拓扑序扫描，依赖全部成功的步骤即为就绪，可立即派发；
  无依赖关系的步骤并发执行，`WithMaxConcurrent(n)` 限制同时在跑的步骤数。
- **状态机**：步骤 `pending → running → succeeded / failed`，依赖失败时标记
  `skipped`；运行终态唯一，为 `succeeded / failed / cancelled` 之一，
  不会残留 `pending/running` 的中间态。
- **持久化**：每次状态变更都写入本地 JSON 文件（先写临时文件再原子 rename），
  崩溃不会留下半写的状态文件。

## 重试与幂等规则

- `RetryPolicy` 可按步骤配置：`MaxAttempts`（最大尝试次数，含首次）、
  `Backoff`（首次退避）、`Multiplier`（退避倍增）、`Timeout`（单次尝试超时）。
- 返回 `PermanentError(err)` 包装的错误表示**不可重试**：立即终止该步骤，
  原始原因保留在步骤记录的 `LastErr` 中。
- 可重试错误在次数耗尽后步骤失败，保留最后一次的原始错误；重试预算
  （`Attempts`）会持久化，重启后继续累计，不会被重置。
- **幂等**：已 `succeeded` 的步骤在重复 `Run`、进程重启（换 `Store`/`Engine`
  实例）后都不会再次生效——终态由持久化记录决定，而非内存。
- **崩溃恢复**：崩溃时处于 `running` 的步骤无法确定是否已生效，恢复时重置为
  `pending` 重新调度。因此引擎提供 **at-least-once** 语义，`Handler` 自身需幂等。
- **取消/超时**：`ctx` 取消或超时后不再调度任何新步骤；已在运行的步骤会跑完
  并把结果落盘，未调度的步骤标记 `skipped`，运行终态为 `cancelled`。
- **并发触发**：同一 `runID` 的并发 `Run` 调用被串行化，步骤只生效一次；
  不同 `runID` 互不影响。

## 本地验证

```bash
# 全部测试（含竞态检测）
go test ./... -race -v

# 只跑加载期校验
go test ./workflow/ -run TestValidate -v
```

测试覆盖：依赖失败级联跳过、重试耗尽、重试后成功、重复执行与重启幂等、
崩溃恢复、并发取消、并发上限、同一 runID 并发触发。每个用例的日志都会
打印步骤的输入和判定依据（`go test -v` 可见）。

## 最小示例

```go
store, _ := workflow.OpenStore("state.json")
def := &workflow.Definition{Steps: []workflow.StepSpec{
    {Name: "fetch", Handler: fetch},
    {Name: "transform", DependsOn: []string{"fetch"}, Handler: transform,
        Retry: workflow.RetryPolicy{MaxAttempts: 3, Backoff: time.Second, Multiplier: 2, Timeout: 10 * time.Second}},
    {Name: "load", DependsOn: []string{"transform"}, Handler: load},
}}
eng, err := workflow.NewEngine(def, store, workflow.WithMaxConcurrent(4))
if err != nil { // 加载期校验失败：重复步骤 / 未知依赖 / 环依赖
    log.Fatal(err)
}
state, err := eng.Run(context.Background(), "run-001")
// err 为 nil / ErrStepFailed / ErrRunCancelled；state 为持久化的唯一终态
```
