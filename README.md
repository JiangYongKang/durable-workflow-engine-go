# durable-workflow-engine-go

本地可运行的持久化工作流编排引擎。用本地 JSON 文件模拟 durable storage，
让多步骤任务在失败、重启和并发触发下都能得到一致、可解释的终态。

## 结构

- `workflow/` — 引擎核心包
  - `types.go` — 步骤定义、重试策略、运行记录等类型
  - `errors.go` — 可区分的加载期错误（`DefinitionError.Kind`）与 `NonRetryable` 标记
  - `validate.go` — 加载期校验与拓扑排序
  - `store.go` — 本地 JSON 状态文件（临时文件 + rename 原子写入）
  - `engine.go` — 调度器：并发限制、重试/退避/超时、取消、幂等恢复
- `cmd/wfengine/` — 本地演示程序

## 执行模型

1. **加载期校验**：`NewEngine` 在加载阶段拒绝非法定义，三类原因可区分：
   - `ErrKindDuplicateStep`：步骤重复定义
   - `ErrKindMissingDep`：依赖了不存在的步骤
   - `ErrKindCycle`：依赖成环（错误信息里带环路径）
2. **调度**：按拓扑序推进状态机。依赖全部 `succeeded` 的步骤才可被调度；
   依赖失败/跳过/取消的步骤标记为 `skipped`（取消发生后统一标记为 `canceled`）。
   无依赖的步骤并发执行，`Config.MaxParallel` 限制同时在跑的步骤数（信号量实现）。
3. **持久化**：每次状态变更都原子写入本地 JSON 文件（先写 `.tmp` 再 `rename`，
   崩溃不会留下半写文件）。`RunRecord` 记录每个步骤的状态、尝试次数与失败原因。
4. **取消与超时**：`Run` 的 ctx 取消后不再调度任何新步骤；在跑步骤的
   attempt ctx 随之取消，步骤结束后所有剩余 pending 步骤标记为 `canceled`，
   运行终态唯一收敛为 `canceled`，不会出现半完成或重复生效。

## 重试与幂等规则

- `RetryPolicy{MaxAttempts, Backoff, Timeout}` 按步骤配置：
  - `MaxAttempts` 含首次尝试；退避为 `Backoff * attempt` 线性增长；
  - `Timeout` 是单次尝试的超时（`context.WithTimeout`）。
- 普通错误按策略重试，耗尽后步骤 `failed` 并保留**最后一次的原始错误**。
- `workflow.NonRetryable(err)` 包裹的错误**立即终止**，不重试，保留原始原因。
- **幂等**：已成功（`succeeded`）的步骤是终态，重复调用 `Run`（同一 runID）
  或进程重启后（新 Engine + 同一状态文件）都不会再次执行，副作用只生效一次。
- **崩溃恢复**：进程崩溃时处于 `running` 的步骤效果未知，重启后重置为
  `pending` 重跑（at-least-once）；已 `succeeded` 的步骤绝不重跑。
- 同一引擎实例上并发调用 `Run` 会串行化（`runMu`），并发触发收敛到同一终态。

## 终态

- 步骤：`succeeded` / `failed` / `skipped` / `canceled`
- 运行：`succeeded`（全部成功）/ `failed`（任一步骤失败或被跳过）/ `canceled`（ctx 取消）

## 本地验证

```bash
# 运行全部测试（含竞态检测）；-v 日志中可看到每个步骤的输入与判定依据
go test ./... -v -race -count=1

# 运行 demo：第一次执行带一次可重试失败；第二次执行验证幂等（不会重复生效）
go run ./cmd/wfengine -state /tmp/wfdemo/run.json
go run ./cmd/wfengine -state /tmp/wfdemo/run.json
```

测试覆盖：依赖失败下游跳过、重试耗尽与不可重试错误、重复执行与重启幂等、
并发触发下的整体取消、并发上限。
