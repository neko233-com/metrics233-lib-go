// Package metrics233 提供仅依赖标准库的 Metrics233 原生 JSON 指标客户端。
//
// Gauge 保存当前值，Counter 保存进程内累计值；两者在 Push 时生成快照。
// Record 保存带时间戳的原始样本。所有缓存都有上限，超限明确返回错误。
// New 不发请求、不启动 goroutine；调用方通过 Push、Run 和 Close 控制生命周期。
package metrics233
