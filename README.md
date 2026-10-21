# metrics233-lib-go

A small, standard-library-only Go SDK for [metrics233-server](https://github.com/neko233-com/metrics233-server).
Go 1.23+. It sends native JSON to `POST /api/v1/ingest`; it has no package initialization side effects,
automatic goroutines, exporter server, database driver or third-party dependencies.

## Quick start

```sh
go get github.com/neko233-com/metrics233-lib-go
```

```go
import metrics "github.com/neko233-com/metrics233-lib-go"

client, err := metrics.New("http://127.0.0.1:7333",
    metrics.WithToken(os.Getenv("METRICS233_TOKEN")))
if err != nil { return err }

shop := client.WithLabels(metrics.Labels{"service": "shop", "environment": "production"})
orders := shop.Counter("business_orders_total") // Reuse handles on hot paths.
active := shop.Gauge("business_active_users")

if err := orders.Inc(); err != nil { return err } // Cumulative total: 1, 2, 3, ...
if err := active.Set(42); err != nil { return err }
if err := client.Push(ctx); err != nil { return err }
```

Use the dedicated ingest-only token supplied by your application for a token-mode server. The SDK never logs tokens or server response
bodies and rejects credentials embedded in URLs. Redirects are not followed. The caller owns TLS/proxy settings;
`WithHTTPClient` accepts an existing `http.Client` without modifying its transport.

For fixed reporting credentials, configure the server with `METRICSPANEL_AUTH=token`, a private management
`METRICSPANEL_TOKEN` (16+ characters), and a **different** `METRICSPANEL_INGEST_TOKEN` (32–8192 printable ASCII
characters without whitespace; server v0.2.2+).
Pass the ingest-only value to `WithToken`; `METRICS233_TOKEN` in these examples is your application's environment
variable, not an automatic server setting. Keep the management token private.
All label views and chained Counter/Gauge/Batch operations retain this client configuration, and every push
sends `Authorization: Bearer …`. Missing or wrong tokens are rejected with HTTP 401 and cannot write samples.
The ingest-only token cannot query data, manage alerts or change source access. Source-IP block/allowlist policy
still applies. Legacy servers also accept the workspace token, which has management access; do not distribute it
as a reporting credential. Local authentication uses its existing service-account roles rather than this separate
token-mode setting. Never expose an unconfigured loopback development server through a public proxy.

For periodic reporting, explicitly run the loop in an application-owned goroutine:

```go
go func() {
    err := client.Run(ctx, 15*time.Second, func(err error) { log.Printf("metrics push: %v", err) })
    if err != nil && !errors.Is(err, context.Canceled) { log.Printf("metrics loop: %v", err) }
}()

// During application shutdown, use a fresh bounded context after canceling ctx.
shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := client.Close(shutdown); err != nil { log.Printf("final metrics push: %v", err) }
```

`Run` pushes immediately, then at the interval. A failed push is reported and retained for the next interval.
Its error callback runs synchronously: keep it short and do not call `Close` from that callback. Passing `nil`
is supported; `client.Stats().LastPushError` exposes the most recent outcome. Canceling `Run` stops the loop but
leaves the client usable. `Close` stops the loop, rejects new recordings and flushes; a failed `Close` can be
retried with a new context. Label views share the same registry and lifecycle.

See [the complete runnable example](examples/basic/main.go) for signal handling and shutdown.

## Batches and existing timestamped observations

```go
err := shop.Batch().
    Counter("business_orders_total", 3). // Add three; do not supply a delta to Gauge.
    Gauge("business_active_users", 45).
    Commit()

err = shop.Record(metrics.Sample{
    Name: "business_queue_depth", Value: 7,
    Labels: metrics.Labels{"queue": "payments"},
    Timestamp: time.Now().UnixMilli(),
})
```

`Batch.Commit` atomically registers up to 10,000 operations; validation, overflow or capacity failure changes
nothing. It performs no network I/O. A builder is single-use and is not concurrent; clients and metric handles
are concurrent. `Record` preserves each queued timestamp, while Gauge/Counter retain only the current value
between pushes. A zero raw timestamp is fixed when registered, so every retry uses the same timestamp.
Use one recording type per metric name; mixing `Record`, Gauge and Counter returns `ErrTypeConflict`.

## Limits and delivery

| Setting | Default | Contract |
| --- | ---: | --- |
| Registered series | 1,000 | Lifetime cap, including raw series; `ErrSeriesLimit` rejects new series |
| Pending raw samples | 1,000 | Includes unacknowledged in-flight samples; `ErrQueueFull`, never silent eviction |
| Samples per HTTP request | 500 | Configurable up to 10,000; additionally split at the server's 4 MiB JSON limit |
| HTTP batch deadline | 5 seconds | Includes network requests, backoff and `Retry-After` waits |
| Additional retries | 2 | Network failures, 429 and 5xx only; configurable 0–5 |
| Labels per series / value | 32 / 512 bytes | Shared server constraints; metric names 200 bytes, label names 100 bytes |

```go
client, err := metrics.New("https://metrics.example.com",
    metrics.WithToken(os.Getenv("METRICS233_TOKEN")),
    metrics.WithTimeout(5*time.Second),
    metrics.WithRetry(2, 200*time.Millisecond),
    metrics.WithLimits(metrics.Limits{MaxSeries: 1000, MaxPending: 1000, BatchSize: 500}),
)
```

Failed deliveries retain their exact body and timestamps and are sent before a fresh snapshot. A complete
`accepted` acknowledgement is required before releasing a batch. Retrying the same series/timestamp replaces
the same observation on SQLite and follows the server's replacement contract on other backends; this is not
a distributed exactly-once guarantee. HTTP `Retry-After` seconds and dates are respected across retries and
separate `Push` calls; a shorter deadline cancels waiting. Concurrent `Push` calls are serialized with cancelable
waiting. One snapshot can span multiple requests, so a whole push is not atomic across those requests.

Counters keep their process-local cumulative value after failures **and successes**. A process restart resets
them, which PromQL `rate`/`increase` handles; two observations are required for a useful rate. There is no durable
client spool or crash recovery. Values follow the server's float64 protocol; integers above 2^53 lose precision.
Use `Record` for historical samples. For Counter/Gauge, intermediate changes
between snapshots are intentionally coalesced. Keep label values bounded: service, region, environment and
small outcome sets work; user/order/request IDs create excessive cardinality. No metric values or labels should
contain credentials or personal identifiers.

## 中文快速接入

1. 从应用自己的环境变量/私有配置读取 URL 和 workspace token，调用 `metrics.New`；构造不联网、不启动任务。
2. `WithLabels` 设置服务、环境等有限维度；复用 `Gauge(name)` / `Counter(name)` 句柄。
3. Gauge 用 `Set` 记录当前值，Counter 用 `Inc` / `Add` 记录增量；SDK 发送的是累计总量。
4. 手动 `Push(ctx)`，或在应用显式创建的 goroutine 中 `Run(ctx, 15*time.Second, onError)`；退出时 `Close`。
5. 在服务端 **Business monitoring** 选择已收到的指标，确认 Gauge/Counter 语义并创建业务看板。

所有容量错误都明确返回，失败不会清空累计计数器。`Batch.Commit` 原子登记；实际网络按样本数和 4 MiB
分批，跨 HTTP 批次不承诺事务。缓存仅在内存中，进程崩溃不提供补发日志；私有部署配置属于部署仓库。

## Verification

```sh
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go test -run '^$' -bench BenchmarkCounterAdd -benchmem
# Optional: independently built server, isolated temporary DB and random token.
METRICS233_TEST_BINARY=/absolute/path/to/metricspanel go test -run TestServerBinaryIntegration -count=1
```

Tests use local `httptest` fixtures and random synthetic tokens. The binary integration test is skipped unless
explicitly configured; it does not import the server's internals. Production exporter uptime, backend capacity
and deployment health require separate acceptance. Licensed under [Apache-2.0](LICENSE).
