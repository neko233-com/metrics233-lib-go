package metrics233

import (
	"fmt"
	"time"
)

type metric struct {
	client *Client
	name   string
	labels Labels
	key    string
	err    error
}

func (c *Client) metric(name string) metric {
	item := metric{client: c, name: name, labels: c.labels, err: c.err}
	if item.err == nil && !metricName.MatchString(name) {
		item.err = fmt.Errorf("%w: invalid metric name", ErrInvalidSample)
	}
	item.key = seriesKey(name, item.labels)
	return item
}

func (item metric) withLabels(labels Labels) metric {
	merged, err := mergeLabels(item.labels, labels)
	item.labels, item.key = merged, seriesKey(item.name, merged)
	if item.err == nil {
		item.err = err
	}
	return item
}

// Gauge 是当前值指标句柄，支持并发 Set。
type Gauge struct{ metric metric }

// Gauge 创建句柄；首次 Set 时才占用 series 配额。
func (c *Client) Gauge(name string) *Gauge { return &Gauge{metric: c.metric(name)} }

// WithLabels 返回新的 Gauge 句柄，不修改当前句柄。
func (g *Gauge) WithLabels(labels Labels) *Gauge {
	return &Gauge{metric: g.metric.withLabels(labels)}
}

// Set 登记有限数值；下一次 Push 发送最新值。
func (g *Gauge) Set(value float64) error { return g.metric.update(kindGauge, value) }

// Counter 是进程内累计计数器；重试或成功 Push 都不会将累计值清零。
// 进程重启后的重置由服务端 PromQL rate/increase 处理。
type Counter struct{ metric metric }

// Counter 创建累计计数器句柄；首次 Add/Inc 时才占用配额。
func (c *Client) Counter(name string) *Counter { return &Counter{metric: c.metric(name)} }

// WithLabels 返回新的 Counter 句柄。
func (c *Counter) WithLabels(labels Labels) *Counter {
	return &Counter{metric: c.metric.withLabels(labels)}
}

// Inc 累加 1。
func (c *Counter) Inc() error { return c.Add(1) }

// Add 累加非负有限值。负数、NaN、Inf 或累计溢出均不修改计数器。
// Add(0) 可登记一个从零开始的 counter。
func (c *Counter) Add(delta float64) error { return c.metric.update(kindCounter, delta) }

func (item metric) update(typ kind, value float64) error {
	if item.err != nil {
		return item.err
	}
	if !finite(value) || (typ == kindCounter && value < 0) {
		return fmt.Errorf("%w: value must be finite and counter deltas non-negative", ErrInvalidSample)
	}
	s := item.client.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.closed {
		return ErrClosed
	}
	if previous, exists := s.names[item.name]; exists && previous != typ {
		return ErrTypeConflict
	}
	entry, exists := s.entries[item.key]
	if !exists {
		if len(s.entries) >= s.config.limits.MaxSeries {
			return ErrSeriesLimit
		}
		entry = series{name: item.name, labels: item.labels, kind: typ}
	}
	if typ == kindCounter {
		value += entry.value
		if !finite(value) {
			return fmt.Errorf("%w: cumulative counter overflow", ErrInvalidSample)
		}
	}
	entry.value = value
	s.entries[item.key], s.names[item.name] = entry, typ
	return nil
}

type operation struct {
	metric metric
	kind   kind
	value  float64
	sample Sample
}

// Record 将原始样本加入有界队列，保留每个时间点而非仅保存最新值。
// 本次调用原子登记；无效样本或容量不足时整组都不入队。
// 同一指标名不能同时使用 Record 和 Gauge/Counter。
func (c *Client) Record(samples ...Sample) error {
	if len(samples) > c.state.config.limits.MaxPending {
		return ErrQueueFull
	}
	ops, err := c.rawOperations(samples)
	if err != nil {
		return err
	}
	return c.state.apply(ops)
}

func (c *Client) rawOperations(samples []Sample) ([]operation, error) {
	if c.err != nil {
		return nil, c.err
	}
	now := time.Now()
	ops := make([]operation, 0, len(samples))
	for _, sample := range samples {
		item := c.metric(sample.Name).withLabels(sample.Labels)
		if item.err != nil {
			return nil, item.err
		}
		if !finite(sample.Value) || sample.Timestamp < 0 || sample.Timestamp > now.Add(5*time.Minute).UnixMilli() {
			return nil, fmt.Errorf("%w: invalid finite value or Unix millisecond timestamp", ErrInvalidSample)
		}
		if sample.Timestamp == 0 {
			sample.Timestamp = now.UnixMilli()
		}
		sample.Labels = item.labels
		ops = append(ops, operation{metric: item, kind: kindRaw, sample: sample})
	}
	return ops, nil
}

func (s *state) apply(ops []operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.closed {
		return ErrClosed
	}
	planned := make(map[string]series)
	names := make(map[string]kind)
	newSeries, rawCount := 0, 0
	for _, op := range ops {
		if previous, exists := s.names[op.metric.name]; exists && previous != op.kind {
			return ErrTypeConflict
		}
		if previous, exists := names[op.metric.name]; exists && previous != op.kind {
			return ErrTypeConflict
		}
		names[op.metric.name] = op.kind
		entry, exists := planned[op.metric.key]
		if !exists {
			entry, exists = s.entries[op.metric.key]
			if !exists {
				newSeries++
				if len(s.entries)+newSeries > s.config.limits.MaxSeries {
					return ErrSeriesLimit
				}
				entry = series{name: op.metric.name, labels: op.metric.labels, kind: op.kind}
			}
		}
		switch op.kind {
		case kindGauge:
			entry.value = op.value
		case kindCounter:
			if !finite(entry.value + op.value) {
				return fmt.Errorf("%w: cumulative counter overflow", ErrInvalidSample)
			}
			entry.value += op.value
		case kindRaw:
			rawCount++
		}
		planned[op.metric.key] = entry
	}
	pending := len(s.pending) + rawCount
	if s.delivery != nil {
		pending += s.delivery.rawRemaining
	}
	if pending > s.config.limits.MaxPending {
		return ErrQueueFull
	}
	for key, entry := range planned {
		s.entries[key] = entry
	}
	for name, typ := range names {
		s.names[name] = typ
	}
	for _, op := range ops {
		if op.kind == kindRaw {
			s.pending = append(s.pending, op.sample)
		}
	}
	return nil
}

// Batch 原子登记一组 Gauge 设置、Counter 增量和原始样本。
// Builder 本身不支持并发；Commit 后不得再次使用。
type Batch struct {
	client *Client
	ops    []operation
	err    error
	done   bool
	raw    int
}

// Batch 创建显式批量登记器；最多 10000 个操作。
func (c *Client) Batch() *Batch { return &Batch{client: c, err: c.err} }

// Gauge 在批次中设置当前值。
func (b *Batch) Gauge(name string, value float64) *Batch {
	return b.addMetric(name, kindGauge, value)
}

// Counter 在批次中累加计数器。
func (b *Batch) Counter(name string, delta float64) *Batch {
	return b.addMetric(name, kindCounter, delta)
}

func (b *Batch) addMetric(name string, typ kind, value float64) *Batch {
	if !b.available(1) {
		return b
	}
	item := b.client.metric(name)
	if item.err != nil {
		b.err = item.err
	} else if !finite(value) || (typ == kindCounter && value < 0) {
		b.err = fmt.Errorf("%w: invalid gauge value or counter delta", ErrInvalidSample)
	} else {
		b.ops = append(b.ops, operation{metric: item, kind: typ, value: value})
	}
	return b
}

// Record 在批次中添加带时间戳的原始样本。
func (b *Batch) Record(samples ...Sample) *Batch {
	if !b.available(len(samples)) {
		return b
	}
	if len(samples) > b.client.state.config.limits.MaxPending-b.raw {
		b.err = ErrQueueFull
		return b
	}
	ops, err := b.client.rawOperations(samples)
	if err != nil {
		b.err = err
	} else {
		b.ops = append(b.ops, ops...)
		b.raw += len(samples)
	}
	return b
}

func (b *Batch) available(count int) bool {
	if b.done {
		b.err = ErrBatchCommitted
	}
	if b.err == nil && (count > 10000 || len(b.ops)+count > 10000) {
		b.err = fmt.Errorf("%w: batch exceeds 10000 operations", ErrQueueFull)
	}
	return b.err == nil
}

// Commit 原子应用整组操作，不执行网络请求。
func (b *Batch) Commit() error {
	if !b.available(0) {
		return b.err
	}
	b.done = true
	return b.client.state.apply(b.ops)
}
