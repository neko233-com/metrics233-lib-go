package metrics233

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const maxRequestBytes = 4 * 1024 * 1024

// HTTPError 表示未获成功确认的 HTTP 响应；不携带服务端正文或凭据。
type HTTPError struct {
	StatusCode int
	RetryAfter time.Duration
}

// Error 返回不含服务端正文的安全错误描述。
func (e *HTTPError) Error() string {
	return fmt.Sprintf("metrics233: ingest returned HTTP %d", e.StatusCode)
}

// Push 同步发送一次指标快照和待发送原始样本。
// 失败批次保留原始时间戳和正文，下次 Push 优先重送；成功后累计 Counter 不清零。
// 并发 Push 会串行执行，等待期间也响应 ctx 取消。
func (c *Client) Push(ctx context.Context) error {
	if c.err != nil {
		return c.err
	}
	return c.push(ctx, false)
}

func (c *Client) push(ctx context.Context, closeAfter bool) (result error) {
	if ctx == nil {
		return errors.New("metrics233: context is nil")
	}
	s := c.state
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.gate:
	}
	defer func() {
		s.mu.Lock()
		s.lastPushError = result
		s.mu.Unlock()
		s.gate <- struct{}{}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if closeAfter {
			return nil
		}
		return ErrClosed
	}
	retained := s.delivery != nil
	if !retained {
		s.snapshotLocked()
	}
	s.mu.Unlock()
	if err := s.flush(ctx); err != nil {
		return err
	}
	if retained {
		// Updates accumulated while the older delivery was pending remain in
		// the registry/queue. Capture them once after the old delivery is acked.
		s.mu.Lock()
		s.snapshotLocked()
		s.mu.Unlock()
		if err := s.flush(ctx); err != nil {
			return err
		}
	}
	if closeAfter {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		if s.ownedTransport != nil {
			s.ownedTransport.CloseIdleConnections()
		}
	}
	return nil
}

func (s *state) snapshotLocked() {
	keys := make([]string, 0, len(s.entries))
	for key, entry := range s.entries {
		if entry.kind != kindRaw {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) == 0 && len(s.pending) == 0 {
		return
	}
	d := &delivery{samples: make([]queuedSample, 0, len(keys)+len(s.pending)), rawRemaining: len(s.pending)}
	now := time.Now().UnixMilli()
	for _, key := range keys {
		entry := s.entries[key]
		d.samples = append(d.samples, queuedSample{sample: Sample{Name: entry.name, Labels: entry.labels, Value: entry.value, Timestamp: now}})
	}
	for _, sample := range s.pending {
		d.samples = append(d.samples, queuedSample{sample: sample, raw: true})
	}
	s.pending = nil
	s.delivery = d
}

func (s *state) flush(ctx context.Context) error {
	for {
		s.mu.Lock()
		d := s.delivery
		s.mu.Unlock()
		if d == nil {
			return nil
		}
		if d.body == nil {
			body := []byte(`{"samples":[`)
			end := d.offset
			for end < len(d.samples) && end-d.offset < s.config.limits.BatchSize {
				encoded, err := json.Marshal(d.samples[end].sample)
				if err != nil {
					return fmt.Errorf("%w: sample encoding", ErrInvalidSample)
				}
				comma := 0
				if end > d.offset {
					comma = 1
				}
				if len(body)+comma+len(encoded)+2 > maxRequestBytes {
					break
				}
				if comma != 0 {
					body = append(body, ',')
				}
				body = append(body, encoded...)
				end++
			}
			if end == d.offset {
				return fmt.Errorf("%w: encoded sample exceeds 4 MiB", ErrInvalidSample)
			}
			d.body, d.batchEnd = append(body, ']', '}'), end
		}
		if err := s.send(ctx, d.body, d.batchEnd-d.offset); err != nil {
			return err
		}
		s.mu.Lock()
		for index := d.offset; index < d.batchEnd; index++ {
			if d.samples[index].raw {
				d.rawRemaining--
			}
			d.samples[index] = queuedSample{}
		}
		d.offset, d.body = d.batchEnd, nil
		if d.offset == len(d.samples) {
			s.delivery = nil
		}
		s.mu.Unlock()
	}
}

func (s *state) send(parent context.Context, body []byte, count int) error {
	ctx, cancel := context.WithTimeout(parent, s.config.timeout)
	defer cancel()
	var last error
	for attempt := 0; attempt <= s.config.retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		notBefore := s.retryNotBefore
		s.mu.Unlock()
		if err := waitContext(ctx, time.Until(notBefore)); err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
		if err != nil {
			return errors.New("metrics233: could not construct ingest request")
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", "metrics233-lib-go")
		if s.config.token != "" {
			request.Header.Set("Authorization", "Bearer "+s.config.token)
		}
		response, transportErr := s.http.Do(request)
		wait := s.config.backoff * time.Duration(1<<attempt)
		if wait > 5*time.Second {
			wait = 5 * time.Second
		}
		if transportErr != nil {
			// Avoid propagating arbitrary custom transport messages or URL data.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			last = errors.New("metrics233: ingest network request failed")
		} else {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, 64*1024+1))
			response.Body.Close()
			if err := ctx.Err(); err != nil {
				return err
			}
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				var result struct {
					Accepted int `json:"accepted"`
				}
				if readErr != nil || len(data) > 64*1024 || json.Unmarshal(data, &result) != nil || result.Accepted != count {
					return ErrProtocol
				}
				return nil
			}
			retryAfter := parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
			last = &HTTPError{StatusCode: response.StatusCode, RetryAfter: retryAfter}
			if response.StatusCode != http.StatusTooManyRequests && response.StatusCode < 500 || response.StatusCode >= 600 {
				return last
			}
			if retryAfter > 0 {
				s.mu.Lock()
				until := time.Now().Add(retryAfter)
				if until.After(s.retryNotBefore) {
					s.retryNotBefore = until
				}
				s.mu.Unlock()
			}
			if retryAfter > wait {
				wait = retryAfter
			}
		}
		if attempt == s.config.retries {
			return last
		}
		if err := waitContext(ctx, wait); err != nil {
			return err
		}
	}
	return last
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		const maximum = time.Duration(1<<63 - 1)
		if seconds > uint64(maximum/time.Second) {
			return maximum
		}
		return time.Duration(seconds) * time.Second
	}
	if stamp, err := http.ParseTime(value); err == nil && stamp.After(now) {
		return stamp.Sub(now)
	}
	return 0
}

// Run 在调用方当前 goroutine 中定时 Push，立即发送一次，并在 ctx 取消时退出。
// 失败保留数据，下一轮继续尝试；onError 可为 nil，此时从 Stats 查看最近错误。
// onError 在本循环内同步调用，应及时返回，不能调用会等待 Run 退出的 Close。
func (c *Client) Run(ctx context.Context, interval time.Duration, onError func(error)) error {
	if c.err != nil {
		return c.err
	}
	if ctx == nil || interval <= 0 {
		return errors.New("metrics233: Run requires a context and positive interval")
	}
	s := c.state
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.runDone != nil {
		s.mu.Unlock()
		return ErrAlreadyRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.runCancel, s.runDone = cancel, done
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		s.runCancel, s.runDone = nil, nil
		close(done)
		s.mu.Unlock()
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := c.Push(runCtx); err != nil {
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			if onError != nil {
				onError(err)
			}
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
	}
}

// Close 禁止新登记，取消并等待 Run，然后最终 Push。
// 失败时数据仍保留，可带新的 ctx 再次 Close；成功后重复 Close 返回 nil。
// 一个标签视图的 Close 会关闭整个共享客户端。
func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("metrics233: context is nil")
	}
	s := c.state
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	if s.runCancel != nil {
		s.runCancel()
	}
	done := s.runDone
	s.mu.Unlock()
	if done != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}
	return c.push(ctx, true)
}
