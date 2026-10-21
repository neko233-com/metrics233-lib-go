package metrics233

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrClosed         = errors.New("metrics233: client is closing or closed")
	ErrSeriesLimit    = errors.New("metrics233: series limit reached")
	ErrQueueFull      = errors.New("metrics233: pending sample limit reached")
	ErrTypeConflict   = errors.New("metrics233: metric name already uses another recording type")
	ErrInvalidSample  = errors.New("metrics233: invalid sample")
	ErrAlreadyRunning = errors.New("metrics233: periodic push is already running")
	ErrBatchCommitted = errors.New("metrics233: batch was already committed")
	ErrProtocol       = errors.New("metrics233: invalid ingest acknowledgement")
)

var metricName = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]{0,199}$`)
var labelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,99}$`)

// Labels 是指标维度。每个 series 最多 32 项；值最多 512 字节。
// 使用 service、region、environment 等有限集合，避免用户 ID 和请求 ID。
type Labels map[string]string

// Sample 与 POST /api/v1/ingest 的原生样本格式一致。
// Timestamp 单位为 Unix 毫秒；Record 会将零值固定为登记时刻。
type Sample struct {
	Name      string  `json:"name"`
	Labels    Labels  `json:"labels,omitempty"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

// Limits 控制常驻 series、待发送原始样本和单个 HTTP 批次。
// 零字段使用默认值 1000、1000、500；BatchSize 最高 10000。
type Limits struct {
	MaxSeries  int
	MaxPending int
	BatchSize  int
}

type config struct {
	token      string
	timeout    time.Duration
	retries    int
	backoff    time.Duration
	limits     Limits
	httpClient *http.Client
}

// Option 配置客户端；所有选项在 New 中验证。
type Option func(*config) error

// WithToken 设置 Bearer token。请从环境变量或调用方私有配置读取。
func WithToken(token string) Option {
	return func(c *config) error {
		if len(token) > 8192 || strings.IndexFunc(token, func(value rune) bool { return value < 0x20 || value == 0x7f }) >= 0 {
			return errors.New("metrics233: invalid token configuration")
		}
		c.token = token
		return nil
	}
}

// WithTimeout 设置每个 HTTP 批次的总时限，包含重试和 Retry-After 等待。
func WithTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return errors.New("metrics233: timeout must be positive")
		}
		c.timeout = timeout
		return nil
	}
}

// WithRetry 设置额外重试次数（0–5）和指数退避初始间隔。
// 仅重试网络错误、429 和 5xx；其他 HTTP 错误保留批次并立即返回。
func WithRetry(retries int, backoff time.Duration) Option {
	return func(c *config) error {
		if retries < 0 || retries > 5 || backoff < 0 || backoff > time.Minute {
			return errors.New("metrics233: invalid retry configuration")
		}
		c.retries, c.backoff = retries, backoff
		return nil
	}
}

// WithLimits 设置内存和请求容量。MaxSeries/MaxPending 最高 100000。
func WithLimits(limits Limits) Option {
	return func(c *config) error {
		if limits.MaxSeries != 0 {
			c.limits.MaxSeries = limits.MaxSeries
		}
		if limits.MaxPending != 0 {
			c.limits.MaxPending = limits.MaxPending
		}
		if limits.BatchSize != 0 {
			c.limits.BatchSize = limits.BatchSize
		}
		if c.limits.MaxSeries < 1 || c.limits.MaxSeries > 100000 || c.limits.MaxPending < 1 || c.limits.MaxPending > 100000 || c.limits.BatchSize < 1 || c.limits.BatchSize > 10000 {
			return errors.New("metrics233: invalid capacity configuration")
		}
		return nil
	}
}

// WithHTTPClient 使用调用方的 Transport/TLS 配置。SDK 复制 Client，拒绝自动重定向，
// 不修改或关闭调用方共享的 Transport。
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) error {
		if client == nil {
			return errors.New("metrics233: HTTP client is nil")
		}
		c.httpClient = client
		return nil
	}
}

type kind uint8

const (
	kindGauge kind = iota
	kindCounter
	kindRaw
)

type series struct {
	name   string
	labels Labels
	kind   kind
	value  float64
}

type queuedSample struct {
	sample Sample
	raw    bool
}

type delivery struct {
	samples      []queuedSample
	offset       int
	rawRemaining int
	body         []byte
	batchEnd     int
}

type state struct {
	mu              sync.Mutex
	gate            chan struct{}
	entries         map[string]series
	names           map[string]kind
	pending         []Sample
	delivery        *delivery
	closing, closed bool
	runCancel       context.CancelFunc
	runDone         chan struct{}
	config          config
	endpoint        string
	http            *http.Client
	ownedTransport  *http.Transport
	lastPushError   error
	retryNotBefore  time.Time
}

// Client 是并发安全的客户端或标签视图；视图共享同一个缓存和生命周期。
type Client struct {
	state  *state
	labels Labels
	err    error
}

// New 创建客户端，不执行 I/O，也不启动定时任务。
// baseURL 可包含代理路径；不得包含凭据、query 或 fragment。
func New(baseURL string, options ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.ForceQuery || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("metrics233: base URL must be an HTTP(S) origin/path without credentials, query or fragment")
	}
	u.Path, u.RawPath = strings.TrimRight(u.Path, "/")+"/api/v1/ingest", ""
	cfg := config{timeout: 5 * time.Second, retries: 2, backoff: 200 * time.Millisecond, limits: Limits{MaxSeries: 1000, MaxPending: 1000, BatchSize: 500}}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("metrics233: nil option")
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	s := &state{gate: make(chan struct{}, 1), entries: make(map[string]series), names: make(map[string]kind), config: cfg, endpoint: u.String()}
	if cfg.httpClient == nil {
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			transport := base.Clone()
			transport.MaxIdleConnsPerHost = 2
			transport.IdleConnTimeout = 30 * time.Second
			s.http, s.ownedTransport = &http.Client{Transport: transport}, transport
		} else {
			s.http = &http.Client{Transport: http.DefaultTransport}
		}
	} else {
		copy := *cfg.httpClient
		s.http = &copy
	}
	s.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	s.gate <- struct{}{}
	return &Client{state: s}, nil
}

// WithLabels 创建不可变标签视图，后设置的同名标签覆盖原值。
// 无效标签由后续 Set/Add/Record/Commit 返回；不会修改原视图。
func (c *Client) WithLabels(labels Labels) *Client {
	merged, err := mergeLabels(c.labels, labels)
	if c.err != nil {
		err = c.err
	}
	return &Client{state: c.state, labels: merged, err: err}
}

func mergeLabels(base, extra Labels) (Labels, error) {
	if len(base) > 32 || len(extra) > 32 {
		return nil, fmt.Errorf("%w: at most 32 labels per series", ErrInvalidSample)
	}
	merged := make(Labels, len(base)+len(extra))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range extra {
		merged[key] = value
	}
	if len(merged) > 32 {
		return nil, fmt.Errorf("%w: at most 32 labels per series", ErrInvalidSample)
	}
	for key, value := range merged {
		if !labelName.MatchString(key) || key == "__name__" || len(value) > 512 || !utf8.ValidString(value) {
			return nil, fmt.Errorf("%w: invalid label name or value length", ErrInvalidSample)
		}
	}
	return merged, nil
}

func seriesKey(name string, labels Labels) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	out.WriteString(name)
	out.WriteByte(0)
	for _, key := range keys {
		out.WriteString(strconv.Itoa(len(key)))
		out.WriteByte(':')
		out.WriteString(key)
		out.WriteString(strconv.Itoa(len(labels[key])))
		out.WriteByte(':')
		out.WriteString(labels[key])
	}
	return out.String()
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

// Stats 是缓存容量和生命周期快照，不包含凭据或标签值。
type Stats struct {
	Series          int
	PendingSamples  int
	InFlightSamples int
	Running         bool
	Closing         bool
	Closed          bool
	LastPushError   error
}

// Stats 返回并发安全的当前状态。
func (c *Client) Stats() Stats {
	s := c.state
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := Stats{Series: len(s.entries), PendingSamples: len(s.pending), Running: s.runDone != nil, Closing: s.closing, Closed: s.closed, LastPushError: s.lastPushError}
	if s.delivery != nil {
		stats.PendingSamples += s.delivery.rawRemaining
		stats.InFlightSamples = len(s.delivery.samples) - s.delivery.offset
	}
	return stats
}
