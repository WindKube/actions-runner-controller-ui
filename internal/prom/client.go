// Package prom is a small read-only client for the Prometheus HTTP query API,
// which Prometheus, Mimir, Thanos and VictoriaMetrics all serve.
package prom

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	URL string

	BearerToken     string
	BearerTokenFile string
	Username        string
	Password        string
	Headers         map[string]string

	CAFile             string
	InsecureSkipVerify bool

	Timeout time.Duration
	// CacheTTL is how long an answer is shared between callers; about one UI
	// refresh interval. Zero disables the cache.
	CacheTTL time.Duration
}

type Client struct {
	base    *url.URL
	http    *http.Client
	cfg     Config
	timeout time.Duration
	cache   *cache

	// observe, when set, receives every request's duration and outcome.
	observe func(endpoint string, d time.Duration, err error)
}

func New(cfg Config) (*Client, error) {
	base, err := url.Parse(strings.TrimSuffix(cfg.URL, "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("prometheus url %q must be absolute, e.g. http://prometheus:9090", cfg.URL)
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.InsecureSkipVerify} //nolint:gosec // opt-in
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read prometheus ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("prometheus ca file %s holds no PEM certificates", cfg.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return &Client{
		base:    base,
		http:    &http.Client{Transport: transport},
		cfg:     cfg,
		timeout: timeout,
		cache:   newCache(cfg.CacheTTL),
	}, nil
}

// Observe installs a hook that sees every request to Prometheus.
func (c *Client) Observe(fn func(endpoint string, d time.Duration, err error)) { c.observe = fn }

// Sample is one series of an instant vector.
type Sample struct {
	Labels map[string]string
	Value  float64
}

type Point struct {
	At    time.Time
	Value float64
}

// Series is one series of a range matrix.
type Series struct {
	Labels map[string]string
	Points []Point
}

// Range is a query_range window. Align it before querying so callers asking
// for "the last hour" a second apart share one cache entry.
type Range struct {
	Start time.Time
	End   time.Time
	Step  time.Duration
}

// Aligned snaps start and end down to multiples of step.
func (r Range) Aligned() Range {
	if r.Step <= 0 {
		return r
	}
	return Range{Start: r.Start.Truncate(r.Step), End: r.End.Truncate(r.Step), Step: r.Step}
}

// Query runs an instant query at the current time. The request outlives a
// cancelled caller because other callers may be waiting on the same answer;
// the client timeout still bounds it.
func (c *Client) Query(ctx context.Context, query string) ([]Sample, error) {
	v, err := c.cache.do("q|"+query, func() (any, error) {
		form := url.Values{"query": {query}}
		var out []Sample
		err := c.post(context.WithoutCancel(ctx), "/api/v1/query", form, "vector", func(raw json.RawMessage) error {
			var rows []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			}
			if err := json.Unmarshal(raw, &rows); err != nil {
				return err
			}
			out = make([]Sample, 0, len(rows))
			for _, row := range rows {
				_, v, err := parsePair(row.Value)
				if err != nil {
					return err
				}
				out = append(out, Sample{Labels: row.Metric, Value: v})
			}
			return nil
		})
		return out, err
	})
	if err != nil {
		return nil, err
	}
	return v.([]Sample), nil
}

// QueryRange runs a range query over r, aligned to its step.
func (c *Client) QueryRange(ctx context.Context, query string, r Range) ([]Series, error) {
	r = r.Aligned()
	if r.Step <= 0 || !r.End.After(r.Start) {
		return nil, fmt.Errorf("invalid range %s..%s step %s", r.Start, r.End, r.Step)
	}
	key := fmt.Sprintf("r|%s|%d|%d|%d", query, r.Start.Unix(), r.End.Unix(), int64(r.Step.Seconds()))
	v, err := c.cache.do(key, func() (any, error) {
		form := url.Values{
			"query": {query},
			"start": {strconv.FormatInt(r.Start.Unix(), 10)},
			"end":   {strconv.FormatInt(r.End.Unix(), 10)},
			"step":  {strconv.FormatFloat(r.Step.Seconds(), 'f', -1, 64)},
		}
		var out []Series
		err := c.post(context.WithoutCancel(ctx), "/api/v1/query_range", form, "matrix", func(raw json.RawMessage) error {
			var rows []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			}
			if err := json.Unmarshal(raw, &rows); err != nil {
				return err
			}
			out = make([]Series, 0, len(rows))
			for _, row := range rows {
				s := Series{Labels: row.Metric, Points: make([]Point, 0, len(row.Values))}
				for _, pair := range row.Values {
					at, v, err := parsePair(pair)
					if err != nil {
						return err
					}
					s.Points = append(s.Points, Point{At: at, Value: v})
				}
				out = append(out, s)
			}
			return nil
		})
		return out, err
	})
	if err != nil {
		return nil, err
	}
	return v.([]Series), nil
}

// APIError is an error the query API answered with, as opposed to a transport
// failure.
type APIError struct {
	Status int
	Type   string
	Msg    string
}

func (e *APIError) Error() string {
	if e.Type != "" {
		return fmt.Sprintf("prometheus %d %s: %s", e.Status, e.Type, e.Msg)
	}
	return fmt.Sprintf("prometheus %d: %s", e.Status, e.Msg)
}

func (c *Client) post(ctx context.Context, path string, form url.Values, wantType string, decode func(json.RawMessage) error) (err error) {
	started := time.Now()
	if c.observe != nil {
		defer func() { c.observe(path, time.Since(started), err) }()
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base.String()+path, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if err := c.authorize(req); err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("prometheus %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("prometheus %s: read body: %w", path, err)
	}

	var envelope struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
		Data      struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return &APIError{Status: resp.StatusCode, Msg: snippet(body)}
	}
	if envelope.Status != "success" {
		msg := envelope.Error
		if msg == "" {
			msg = snippet(body)
		}
		return &APIError{Status: resp.StatusCode, Type: envelope.ErrorType, Msg: msg}
	}
	if envelope.Data.ResultType != wantType {
		return fmt.Errorf("prometheus %s: result type %q, want %q", path, envelope.Data.ResultType, wantType)
	}
	return decode(envelope.Data.Result)
}

func (c *Client) authorize(req *http.Request) error {
	for k, v := range c.cfg.Headers {
		req.Header.Set(k, v)
	}
	switch {
	case c.cfg.BearerTokenFile != "":
		// Re-read per request so a rotated projected token is picked up.
		tok, err := os.ReadFile(c.cfg.BearerTokenFile)
		if err != nil {
			return fmt.Errorf("read prometheus bearer token file: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	case c.cfg.BearerToken != "":
		req.Header.Set("Authorization", "Bearer "+c.cfg.BearerToken)
	case c.cfg.Username != "":
		req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	}
	return nil
}

func parsePair(pair [2]any) (time.Time, float64, error) {
	ts, ok := pair[0].(float64)
	if !ok {
		return time.Time{}, 0, errors.New("prometheus: malformed sample timestamp")
	}
	raw, ok := pair[1].(string)
	if !ok {
		return time.Time{}, 0, errors.New("prometheus: malformed sample value")
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("prometheus: sample value %q: %w", raw, err)
	}
	sec := int64(ts)
	return time.Unix(sec, int64((ts-float64(sec))*1e9)).UTC(), v, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	if s == "" {
		return "empty response"
	}
	return s
}
