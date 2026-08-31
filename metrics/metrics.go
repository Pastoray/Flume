// Package metrics is a minimal, dependency-free metrics library that
// renders in Prometheus text exposition format. It exists so the engine
// can be instrumented (request counts, latencies, table/level gauges)
// without pulling in the official Prometheus client library - everything
// here is built on sync/atomic and the standard library.
//
// It intentionally supports only what this project needs: Counter,
// Gauge, a single-label GaugeVec (used for per-level SSTable counts),
// and a fixed-bucket latency Histogram. A Registry collects metrics and
// writes them all out in the standard exposition format that Prometheus
// (or anything else that speaks it) can scrape directly.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// metric is the interface every exported metric type satisfies so a
// Registry can render them uniformly.
type metric interface {
	render(w io.Writer)
}

// Registry collects metrics and renders them in Prometheus text
// exposition format via Render. A zero-value Registry is not usable;
// construct with NewRegistry.
type Registry struct {
	mu      sync.Mutex
	metrics []metric
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) register(m metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, m)
}

// Render renders every registered metric to w in Prometheus text
// exposition format, in registration order.
func (r *Registry) Render(w io.Writer) error {
	r.mu.Lock()
	snapshot := make([]metric, len(r.metrics))
	copy(snapshot, r.metrics)
	r.mu.Unlock()

	for _, m := range snapshot {
		m.render(w)
	}
	return nil
}

// Counter is a monotonically increasing value (e.g. total requests
// served). Safe for concurrent use.
type Counter struct {
	name, help string
	value      int64
}

// NewCounter creates and registers a new Counter.
func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	r.register(c)
	return c
}

// Inc increments the counter by 1.
func (c *Counter) Inc() { atomic.AddInt64(&c.value, 1) }

// Add increments the counter by delta (which should be non-negative;
// counters are not meant to decrease).
func (c *Counter) Add(delta int64) { atomic.AddInt64(&c.value, delta) }

// Value returns the counter's current value.
func (c *Counter) Value() int64 { return atomic.LoadInt64(&c.value) }

func (c *Counter) render(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", c.name, c.help, c.name, c.name, c.Value())
}

// Gauge is a value that can go up or down (e.g. current MemTable entry
// count). Safe for concurrent use.
type Gauge struct {
	name, help string
	value      int64
}

// NewGauge creates and registers a new Gauge.
func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &Gauge{name: name, help: help}
	r.register(g)
	return g
}

// Set sets the gauge to an absolute value.
func (g *Gauge) Set(v int64) { atomic.StoreInt64(&g.value, v) }

// Add adjusts the gauge by delta, which may be negative.
func (g *Gauge) Add(delta int64) { atomic.AddInt64(&g.value, delta) }

// Value returns the gauge's current value.
func (g *Gauge) Value() int64 { return atomic.LoadInt64(&g.value) }

func (g *Gauge) render(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", g.name, g.help, g.name, g.name, g.Value())
}

// GaugeVec is a gauge with a single label dimension (e.g. "level" for
// per-level SSTable counts). Safe for concurrent use; new label values
// are created lazily on first use.
type GaugeVec struct {
	name, help, labelName string

	mu     sync.Mutex
	values map[string]*int64
}

// NewGaugeVec creates and registers a new GaugeVec with the given label
// name (e.g. "level").
func (r *Registry) NewGaugeVec(name, help, labelName string) *GaugeVec {
	gv := &GaugeVec{name: name, help: help, labelName: labelName, values: make(map[string]*int64)}
	r.register(gv)
	return gv
}

// Set sets the gauge for the given label value to an absolute value.
func (gv *GaugeVec) Set(labelValue string, v int64) {
	gv.mu.Lock()
	p, ok := gv.values[labelValue]
	if !ok {
		p = new(int64)
		gv.values[labelValue] = p
	}
	gv.mu.Unlock()
	atomic.StoreInt64(p, v)
}

func (gv *GaugeVec) render(w io.Writer) {
	gv.mu.Lock()
	keys := make([]string, 0, len(gv.values))
	for k := range gv.values {
		keys = append(keys, k)
	}
	values := make(map[string]int64, len(gv.values))
	for k, p := range gv.values {
		values[k] = atomic.LoadInt64(p)
	}
	gv.mu.Unlock()

	sort.Strings(keys) // deterministic output
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", gv.name, gv.help, gv.name)
	for _, k := range keys {
		fmt.Fprintf(w, "%s{%s=%q} %d\n", gv.name, gv.labelName, k, values[k])
	}
}

// defaultLatencyBucketsNs are the upper bounds (in nanoseconds) used by
// NewLatencyHistogram, chosen to span typical KV-store operation
// latencies from well under a millisecond (in-memory hits) up past a
// second (a slow disk-bound compaction).
var defaultLatencyBucketsNs = []int64{
	int64(100 * time.Microsecond),
	int64(500 * time.Microsecond),
	int64(time.Millisecond),
	int64(5 * time.Millisecond),
	int64(10 * time.Millisecond),
	int64(50 * time.Millisecond),
	int64(100 * time.Millisecond),
	int64(500 * time.Millisecond),
	int64(time.Second),
	int64(5 * time.Second),
}

// Histogram tracks the distribution of a series of time.Duration
// observations using fixed buckets, rendered in the standard Prometheus
// cumulative histogram format (_bucket{le=...}, _sum, _count). Safe for
// concurrent use.
type Histogram struct {
	name, help string
	bounds     []int64 // ascending upper bounds in nanoseconds
	counts     []int64 // per-bucket (non-cumulative) counts; summed at render time
	sumNs      int64
	count      int64
}

// NewLatencyHistogram creates and registers a new Histogram using a
// fixed set of buckets suitable for operation latencies.
func (r *Registry) NewLatencyHistogram(name, help string) *Histogram {
	h := &Histogram{
		name:   name,
		help:   help,
		bounds: defaultLatencyBucketsNs,
		counts: make([]int64, len(defaultLatencyBucketsNs)),
	}
	r.register(h)
	return h
}

// Observe records a single duration observation.
func (h *Histogram) Observe(d time.Duration) {
	ns := int64(d)
	if ns < 0 {
		ns = 0
	}
	atomic.AddInt64(&h.sumNs, ns)
	atomic.AddInt64(&h.count, 1)

	idx := sort.Search(len(h.bounds), func(i int) bool { return h.bounds[i] >= ns })
	if idx == len(h.bounds) {
		return // falls only into the implicit +Inf bucket, covered by h.count
	}
	atomic.AddInt64(&h.counts[idx], 1)
}

func (h *Histogram) render(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	var cumulative int64
	for i, bound := range h.bounds {
		cumulative += atomic.LoadInt64(&h.counts[i])
		fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", h.name, formatSeconds(bound), cumulative)
	}
	total := atomic.LoadInt64(&h.count)
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.name, total)
	fmt.Fprintf(w, "%s_sum %s\n", h.name, formatSeconds(atomic.LoadInt64(&h.sumNs)))
	fmt.Fprintf(w, "%s_count %d\n", h.name, total)
}

// formatSeconds converts a nanosecond count to the seconds-as-float
// string Prometheus expects for duration values.
func formatSeconds(ns int64) string {
	return strconv.FormatFloat(float64(ns)/1e9, 'g', -1, 64)
}
