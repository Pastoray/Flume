package metrics

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCounterAndGaugeRendering(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("kvstore_puts_total", "total puts")
	c.Inc()
	c.Add(4)

	g := r.NewGauge("kvstore_memtable_entries", "current memtable entries")
	g.Set(42)

	var buf bytes.Buffer
	if err := r.Render(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, "kvstore_puts_total 5\n") {
		t.Fatalf("expected counter value 5 in output:\n%s", out)
	}
	if !strings.Contains(out, "kvstore_memtable_entries 42\n") {
		t.Fatalf("expected gauge value 42 in output:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE kvstore_puts_total counter") {
		t.Fatalf("expected TYPE line for counter:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE kvstore_memtable_entries gauge") {
		t.Fatalf("expected TYPE line for gauge:\n%s", out)
	}
}

func TestGaugeVecPerLabel(t *testing.T) {
	r := NewRegistry()
	gv := r.NewGaugeVec("kvstore_sstables_per_level", "sstables per level", "level")
	gv.Set("0", 3)
	gv.Set("1", 7)
	gv.Set("0", 5) // overwrite

	var buf bytes.Buffer
	if err := r.Render(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, `kvstore_sstables_per_level{level="0"} 5`) {
		t.Fatalf("expected level 0 = 5 in output:\n%s", out)
	}
	if !strings.Contains(out, `kvstore_sstables_per_level{level="1"} 7`) {
		t.Fatalf("expected level 1 = 7 in output:\n%s", out)
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	r := NewRegistry()
	h := r.NewLatencyHistogram("kvstore_get_duration_seconds", "get latency")

	h.Observe(50 * time.Microsecond) // falls in first bucket (100µs)
	h.Observe(50 * time.Microsecond) // same
	h.Observe(2 * time.Millisecond)  // falls in the 5ms bucket
	h.Observe(10 * time.Second)      // falls only in +Inf

	var buf bytes.Buffer
	if err := r.Render(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := buf.String()

	// The 100µs bucket should show 2 (the two 50µs observations).
	if !strings.Contains(out, `le="0.0001"} 2`) {
		t.Fatalf("expected 100us bucket = 2 in output:\n%s", out)
	}
	// The 5ms bucket should show 3 (cumulative: 2 + the 2ms observation).
	if !strings.Contains(out, `le="0.005"} 3`) {
		t.Fatalf("expected 5ms bucket cumulative = 3 in output:\n%s", out)
	}
	// +Inf must reflect the total count, including the 10s outlier.
	if !strings.Contains(out, `le="+Inf"} 4`) {
		t.Fatalf("expected +Inf bucket = 4 in output:\n%s", out)
	}
	if !strings.Contains(out, "kvstore_get_duration_seconds_count 4") {
		t.Fatalf("expected _count = 4 in output:\n%s", out)
	}
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("c", "c")
	g := r.NewGauge("g", "g")
	gv := r.NewGaugeVec("gv", "gv", "label")
	h := r.NewLatencyHistogram("h", "h")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Inc()
				g.Set(int64(j))
				gv.Set("x", int64(j))
				h.Observe(time.Duration(j) * time.Microsecond)
			}
		}(i)
	}
	wg.Wait()

	var buf bytes.Buffer
	if err := r.Render(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if c.Value() != 2000 {
		t.Fatalf("expected counter = 2000, got %d", c.Value())
	}
}
