package engine

import "kvstore/metrics"

// engineMetrics holds every metric the engine instruments itself with.
// All of it is exposed to the outside world via Engine.MetricsRegistry,
// which a caller can mount at an HTTP /metrics endpoint (see the server
// package's debug server) for Prometheus-style scraping.
type engineMetrics struct {
	putTotal              *metrics.Counter
	deleteTotal           *metrics.Counter
	getTotal              *metrics.Counter
	getHitTotal           *metrics.Counter
	getMissTotal          *metrics.Counter
	scanTotal             *metrics.Counter
	flushTotal            *metrics.Counter
	flushFailedTotal      *metrics.Counter
	compactionTotal       *metrics.Counter
	compactionFailedTotal *metrics.Counter
	snapshotOpenTotal     *metrics.Counter

	memtableEntries    *metrics.Gauge
	immutableMemtables *metrics.Gauge
	sstablesTotal      *metrics.Gauge
	sstablesPerLevel   *metrics.GaugeVec
	activeSnapshots    *metrics.Gauge

	writeLatency      *metrics.Histogram
	getLatency        *metrics.Histogram
	flushLatency      *metrics.Histogram
	compactionLatency *metrics.Histogram
}

func newEngineMetrics(r *metrics.Registry) *engineMetrics {
	return &engineMetrics{
		putTotal:              r.NewCounter("kvstore_puts_total", "Total number of Put calls."),
		deleteTotal:           r.NewCounter("kvstore_deletes_total", "Total number of Delete calls."),
		getTotal:              r.NewCounter("kvstore_gets_total", "Total number of Get calls (including Snapshot.Get)."),
		getHitTotal:           r.NewCounter("kvstore_get_hits_total", "Total number of Get calls that found a value."),
		getMissTotal:          r.NewCounter("kvstore_get_misses_total", "Total number of Get calls that found no value."),
		scanTotal:             r.NewCounter("kvstore_scans_total", "Total number of Scan calls started (including Snapshot.Scan)."),
		flushTotal:            r.NewCounter("kvstore_flushes_total", "Total number of MemTable flushes attempted."),
		flushFailedTotal:      r.NewCounter("kvstore_flush_failures_total", "Total number of MemTable flushes that failed."),
		compactionTotal:       r.NewCounter("kvstore_compactions_total", "Total number of compaction passes attempted."),
		compactionFailedTotal: r.NewCounter("kvstore_compaction_failures_total", "Total number of compaction passes that failed."),
		snapshotOpenTotal:     r.NewCounter("kvstore_snapshots_opened_total", "Total number of snapshots opened."),

		memtableEntries:    r.NewGauge("kvstore_memtable_entries", "Current number of entries in the active MemTable."),
		immutableMemtables: r.NewGauge("kvstore_immutable_memtables", "Current number of immutable MemTables awaiting flush."),
		sstablesTotal:      r.NewGauge("kvstore_sstables_total", "Current total number of on-disk SSTables across all levels."),
		sstablesPerLevel:   r.NewGaugeVec("kvstore_sstables_per_level", "Current number of SSTables at each compaction level.", "level"),
		activeSnapshots:    r.NewGauge("kvstore_active_snapshots", "Current number of open snapshots."),

		writeLatency:      r.NewLatencyHistogram("kvstore_write_duration_seconds", "Put/Delete call latency in seconds."),
		getLatency:        r.NewLatencyHistogram("kvstore_get_duration_seconds", "Get call latency in seconds."),
		flushLatency:      r.NewLatencyHistogram("kvstore_flush_duration_seconds", "MemTable flush latency in seconds."),
		compactionLatency: r.NewLatencyHistogram("kvstore_compaction_duration_seconds", "Compaction pass latency in seconds."),
	}
}
