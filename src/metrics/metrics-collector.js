/**
 * MetricsCollector — In-Memory Observability Metrics Engine
 * 
 * Tracks system operational metrics, connection counts, produce/consume volumes,
 * error counts, replication performance, election events, and operation latencies.
 */

export class MetricsCollector {
  constructor() {
    this.counters = {
      connections_total: 0,
      active_connections: 0,
      messages_produced_total: 0,
      messages_consumed_total: 0,
      produce_errors_total: 0,
      consume_errors_total: 0,
      replication_success_total: 0,
      replication_failure_total: 0,
      replica_sync_total: 0,
      leader_elections_total: 0,
      leader_changes_total: 0
    };

    this.latencies = {
      produce: { totalMs: 0, count: 0, lastMs: 0 },
      consume: { totalMs: 0, count: 0, lastMs: 0 },
      replication: { totalMs: 0, count: 0, lastMs: 0 }
    };
  }

  /**
   * Increments a counter metric.
   * @param {string} name 
   * @param {number} [delta=1] 
   */
  increment(name, delta = 1) {
    if (this.counters[name] !== undefined) {
      this.counters[name] += delta;
    } else {
      this.counters[name] = delta;
    }
  }

  /**
   * Decrements a counter/gauge metric.
   * @param {string} name 
   * @param {number} [delta=1] 
   */
  decrement(name, delta = 1) {
    if (this.counters[name] !== undefined) {
      this.counters[name] = Math.max(0, this.counters[name] - delta);
    } else {
      this.counters[name] = 0;
    }
  }

  /**
   * Sets a gauge metric value directly.
   * @param {string} name 
   * @param {number} value 
   */
  setGauge(name, value) {
    this.counters[name] = Math.max(0, value);
  }

  /**
   * Records execution latency in milliseconds.
   * @param {'produce'|'consume'|'replication'} name 
   * @param {number} durationMs 
   */
  recordLatency(name, durationMs) {
    if (this.latencies[name]) {
      this.latencies[name].totalMs += durationMs;
      this.latencies[name].count += 1;
      this.latencies[name].lastMs = durationMs;
    }
  }

  /**
   * Returns current metrics snapshot as a JSON object.
   * @returns {object}
   */
  getMetricsJSON() {
    const latSummary = {};
    for (const [key, val] of Object.entries(this.latencies)) {
      const avg = val.count > 0 ? Number((val.totalMs / val.count).toFixed(2)) : 0;
      latSummary[`${key}_latency_avg_ms`] = avg;
      latSummary[`${key}_latency_last_ms`] = Number(val.lastMs.toFixed(2));
    }

    return {
      ...this.counters,
      ...latSummary
    };
  }

  /**
   * Returns metrics formatted as machine-readable plain text.
   * @returns {string}
   */
  getMetricsFormatted() {
    const json = this.getMetricsJSON();
    const lines = [];
    for (const [key, value] of Object.entries(json)) {
      lines.push(`${key} ${value}`);
    }
    return lines.join('\n') + '\n';
  }

  /**
   * Resets all metrics counters and latencies to 0.
   */
  reset() {
    for (const key of Object.keys(this.counters)) {
      this.counters[key] = 0;
    }
    for (const key of Object.keys(this.latencies)) {
      this.latencies[key] = { totalMs: 0, count: 0, lastMs: 0 };
    }
  }
}

// Export singleton instance
export const metricsCollector = new MetricsCollector();
