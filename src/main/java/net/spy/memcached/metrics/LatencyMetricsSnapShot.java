package net.spy.memcached.metrics;

class LatencyMetricsSnapShot {
  private static final LatencyMetricsSnapShot EMPTY = new LatencyMetricsSnapShot(0, 0, 0, 0, 0, 0);

  private final long avgLatency;
  private final long minLatency;
  private final long maxLatency;
  private final long p25Latency;
  private final long p50Latency;
  private final long p75Latency;
  private final long timestamp;  // 캐시 생성 시간

  LatencyMetricsSnapShot(long avg, long min, long max, long p25, long p50, long p75) {
    this.avgLatency = avg;
    this.minLatency = min;
    this.maxLatency = max;
    this.p25Latency = p25;
    this.p50Latency = p50;
    this.p75Latency = p75;
    this.timestamp = System.currentTimeMillis();
  }

  public static LatencyMetricsSnapShot empty() {
    return EMPTY;
  }

  public long getAvgLatency() {
    return avgLatency;
  }

  public long getMinLatency() {
    return minLatency;
  }

  public long getMaxLatency() {
    return maxLatency;
  }

  public long getP25Latency() {
    return p25Latency;
  }

  public long getP50Latency() {
    return p50Latency;
  }

  public long getP75Latency() {
    return p75Latency;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
