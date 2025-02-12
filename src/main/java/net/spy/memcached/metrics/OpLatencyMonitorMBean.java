package net.spy.memcached.metrics;

public interface OpLatencyMonitorMBean {
  long getAverageLatencyMicros();

  long getMaxLatencyMicros();

  long getMinLatencyMicros();

  long get25thPercentileLatencyMicros();

  long get50thPercentileLatencyMicros();

  long get75thPercentileLatencyMicros();

  void resetStatistics();
}
