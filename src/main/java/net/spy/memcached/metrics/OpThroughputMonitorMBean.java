package net.spy.memcached.metrics;

public interface OpThroughputMonitorMBean {
  long getCompletedOps();

  long getCanceledOps();

  long getTimeoutOps();

  void resetStatistics();
}
