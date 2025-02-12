package net.spy.memcached.metrics;

import java.util.concurrent.atomic.LongAdder;

import net.spy.memcached.ArcusMBeanServer;

public final class OpThroughputMonitor implements OpThroughputMonitorMBean {
  private static final OpThroughputMonitor INSTANCE = new OpThroughputMonitor();

  private final LongAdder completeOps = new LongAdder();
  private final LongAdder cancelOps = new LongAdder();
  private final LongAdder timeOutOps = new LongAdder();
  private final boolean enabled;

  private OpThroughputMonitor() {
    if (System.getProperty("arcus.mbean", "false").toLowerCase().equals("false")) {
      enabled = false;
      return;
    }
    enabled = true;
    try {
      ArcusMBeanServer mbs = ArcusMBeanServer.getInstance();
      mbs.registMBean(this, this.getClass().getPackage().getName()
              + ":type=" + this.getClass().getSimpleName());
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Throughput MBean", e);
    }
  }

  public static OpThroughputMonitor getInstance() {
    return INSTANCE;
  }

  public void addCompletedOpCount() {
    if (!enabled) {
      return;
    }
    completeOps.increment();
  }

  public void addCanceledOpCount() {
    if (!enabled) {
      return;
    }
    cancelOps.increment();
  }

  public void addTimeOutedOpCount(int count) {
    if (!enabled) {
      return;
    }
    timeOutOps.add(count);
  }

  @Override
  public long getCompletedOps() {
    return completeOps.sum();
  }

  @Override
  public long getCanceledOps() {
    return cancelOps.sum();
  }

  @Override
  public long getTimeoutOps() {
    return timeOutOps.sum();
  }

  @Override
  public void resetStatistics() {
    completeOps.reset();
    cancelOps.reset();
    timeOutOps.reset();
  }
}
