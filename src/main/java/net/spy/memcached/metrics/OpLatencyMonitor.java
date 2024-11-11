package net.spy.memcached.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.spy.memcached.ArcusMBeanServer;

public final class OpLatencyMonitor implements OpLatencyMonitorMBean {

  private static final OpLatencyMonitor INSTANCE = new OpLatencyMonitor();
  private static final long CACHE_DURATION = 2000; // 2초 캐시
  private static final int WINDOW_SIZE = 10_000;

  private final AtomicReferenceArray<Long> latencies = new AtomicReferenceArray<>(WINDOW_SIZE);
  private final AtomicInteger currentIndex = new AtomicInteger(0);
  private final AtomicInteger count = new AtomicInteger(0);
  private final AtomicReference<LatencyMetricsSnapShot> cachedMetrics
          = new AtomicReference<>(LatencyMetricsSnapShot.empty());
  private final boolean enabled;

  private OpLatencyMonitor() {
    if (System.getProperty("arcus.mbean", "false").toLowerCase().equals("false")) {
      enabled = false;
      return;
    }
    enabled = true;
    for (int i = 0; i < WINDOW_SIZE; i++) {
      latencies.set(i, 0L);
    }

    try {
      ArcusMBeanServer mbs = ArcusMBeanServer.getInstance();
      mbs.registMBean(this, this.getClass().getPackage().getName()
              + ":type=" + this.getClass().getSimpleName());
    } catch (Exception e) {
      throw new RuntimeException("Failed to register MBean", e);
    }
  }

  public static OpLatencyMonitor getInstance() {
    return INSTANCE;
  }

  public void recordLatency(long startNanos) {
    if (!enabled) {
      return;
    }
    long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
    int index = currentIndex.getAndUpdate(i -> (i + 1) % WINDOW_SIZE);
    latencies.lazySet(index, latencyMicros);

    if (count.get() < WINDOW_SIZE) {
      count.incrementAndGet();
    }
  }

  // 모든 메트릭을 한 번에 계산하고 캐시하는 메서드
  private LatencyMetricsSnapShot computeMetrics() {
    int currentCount = count.get();
    if (currentCount == 0) {
      return LatencyMetricsSnapShot.empty();
    }

    // 현재 데이터를 배열로 복사
    List<Long> sortedLatencies = new ArrayList<>(currentCount);
    int startIndex = currentIndex.get();

    for (int i = 0; i < currentCount; i++) {
      int idx = (startIndex - i + WINDOW_SIZE) % WINDOW_SIZE;
      long value = latencies.get(idx);
      if (value > 0) {
        sortedLatencies.add(value);
      }
    }

    if (sortedLatencies.isEmpty()) {
      return LatencyMetricsSnapShot.empty();
    }

    sortedLatencies.sort(Long::compareTo);

    // 모든 메트릭을 한 번에 계산
    long avg = sortedLatencies.stream().mapToLong(Long::longValue).sum() / sortedLatencies.size();
    long min = sortedLatencies.get(0);
    long max = sortedLatencies.get(sortedLatencies.size() - 1);
    long p25 = sortedLatencies.get((int) Math.ceil((sortedLatencies.size() * 25.0) / 100.0) - 1);
    long p50 = sortedLatencies.get((int) Math.ceil((sortedLatencies.size() * 50.0) / 100.0) - 1);
    long p75 = sortedLatencies.get((int) Math.ceil((sortedLatencies.size() * 75.0) / 100.0) - 1);

    return new LatencyMetricsSnapShot(avg, min, max, p25, p50, p75);
  }

  // 캐시된 메트릭을 가져오거나 필요시 새로 계산
  private LatencyMetricsSnapShot getMetricsSnapshot() {
    LatencyMetricsSnapShot current = cachedMetrics.get();
    long now = System.currentTimeMillis();

    // 캐시가 유효한지 확인
    if (now - current.getTimestamp() < CACHE_DURATION) {
      return current;
    }

    // 새로운 메트릭 계산 및 캐시 업데이트
    LatencyMetricsSnapShot newMetrics = computeMetrics();
    cachedMetrics.set(newMetrics);
    return newMetrics;
  }

  @Override
  public long getAverageLatencyMicros() {
    return getMetricsSnapshot().getAvgLatency();
  }

  @Override
  public long getMinLatencyMicros() {
    return getMetricsSnapshot().getMinLatency();
  }

  @Override
  public long getMaxLatencyMicros() {
    return getMetricsSnapshot().getMaxLatency();
  }

  @Override
  public long get25thPercentileLatencyMicros() {
    return getMetricsSnapshot().getP25Latency();
  }

  @Override
  public long get50thPercentileLatencyMicros() {
    return getMetricsSnapshot().getP50Latency();
  }

  @Override
  public long get75thPercentileLatencyMicros() {
    return getMetricsSnapshot().getP75Latency();
  }

  @Override
  public void resetStatistics() {
    count.set(0);
    currentIndex.set(0);
    cachedMetrics.set(LatencyMetricsSnapShot.empty());
  }
}
