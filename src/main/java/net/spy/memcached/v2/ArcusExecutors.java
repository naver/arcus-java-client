package net.spy.memcached.v2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides shared thread pool executors for Arcus v2 async operations.
 */
final class ArcusExecutors {

  /**
   * Shared executor for decoding and combining results.
   * Used by both ArcusFutureImpl and ArcusMultiFuture.
   * Suppressing deprecation warning for JDK 19 and later where
   * Thread.getId() is deprecated.
   */
  @SuppressWarnings("deprecation")
  static final ExecutorService COMPLETION_EXECUTOR =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
          r -> {
            Thread t = new Thread(r);
            t.setName("arcus-completion-" + t.getId());
            t.setDaemon(true);
            return t;
          });

  private ArcusExecutors() {
  }
}
