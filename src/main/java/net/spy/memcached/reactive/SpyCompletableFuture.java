package net.spy.memcached.reactive;

import java.util.concurrent.CompletableFuture;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public class SpyCompletableFuture<T> extends CompletableFuture<T> {
  private transient Logger logger = null;

  /**
   * Get a Logger instance for this class.
   *
   * @return an appropriate logger instance.
   */
  protected Logger getLogger() {
    if (logger == null) {
      logger = LoggerFactory.getLogger(getClass());
    }
    return (logger);
  }
}
