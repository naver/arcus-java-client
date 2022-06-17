package net.spy.memcached.protocol.ascii.callback;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public abstract class BaseOperationCallback {

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
