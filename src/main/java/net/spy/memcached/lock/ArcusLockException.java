package net.spy.memcached.lock;

import net.spy.memcached.ArcusClientException;

public class ArcusLockException extends ArcusClientException {

  private static final long serialVersionUID = -5312402618576657589L;

  public ArcusLockException(String message) {
    super(message);
  }

  public ArcusLockException(String message, Throwable e) {
    super(message, e);
  }
}
