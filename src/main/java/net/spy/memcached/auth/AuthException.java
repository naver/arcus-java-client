package net.spy.memcached.auth;

import net.spy.memcached.internal.ReconnDelay;

public class AuthException extends RuntimeException {
  private static final long serialVersionUID = 7788260043728294174L;

  private final ReconnDelay reconnDelay;

  public AuthException(String message, ReconnDelay delay) {
    super(message);

    this.reconnDelay = delay;
  }

  public ReconnDelay getReconnDelay() {
    return reconnDelay;
  }
}
