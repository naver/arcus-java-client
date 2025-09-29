package net.spy.memcached.auth;

import net.spy.memcached.internal.ReconnDelay;

public class AuthException extends RuntimeException {
  private static final long serialVersionUID = -3995370629401633195L;

  private final AuthExceptionType type;
  private final ReconnDelay delay;

  public AuthException(String message, AuthExceptionType type, ReconnDelay delay) {
    super(message);

    this.type = type;
    this.delay = delay;
  }

  public AuthExceptionType getType() {
    return type;
  }

  public ReconnDelay getDelay() {
    return delay;
  }
}
