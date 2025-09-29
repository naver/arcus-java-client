package net.spy.memcached.auth;

public class AuthException extends RuntimeException {
  private static final long serialVersionUID = 7788260043728294174L;

  public AuthException(String message) {
    super(message);
  }
}
