package net.spy.memcached.auth;

import javax.security.auth.callback.CallbackHandler;

/**
 * Information required to specify authentication mechanisms and callbacks.
 */
public class AuthDescriptor {

  private final String[] mechs;
  private final CallbackHandler cbh;
  private int authAttempts;
  private final int allowedAuthAttempts;

  /**
   * Request authentication using the given list of mechanisms and callback
   * handler.
   *
   * @param m list of mechanisms
   * @param h the callback handler for grabbing credentials and stuff
   */
  public AuthDescriptor(String[] m, CallbackHandler h) {
    mechs = m;
    cbh = h;
    authAttempts = 0;
    String authThreshold = System.getProperty(
            "net.spy.memcached.auth.AuthThreshold");
    if (authThreshold != null) {
      allowedAuthAttempts = Integer.parseInt(authThreshold);
    } else {
      allowedAuthAttempts = -1; // auth forever
    }
  }

  /**
   * Get a typical auth descriptor for CRAM-MD5 or PLAIN auth with the given
   * username and password.
   *
   * @param u the username
   * @param p the password
   * @return an AuthDescriptor
   */
  public static AuthDescriptor typical(String u, String p) {
    return new AuthDescriptor(new String[]{"CRAM-MD5", "PLAIN"},
            new PlainCallbackHandler(u, p));
  }

  /**
   * Get a typical auth descriptor for SCRAM-SHA-256 auth with the given
   * username and password.
   *
   * @param u the username
   * @param p the password
   * @return an AuthDescriptor
   */
  public static AuthDescriptor scramSha256(String u, String p) {
    return new AuthDescriptor(new String[]{"SCRAM-SHA-256"},
            new PlainCallbackHandler(u, p));
  }

  public boolean authThresholdReached() {
    if (allowedAuthAttempts < 0) {
      return false; // negative value means auth forever
    } else if (authAttempts >= allowedAuthAttempts) {
      return true;
    } else {
      authAttempts++;
      return false;
    }
  }

  public String[] getMechs() {
    return mechs;
  }

  public CallbackHandler getCallback() {
    return cbh;
  }
}
