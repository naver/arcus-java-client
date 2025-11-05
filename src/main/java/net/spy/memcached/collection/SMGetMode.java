package net.spy.memcached.collection;

/**
 * A type component for "bop smgetmode"
 */
public enum SMGetMode {
  @Deprecated
  UNIQUE("unique"),
  @Deprecated
  DUPLICATE("duplicate");

  private String mode;

  SMGetMode(String mode) {
    this.mode = mode;
  }

  @Deprecated
  public String getMode() {
    return mode;
  }
}
