package net.spy.memcached.collection;

/**
 * A type component for "bop smgetmode"
 */
public enum SMGetMode {
  UNIQUE("unique"),
  DUPLICATE("duplicate");

  private String mode;

  SMGetMode(String mode) {
    this.mode = mode;
  }

  public String getMode() {
    return mode;
  }
}