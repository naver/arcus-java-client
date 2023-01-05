package net.spy.memcached;

public enum MigrationType {
  UNKNOWN,
  JOIN,
  LEAVE;

  public static MigrationType fromString(String type) {
    if ("JOIN".equals(type)) {
      return JOIN;
    } else if ("LEAVE".equals(type)) {
      return LEAVE;
    }
    return UNKNOWN;
  }
}
