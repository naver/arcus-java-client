package net.spy.memcached.v2;

public enum StatsArg {
  GENERAL(null),
  SETTINGS("settings"),
  ITEMS("items"),
  SLABS("slabs"),
  PREFIXES("prefixes");

  private final String arg;

  StatsArg(String arg) {
    this.arg = arg;
  }

  public String getArg() {
    return arg;
  }

}
