package net.spy.memcached;

public enum MigrationState {
  UNKNOWN,
  BEGIN,
  PREPARED,
  DONE;

  public static MigrationState fromString(String state) {
    if ("BEGIN".equals(state)) {
      return BEGIN;
    } else if ("PREPARED".equals(state)) {
      return PREPARED;
    } else if ("DONE".equals(state)) {
      return DONE;
    }

    return UNKNOWN;
  }
}
