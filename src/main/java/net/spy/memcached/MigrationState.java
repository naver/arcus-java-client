package net.spy.memcached;

public enum MigrationState {
  UNKNOWN,
  BEGIN,
  PREPARED,
  PROGRESS,
  DONE
}
