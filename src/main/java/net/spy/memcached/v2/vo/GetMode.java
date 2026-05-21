package net.spy.memcached.v2.vo;

public enum GetMode {
  /**
   * Retrieves elements without deleting them.
   */
  NONE(false, false),

  /**
   * Retrieves elements and deletes them from the collection.
   */
  DELETE(true, false),

  /**
   * Retrieves elements, deletes them, and drops the collection if it becomes empty.
   */
  DROP(true, true);

  private final boolean withDelete;
  private final boolean dropIfEmpty;

  GetMode(boolean withDelete, boolean dropIfEmpty) {
    this.withDelete = withDelete;
    this.dropIfEmpty = dropIfEmpty;
  }

  public boolean isWithDelete() {
    return withDelete;
  }

  public boolean isDropIfEmpty() {
    return dropIfEmpty;
  }
}
