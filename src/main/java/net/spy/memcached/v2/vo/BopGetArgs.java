package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagFilter;

public final class BopGetArgs {

  public static final BopGetArgs DEFAULT = new BopGetArgs.Builder().build();

  private final ElementFlagFilter eFlagFilter;
  private final int offset;
  private final int count;
  private final boolean withDelete;
  private final boolean dropIfEmpty;

  private BopGetArgs(ElementFlagFilter eFlagFilter, int offset, int count,
                     boolean withDelete, boolean dropIfEmpty) {
    this.eFlagFilter = eFlagFilter;
    this.offset = offset;
    this.count = count;
    this.withDelete = withDelete;
    this.dropIfEmpty = dropIfEmpty;
  }

  public ElementFlagFilter getElementFlagFilter() {
    return eFlagFilter;
  }

  public int getOffset() {
    return offset;
  }

  public int getCount() {
    return count;
  }

  public boolean isWithDelete() {
    return withDelete;
  }

  public boolean isDropIfEmpty() {
    return dropIfEmpty;
  }

  public static final class Builder {
    private ElementFlagFilter eFlagFilter = null;
    private int offset = 0;
    private int count = 50;
    private boolean withDelete = false;
    private boolean dropIfEmpty = false;

    public Builder eFlagFilter(ElementFlagFilter eFlagFilter) {
      this.eFlagFilter = eFlagFilter;
      return this;
    }

    /**
     * Set the offset only for {@code AsyncArcusCommands#bopGet}
     * or {@code AsyncArcusCommands#bopMultiGet}
     *
     * @param offset to skip elements that match condition from the 'from' BKey
     */
    public Builder offset(int offset) {
      if (offset < 0) {
        throw new IllegalArgumentException("offset cannot be negative");
      }
      this.offset = offset;
      return this;
    }

    /**
     * Set the count of elements to retrieve.
     *
     * @param count For bopGet or bopMultiGet method,
     *              set the number of elements to retrieve from each BTree item.
     *              For bopSortMergeGet method,
     *              set the total number of elements to retrieve across all BTree items.
     */
    public Builder count(int count) {
      if (count < 0) {
        throw new IllegalArgumentException("count cannot be negative");
      }
      this.count = count;
      return this;
    }

    public Builder withDelete() {
      this.withDelete = true;
      return this;
    }

    public Builder dropIfEmpty() {
      this.dropIfEmpty = true;
      return this;
    }

    public BopGetArgs build() {
      return new BopGetArgs(eFlagFilter, offset, count, withDelete, dropIfEmpty);
    }
  }
}
