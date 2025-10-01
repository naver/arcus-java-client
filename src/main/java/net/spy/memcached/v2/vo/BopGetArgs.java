package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagFilter;

public final class BopGetArgs {
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
    private ElementFlagFilter eFlagFilter;
    private int offset = 0;
    private int count = 50;
    private boolean withDelete = false;
    private boolean dropIfEmpty = false;

    public Builder eFlagFilter(ElementFlagFilter eFlagFilter) {
      this.eFlagFilter = eFlagFilter;
      return this;
    }

    public Builder offset(int offset) {
      if (offset < 0) {
        throw new IllegalArgumentException("offset cannot be negative");
      }
      this.offset = offset;
      return this;
    }

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
