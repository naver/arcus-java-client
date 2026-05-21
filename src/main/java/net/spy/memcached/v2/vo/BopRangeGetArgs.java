package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagFilter;

public class BopRangeGetArgs {

  public static final BopRangeGetArgs DEFAULT
      = new BopRangeGetArgs(ElementFlagFilter.DO_NOT_FILTER, 0, 0, GetMode.NONE);

  private final ElementFlagFilter eFlagFilter;
  private final int offset;
  private final int count;
  private final GetMode mode;

  public BopRangeGetArgs(ElementFlagFilter eFlagFilter, int offset, int count, GetMode mode) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset cannot be negative");
    }

    if (count < 0) {
      throw new IllegalArgumentException("count cannot be negative");
    }

    if (mode == null) {
      throw new IllegalArgumentException("mode cannot be null");
    }

    this.eFlagFilter = eFlagFilter;
    this.offset = offset;
    this.count = count;
    this.mode = mode;
  }

  public ElementFlagFilter getEFlagFilter() {
    return eFlagFilter;
  }

  public int getOffset() {
    return offset;
  }

  public int getCount() {
    return count;
  }

  public boolean isWithDelete() {
    return mode.isWithDelete();
  }

  public boolean isDropIfEmpty() {
    return mode.isDropIfEmpty();
  }
}
