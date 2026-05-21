package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagFilter;

public class BopMultiGetArgs {

  private static final int MAX_MGET_COUNT = 50;

  public static final BopMultiGetArgs DEFAULT
      = new BopMultiGetArgs(ElementFlagFilter.DO_NOT_FILTER, 0, MAX_MGET_COUNT);

  private final ElementFlagFilter eFlagFilter;
  private final int offset;
  private final int count;

  public BopMultiGetArgs(ElementFlagFilter eFlagFilter, int offset, int count) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset cannot be negative");
    }

    if (count < 1 || count > MAX_MGET_COUNT) {
      throw new IllegalArgumentException("count must be between 1 and 50");
    }

    this.eFlagFilter = eFlagFilter;
    this.offset = offset;
    this.count = count;
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
}
