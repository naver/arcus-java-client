package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagFilter;

public class BopSMGetArgs {

  private static final int MAX_SMGET_COUNT = 1000;

  public static final BopSMGetArgs DEFAULT
      = new BopSMGetArgs(ElementFlagFilter.DO_NOT_FILTER, MAX_SMGET_COUNT, false);

  private final ElementFlagFilter eFlagFilter;
  private final int count;
  private final boolean unique;

  public BopSMGetArgs(ElementFlagFilter eFlagFilter, int count, boolean unique) {
    if (count < 1 || count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException("count must be between 1 and 1000");
    }

    this.eFlagFilter = eFlagFilter;
    this.count = count;
    this.unique = unique;
  }

  public ElementFlagFilter getEFlagFilter() {
    return eFlagFilter;
  }

  public int getCount() {
    return count;
  }

  public boolean isUnique() {
    return unique;
  }
}
