package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagFilter;

public final class BopGetArgs {

  public static final BopGetArgs DEFAULT
      = new BopGetArgs(ElementFlagFilter.DO_NOT_FILTER, GetMode.NONE);

  private final ElementFlagFilter eFlagFilter;
  private final GetMode mode;

  public BopGetArgs(ElementFlagFilter eFlagFilter, GetMode mode) {
    if (mode == null) {
      throw new IllegalArgumentException("mode cannot be null");
    }

    this.eFlagFilter = eFlagFilter;
    this.mode = mode;
  }

  public ElementFlagFilter getEFlagFilter() {
    return eFlagFilter;
  }

  public boolean isWithDelete() {
    return mode.isWithDelete();
  }

  public boolean isDropIfEmpty() {
    return mode.isDropIfEmpty();
  }
}
