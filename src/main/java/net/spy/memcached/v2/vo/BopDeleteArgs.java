package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagFilter;

public final class BopDeleteArgs {

  public static final BopDeleteArgs DEFAULT = new BopDeleteArgs.Builder().build();

  private final ElementFlagFilter eFlagFilter;
  private final int count;
  private final boolean dropIfEmpty;

  private BopDeleteArgs(ElementFlagFilter eFlagFilter, int count, boolean dropIfEmpty) {
    this.eFlagFilter = eFlagFilter;
    this.count = count;
    this.dropIfEmpty = dropIfEmpty;
  }

  public ElementFlagFilter getEFlagFilter() {
    return eFlagFilter;
  }

  public int getCount() {
    return count;
  }

  public boolean isDropIfEmpty() {
    return dropIfEmpty;
  }

  public static class Builder {
    private ElementFlagFilter eFlagFilter = ElementFlagFilter.DO_NOT_FILTER;
    private int count = 0;
    private boolean dropIfEmpty = false;

    public Builder eFlagFilter(ElementFlagFilter eFlagFilter) {
      this.eFlagFilter = eFlagFilter;
      return this;
    }

    public Builder count(int count) {
      if (count < 0) {
        throw new IllegalArgumentException("Count must be non-negative.");
      }
      this.count = count;
      return this;
    }

    public Builder dropIfEmpty() {
      this.dropIfEmpty = true;
      return this;
    }

    public BopDeleteArgs build() {
      return new BopDeleteArgs(eFlagFilter, count, dropIfEmpty);
    }
  }
}
