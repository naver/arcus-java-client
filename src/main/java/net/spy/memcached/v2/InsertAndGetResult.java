package net.spy.memcached.v2;

import net.spy.memcached.v2.vo.BTreeElement;

public final class InsertAndGetResult<T> {
  private final boolean isInserted;
  private final BTreeElement<T> trimmedElement;

  public InsertAndGetResult(boolean insertSuccessful, BTreeElement<T> trimmedElement) {
    this.isInserted = insertSuccessful;
    this.trimmedElement = trimmedElement;
  }

  public boolean isInserted() {
    return isInserted;
  }

  public BTreeElement<T> getTrimmedElement() {
    return trimmedElement;
  }
}
