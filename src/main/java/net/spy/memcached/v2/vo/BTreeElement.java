package net.spy.memcached.v2.vo;

import java.util.Arrays;
import java.util.Objects;

public final class BTreeElement<V> implements Comparable<BTreeElement<V>> {
  private final BKey bKey;
  private final V value;
  private final byte[] eFlag;

  public BTreeElement(BKey bKey, V value, byte[] eFlag) {
    if (bKey == null) {
      throw new IllegalArgumentException("BKey cannot be null");
    }
    this.bKey = bKey;
    this.value = value;
    this.eFlag = eFlag;
  }

  public BKey getBKey() {
    return bKey;
  }

  public V getValue() {
    return value;
  }

  public byte[] getEFlag() {
    return eFlag;
  }

  @Override
  public int compareTo(BTreeElement<V> o) {
    return this.bKey.compareTo(o.bKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BTreeElement<?> that = (BTreeElement<?>) o;
    return Objects.equals(bKey, that.bKey) &&
        Objects.equals(value, that.value) && Objects.deepEquals(eFlag, that.eFlag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bKey, value, Arrays.hashCode(eFlag));
  }
}
