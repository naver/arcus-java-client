package net.spy.memcached.v2.vo;

import java.util.Arrays;
import java.util.Objects;

public final class BTreeElement<V> implements Comparable<BTreeElement<V>> {
  private final BKey bkey;
  private final V value;
  private final byte[] eFlag;

  public BTreeElement(BKey bkey, V value, byte[] eFlag) {
    if (bkey == null) {
      throw new IllegalArgumentException("BKey cannot be null");
    }
    this.bkey = bkey;
    this.value = value;
    this.eFlag = eFlag;
  }

  public BKey getBkey() {
    return bkey;
  }

  public V getValue() {
    return value;
  }

  public byte[] getEFlag() {
    return eFlag;
  }

  @Override
  public int compareTo(BTreeElement<V> o) {
    return this.bkey.compareTo(o.bkey);
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
    return Objects.equals(bkey, that.bkey) &&
            Objects.equals(value, that.value) && Objects.deepEquals(eFlag, that.eFlag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bkey, value, Arrays.hashCode(eFlag));
  }
}
