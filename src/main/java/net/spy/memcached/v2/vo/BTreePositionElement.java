package net.spy.memcached.v2.vo;

import java.util.Objects;

public final class BTreePositionElement<V> implements Comparable<BTreePositionElement<V>> {
  private final BTreeElement<V> element;
  private final int position;

  public BTreePositionElement(BKey bKey, V value, byte[] eFlag, int position) {
    this.element = new BTreeElement<>(bKey, value, eFlag);
    this.position = position;
  }

  public BKey getBKey() {
    return element.getBKey();
  }

  public V getValue() {
    return element.getValue();
  }

  public byte[] getEFlag() {
    return element.getEFlag();
  }

  public int getPosition() {
    return position;
  }

  @Override
  public int compareTo(BTreePositionElement<V> o) {
    return this.element.compareTo(o.element);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BTreePositionElement<?> that = (BTreePositionElement<?>) o;
    return Objects.equals(element, that.element) &&
        this.position == that.position;
  }

  @Override
  public int hashCode() {
    return Objects.hash(element, position);
  }
}
