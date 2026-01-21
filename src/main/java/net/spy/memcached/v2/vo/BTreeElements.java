package net.spy.memcached.v2.vo;

import java.util.Collections;
import java.util.List;

public final class BTreeElements<V> {
  private boolean isTrimmed;
  private final List<BTreeElement<V>> elements;

  public BTreeElements(List<BTreeElement<V>> elements) {
    if (elements == null) {
      throw new IllegalArgumentException("Elements map cannot be null");
    }
    this.elements = elements;
  }

  public boolean isTrimmed() {
    return isTrimmed;
  }

  public List<BTreeElement<V>> getElements() {
    return Collections.unmodifiableList(elements);
  }

  public void trimmed() {
    this.isTrimmed = true;
  }

  public void addElement(BTreeElement<V> element) {
    this.elements.add(element);
  }
}
