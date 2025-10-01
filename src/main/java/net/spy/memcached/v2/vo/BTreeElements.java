package net.spy.memcached.v2.vo;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

public final class BTreeElements<V> {
  private boolean isTrimmed;
  private final SortedMap<BKey, BTreeElement<V>> elements;

  public BTreeElements(SortedMap<BKey, BTreeElement<V>> elements) {
    if (elements == null) {
      throw new IllegalArgumentException("Elements map cannot be null");
    }
    this.elements = elements;
  }

  public boolean isTrimmed() {
    return isTrimmed;
  }

  public Map<BKey, BTreeElement<V>> getElements() {
    return Collections.unmodifiableMap(elements);
  }

  public void trimmed() {
    this.isTrimmed = true;
  }

  public void addElement(BKey bkey, BTreeElement<V> element) {
    this.elements.put(bkey, element);
  }
}
