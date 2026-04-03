package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagUpdate;

public final class BTreeUpdateElement<V> {
  private final BKey bKey;
  private final V value;
  private final ElementFlagUpdate eFlagUpdate;

  private BTreeUpdateElement(BKey bKey, V value, ElementFlagUpdate eFlagUpdate) {
    if (bKey == null) {
      throw new IllegalArgumentException("BKey cannot be null.");
    }

    this.bKey = bKey;
    this.value = value;
    this.eFlagUpdate = eFlagUpdate;
  }

  public static <V> BTreeUpdateElement<V> withValue(BKey bKey, V value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    return new BTreeUpdateElement<>(bKey, value, null);
  }

  public static <V> BTreeUpdateElement<V> withEFlagUpdate(BKey bKey,
                                                          ElementFlagUpdate eFlagUpdate) {
    if (eFlagUpdate == null) {
      throw new IllegalArgumentException("EFlagUpdate cannot be null.");
    }
    return new BTreeUpdateElement<>(bKey, null, eFlagUpdate);
  }

  public static <V> BTreeUpdateElement<V> withValueAndEFlag(BKey bKey,
                                                            V value,
                                                            ElementFlagUpdate eFlagUpdate) {
    if (value == null || eFlagUpdate == null) {
      throw new IllegalArgumentException("Both value and eFlagUpdate cannot be null.");
    }
    return new BTreeUpdateElement<>(bKey, value, eFlagUpdate);
  }

  public BKey getBKey() {
    return bKey;
  }

  public V getValue() {
    return value;
  }

  public ElementFlagUpdate getEFlagUpdate() {
    return eFlagUpdate;
  }
}
