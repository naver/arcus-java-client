package net.spy.memcached.v2.vo;

import net.spy.memcached.collection.ElementFlagUpdate;

public final class BTreeUpdateElement<V> {
  private final BKey bkey;
  private final V value;
  private final ElementFlagUpdate eFlagUpdate;

  private BTreeUpdateElement(BKey bkey, V value, ElementFlagUpdate eFlagUpdate) {
    if (bkey == null) {
      throw new IllegalArgumentException("BKey cannot be null.");
    }

    this.bkey = bkey;
    this.value = value;
    this.eFlagUpdate = eFlagUpdate;
  }

  public static <V> BTreeUpdateElement<V> withValue(BKey bkey, V value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    return new BTreeUpdateElement<>(bkey, value, null);
  }

  public static <V> BTreeUpdateElement<V> withEFlagUpdate(BKey bkey,
                                                          ElementFlagUpdate eFlagUpdate) {
    if (eFlagUpdate == null) {
      throw new IllegalArgumentException("EFlagUpdate cannot be null.");
    }
    return new BTreeUpdateElement<>(bkey, null, eFlagUpdate);
  }

  public static <V> BTreeUpdateElement<V> withValueAndEFlag(BKey bkey,
                                                            V value,
                                                            ElementFlagUpdate eFlagUpdate) {
    if (value == null || eFlagUpdate == null) {
      throw new IllegalArgumentException("Both value and eFlagUpdate cannot be null.");
    }
    return new BTreeUpdateElement<>(bkey, value, eFlagUpdate);
  }

  public BKey getBkey() {
    return bkey;
  }

  public V getValue() {
    return value;
  }

  public ElementFlagUpdate getEFlagUpdate() {
    return eFlagUpdate;
  }
}
