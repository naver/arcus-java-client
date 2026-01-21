package net.spy.memcached.v2.vo;

import java.util.Arrays;
import java.util.Objects;

import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.util.BTreeUtil;

public final class BKey implements Comparable<BKey> {
  private final BKeyType type;
  private final Object data;

  private BKey(long data) {
    if (data < 0) {
      throw new IllegalArgumentException("BKey long value cannot be negative.");
    }
    this.type = BKeyType.LONG;
    this.data = data;
  }

  private BKey(byte[] data) {
    if (data == null) {
      throw new IllegalArgumentException("BKey byte array cannot be null.");
    }

    if (data.length > 31) {
      throw new IllegalArgumentException(
          "BKey byte array size must be between 0 and 31. Given size: " + data.length);
    }

    this.type = BKeyType.BYTE_ARRAY;
    this.data = Arrays.copyOf(data, data.length);
  }

  public static BKey of(Object bKey) {
    if (bKey == null) {
      throw new IllegalArgumentException("BKey cannot be null");
    }
    if (bKey instanceof Long) {
      return new BKey((Long) bKey);
    } else if (bKey instanceof byte[]) {
      return new BKey((byte[]) bKey);
    } else if (bKey instanceof String) {
      String bkeyString = (String) bKey;
      try {
        return new BKey(Long.parseLong(bkeyString));
      } catch (NumberFormatException e) {
        return new BKey(BTreeUtil.hexStringToByteArrays(bkeyString));
      }
    } else {
      throw new IllegalArgumentException("Unsupported BKey type: " + bKey.getClass());
    }
  }

  public static BKey of(BKeyObject bkeyObject) {
    if (bkeyObject == null) {
      throw new IllegalArgumentException("BKeyObject cannot be null");
    }

    if (bkeyObject.isByteArray()) {
      return new BKey(bkeyObject.getByteArrayBKeyRaw());
    } else {
      return new BKey(bkeyObject.getLongBKey());
    }
  }

  public enum BKeyType {
    BYTE_ARRAY,
    LONG;
  }

  public Object getData() {
    if (type == BKeyType.BYTE_ARRAY) {
      byte[] bytes = (byte[]) data;
      return Arrays.copyOf(bytes, bytes.length);
    }
    return data;
  }

  public BKeyType getType() {
    return type;
  }

  @Override
  public int compareTo(BKey o) {
    if (this.type != o.type) {
      throw new IllegalArgumentException("Cannot compare different BKey types.");
    }

    if (this.type == BKeyType.LONG) {
      return ((Long) this.data).compareTo((Long) o.data);
    } else {
      return BTreeUtil.compareByteArraysInLexOrder((byte[]) this.data, (byte[]) o.data);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    BKey bKey = (BKey) o;
    if (this.type != bKey.type) {
      return false;
    }

    if (this.type == BKeyType.LONG) {
      return this.data.equals(bKey.data);
    } else {
      return Arrays.equals((byte[]) this.data, (byte[]) bKey.data);
    }
  }

  @Override
  public int hashCode() {
    if (this.type == BKeyType.LONG) {
      return Objects.hash(this.type, this.data);
    } else {
      return Objects.hash(this.type, Arrays.hashCode((byte[]) this.data));
    }
  }

  @Override
  public String toString() {
    if (this.type == BKeyType.LONG) {
      return String.valueOf(this.data);
    } else {
      return BTreeUtil.toHex((byte[]) this.data);
    }
  }
}
