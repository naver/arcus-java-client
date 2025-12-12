/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.collection;

import java.util.Objects;

import net.spy.memcached.KeyValidator;
import net.spy.memcached.util.BTreeUtil;

public class BKeyObject implements Comparable<BKeyObject> {

  public enum BKeyType {
    LONG, BYTEARRAY
  }

  private final BKeyType type;
  private final Long longBKey;
  private final ByteArrayBKey byteArrayBKey;

  public BKeyObject(long longBKey) {
    KeyValidator.validateBKey(longBKey);
    this.type = BKeyType.LONG;
    this.longBKey = longBKey;
    this.byteArrayBKey = null;
  }

  public BKeyObject(byte[] byteArrayBKey) {
    KeyValidator.validateBKey(byteArrayBKey);
    this.type = BKeyType.BYTEARRAY;
    this.longBKey = null;
    this.byteArrayBKey = new ByteArrayBKey(byteArrayBKey);
  }

  public BKeyType getType() {
    return type;
  }

  public Long getLongBKey() {
    if (isLong()) {
      return longBKey;
    }
    throw new IllegalStateException("This Object has byte[] bkey.");
  }

  public ByteArrayBKey getByteArrayBKey() {
    if (isByteArray()) {
      return byteArrayBKey;
    }
    throw new IllegalStateException("This Object has java.lang.Long type bkey.");
  }

  public byte[] getByteArrayBKeyRaw() {
    if (isByteArray()) {
      return byteArrayBKey.getBytes();
    }
    throw new IllegalStateException("This Object has java.lang.Long type bkey.");
  }

  public boolean isLong() {
    return type == BKeyType.LONG;
  }

  public boolean isByteArray() {
    return type == BKeyType.BYTEARRAY;
  }

  @Override
  public int compareTo(BKeyObject another) {
    if (isByteArray() && another.isByteArray()) {
      return byteArrayBKey.compareTo(another.getByteArrayBKey());
    } else if (isLong() && another.isLong()) {
      return longBKey.compareTo(another.getLongBKey());
    }
    throw new IllegalArgumentException("not supported comparing different type of bkey");
  }

  @Override
  public String toString() {
    if (isLong()) {
      return String.valueOf(longBKey);
    }
    return BTreeUtil.toHex(byteArrayBKey.getBytes());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BKeyObject)) {
      return false;
    }

    BKeyObject that = (BKeyObject) o;
    return type == that.type &&
            Objects.equals(longBKey, that.longBKey) &&
            Objects.equals(byteArrayBKey, that.byteArrayBKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, longBKey, byteArrayBKey);
  }

}
