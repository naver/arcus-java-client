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

import java.util.Arrays;
import java.util.Objects;

import net.spy.memcached.util.BTreeUtil;

public class SMGetElement<T> implements Comparable<SMGetElement<T>> {

  private final String key;
  private final BKeyObject bKeyObject;
  private final byte[] eflag;
  private final T value;

  public SMGetElement(String key, long bkey, byte[] eflag, T value) {
    this.key = key;
    this.bKeyObject = new BKeyObject(bkey);
    this.eflag = eflag;
    this.value = value;
  }

  public SMGetElement(String key, byte[] bkey, byte[] eflag, T value) {
    this.key = key;
    this.bKeyObject = new BKeyObject(bkey);
    this.eflag = eflag;
    this.value = value;
  }

  @Override
  public String toString() {
    if (eflag != null) {
      return "SMGetElement {KEY:" + key + ", BKEY:" + bKeyObject
          + ", EFLAG: " + BTreeUtil.toHex(eflag) +  ", VALUE:" + value + "}";
    }
    return "SMGetElement {KEY:" + key + ", BKEY:" + bKeyObject + ", VALUE:" + value + "}";
  }

  @Override
  public int compareTo(SMGetElement<T> param) {
    assert param != null;

    /* compare bkey */
    int comp = bKeyObject.compareTo(param.bKeyObject);

    /* if bkey is equal, then compare key */
    if (comp == 0) {
      comp = key.compareTo(param.getKey());
    }

    return comp;
  }

  public int compareBkeyTo(SMGetElement<T> param) {
    assert param != null;

    /* compare bkey */
    return bKeyObject.compareTo(param.bKeyObject);
  }

  public int compareKeyTo(SMGetElement<T> param) {
    assert param != null;

    /* compare key */
    return key.compareTo(param.getKey());
  }

  public String getKey() {
    return key;
  }

  public long getBkey() {
    return bKeyObject.getLongBKey();
  }

  public byte[] getByteBkey() {
    return bKeyObject.getByteArrayBKeyRaw();
  }

  public BKeyObject getBkeyObject() {
    return bKeyObject;
  }

  public byte[] getEflag() {
    return eflag;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SMGetElement)) {
      return false;
    }

    SMGetElement<?> that = (SMGetElement<?>) o;
    return Objects.equals(key, that.key) &&
            Objects.equals(bKeyObject, that.bKeyObject) &&
            Arrays.equals(eflag, that.eflag) &&
            Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, bKeyObject, Arrays.hashCode(eflag), value);
  }

}
