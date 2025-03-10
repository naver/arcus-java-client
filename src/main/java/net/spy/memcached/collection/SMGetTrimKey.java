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

public class SMGetTrimKey implements Comparable<SMGetTrimKey> {
  private String key;
  private BKeyObject bKeyObject;

  public SMGetTrimKey(String key, byte[] bkey) {
    this.key = key;
    this.bKeyObject = new BKeyObject(bkey);
  }

  public SMGetTrimKey(String key, long bkey) {
    this.key = key;
    this.bKeyObject = new BKeyObject(bkey);
  }

  public SMGetTrimKey(String key, BKeyObject bkeyObject) {
    this.key = key;
    this.bKeyObject = bkeyObject;
  }

  @Override
  public String toString() {
    return "SMGetElement {KEY:" + key + ", BKEY:" + bKeyObject + "}";
  }

  @Override
  public int compareTo(SMGetTrimKey param) {
    assert param != null;

    /* compare bkey */
    int comp = bKeyObject.compareTo(param.bKeyObject);

    /* if bkey is equal, then compare key */
    if (comp == 0) {
      comp = key.compareTo(param.getKey());
    }

    return comp;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SMGetTrimKey)) {
      return false;
    }

    SMGetTrimKey that = (SMGetTrimKey) o;
    return Objects.equals(key, that.key) &&
            Objects.equals(bKeyObject, that.bKeyObject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, bKeyObject);
  }

}
