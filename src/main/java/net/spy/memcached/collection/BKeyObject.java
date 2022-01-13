/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
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

import net.spy.memcached.util.BTreeUtil;

public class BKeyObject {

  public enum BKeyType {
    LONG, BYTEARRAY, UNKNOWN
  }

  private BKeyType type = BKeyType.UNKNOWN;
  private Long longBKey;
  private ByteArrayBKey byteArrayBKey;

  public BKeyObject() {

  }

  public BKeyObject(long longBKey) {
    setLongBKey(longBKey);
  }

  public BKeyObject(byte[] byteArrayBKey) {
    setByteArrayBKey(new ByteArrayBKey(byteArrayBKey));
  }

  public BKeyObject(ByteArrayBKey byteArrayBKey) {
    setByteArrayBKey(byteArrayBKey);
  }

  public BKeyObject(String bkeyString) {
    byte[] b = BTreeUtil.hexStringToByteArrays(bkeyString);
    ByteArrayBKey byteArrayBKey = new ByteArrayBKey(b);
    setByteArrayBKey(byteArrayBKey);
  }

  public BKeyType getType() {
    return type;
  }

  public Long getLongBKey() {
    if (BKeyType.LONG == type) {
      return longBKey;
    } else {
      return null;
    }
  }

  public void setLongBKey(long longBKey) {
    this.type = BKeyType.LONG;
    this.longBKey = longBKey;
    this.byteArrayBKey = null;
  }

  public ByteArrayBKey getByteArrayBKey() {
    if (BKeyType.BYTEARRAY == type) {
      return byteArrayBKey;
    } else {
      return null;
    }
  }

  public byte[] getByteArrayBKeyRaw() {
    if (BKeyType.BYTEARRAY == type) {
      return byteArrayBKey.getBytes();
    } else {
      return null;
    }
  }

  public void setByteArrayBKey(ByteArrayBKey byteArrayBKey) {
    this.type = BKeyType.BYTEARRAY;
    this.byteArrayBKey = byteArrayBKey;
    this.longBKey = null;
  }

  public String getBKeyAsString() {
    if (BKeyType.LONG == type) {
      return BTreeUtil.toULong(longBKey);
    } else if (BKeyType.BYTEARRAY == type) {
      return BTreeUtil.toHex(byteArrayBKey.getBytes());
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    return getBKeyAsString();
  }

}
