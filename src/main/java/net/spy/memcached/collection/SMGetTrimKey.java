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

public class SMGetTrimKey implements Comparable<SMGetTrimKey> {
  private String key;
  private long bkey;
  private byte[] bytebkey;

  public SMGetTrimKey(String key, Object bkey) {
    this.key = key;
    if (bkey instanceof byte[]) {
      this.bytebkey = new ByteArrayBKey((byte[]) bkey).getBytes();
      this.bkey = -1;
    } else {
      this.bkey = (Long) bkey;
      this.bytebkey = null;
    }

  }

  @Override
  public String toString() {
    return "SMGetElement {KEY:" + key + ", BKEY:"
            + ((bytebkey == null) ? BTreeUtil.toULong(bkey) : BTreeUtil.toHex(bytebkey)) + "}";
  }

  @Override
  public int compareTo(SMGetTrimKey param) {
    assert param != null;

    int comp;
        /* compare bkey */
    if (bytebkey == null) {
      comp = Long.valueOf(bkey).compareTo(param.getBkey());
    } else {
      comp = BTreeUtil.compareByteArraysInLexOrder(bytebkey, param.getByteBkey());
    }

    /* if bkey is equal, then compare key */
    if (comp == 0) {
      comp = key.compareTo(param.getKey());
    }

    return comp;
  }

  public int compareBkeyTo(SMGetTrimKey param) {
    assert param != null;

    /* compare bkey */
    if (bytebkey == null) {
      return Long.valueOf(bkey).compareTo(param.getBkey());
    } else {
      return BTreeUtil.compareByteArraysInLexOrder(bytebkey, param.getByteBkey());
    }
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public long getBkey() {
    if (bkey == -1) {
      throw new IllegalStateException("This element has byte[] bkey. " + toString());
    }
    return bkey;
  }

  public byte[] getByteBkey() {
    if (bytebkey == null) {
      throw new IllegalStateException(
              "This element has java.lang.Long type bkey. " + toString());
    }
    return bytebkey;
  }

  public Object getBkeyByObject() {
    if (bytebkey != null) {
      return bytebkey;
    } else {
      return bkey;
    }
  }

  public void setBkey(long bkey) {
    this.bkey = bkey;
  }

  public boolean hasByteArrayBkey() {
    return bytebkey != null;
  }
}
