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

import net.spy.memcached.util.BTreeUtil;

public class ByteArrayBKey implements Comparable<ByteArrayBKey> {

  public static final byte[] MIN = new byte[]{(byte) 0};

  public static final byte[] MAX = new byte[]{(byte) 255, (byte) 255,
      (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
      (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
      (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
      (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
      (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
      (byte) 255, (byte) 255, (byte) 255, (byte) 255
  };

  private final byte[] bkey;

  public ByteArrayBKey(byte[] bkey) {
    BTreeUtil.validateBkey(bkey);
    this.bkey = bkey;
  }

  public byte[] getBytes() {
    return bkey;
  }

  @Override
  public int compareTo(ByteArrayBKey o) {
    return BTreeUtil.compareByteArraysInLexOrder(bkey, o.getBytes());
  }

  @Override
  public String toString() {
    return Arrays.toString(bkey);
  }
}
