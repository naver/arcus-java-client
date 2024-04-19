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
package net.spy.memcached.util;

import net.spy.memcached.CachedData;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.Element;
import net.spy.memcached.transcoders.Transcoder;

public final class BTreeUtil {

  private static final String HEXES = "0123456789ABCDEF";
  private static final int MAX_BKEY_BYTE_ARRAY_SIZE = 31;

  private BTreeUtil() {
  }

  public static String toHex(byte[] byteArray) {
    if (byteArray == null) {
      return null;
    }

    final StringBuilder hex = new StringBuilder(2 * byteArray.length + 2);

    hex.append("0x");

    for (final byte b : byteArray) {
      hex.append(HEXES.charAt((b & 0xF0) >> 4));
      hex.append(HEXES.charAt((b & 0x0F)));
    }

    return hex.toString();
  }

  public static byte[] hexStringToByteArrays(String str) {
    if (str == null) {
      return null;
    }

    if (str.startsWith("0x")) {
      str = str.substring(2);
    }

    if (str.length() == 0) {
      return new byte[0];
    }

    if (str.length() % 2 != 0) {
      throw new IllegalArgumentException("Invalid hex string.");
    }

    int len = str.length();
    byte[] data = new byte[len / 2];

    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(str.charAt(i), 16) << 4) + Character
              .digit(str.charAt(i + 1), 16));
    }

    return data;
  }

  public static int compareByteArraysInLexOrder(byte[] array1, byte[] array2) {
    int diff;
    for (int i = 0; i < array1.length && i < array2.length; i++) {
      diff = (array1[i] & 0xFF) - (array2[i] & 0xFF);
      if (diff != 0) {
        return diff;
      }
    }
    return array1.length - array2.length;
  }

  public static void validateBkey(byte[] ...bkeys) {
    for (byte[] bkey : bkeys) {
      if (bkey == null) {
        throw new IllegalArgumentException("bkey is null");
      }
      if (bkey.length > MAX_BKEY_BYTE_ARRAY_SIZE) {
        throw new IllegalArgumentException("bkey size exceeded 31");
      }
    }
  }

  public static void validateBkey(long ...bkeys) {
    for (long bkey : bkeys) {
      if (bkey < 0) {
        throw new IllegalArgumentException(
                String.format("not supported unsigned long bkey : %s, use byte array bkey", bkey));
      }
    }
  }

  public static <T> Element<T> makeBTreeElement(BKeyObject bkey,
                                                CachedData cachedData,
                                                Transcoder<T> tc) {
    Element<T> element = null;
    T value = tc.decode(cachedData);

    switch (bkey.getType()) {
      case LONG:
        element = new Element<T>(bkey.getLongBKey(), value, cachedData.getEFlag());
        break;
      case BYTEARRAY:
        element = new Element<T>(bkey.getByteArrayBKeyRaw(), value, cachedData.getEFlag());
        break;
    }
    return element;
  }
}
