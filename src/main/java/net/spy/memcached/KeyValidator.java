/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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
package net.spy.memcached;

import java.util.Collection;
import java.util.HashSet;

/**
 * Validator for memcached keys.
 */
public final class KeyValidator {

  private static final int MAX_KEY_LENGTH = 4000;
  private static final int MAX_MKEY_LENGTH = 250;
  private static final int MAX_BKEY_BYTE_ARRAY_LENGTH = 31;

  private final byte delimiter;

  public KeyValidator(byte delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * Validate cache key.
   *
   * @param key the cache key to validate
   * @throws IllegalArgumentException if the key is invalid
   */
  public void validateKey(String key) {
    boolean hasPrefix = false;

    byte[] keyBytes = KeyUtil.getKeyBytes(key);
    if (keyBytes.length > MAX_KEY_LENGTH) {
      throw new IllegalArgumentException("Key is too long (maxlen = "
          + MAX_KEY_LENGTH + ")");
    } else if (keyBytes.length == 0) {
      throw new IllegalArgumentException(
          "Key must contain at least one character.");
    }
    // Validate the key
    for (byte b : keyBytes) {
      if (b == ' ' || b == '\n' || b == '\r' || b == 0) {
        throw new IllegalArgumentException(
            "Key contains invalid characters:  ``" + key + "''");
      }
      if (b == delimiter) {
        hasPrefix = true;
      }
    }

    // Validate the prefix
    if (hasPrefix) {
      if (keyBytes[0] == '-') {
        throw new IllegalArgumentException(
            "Key contains invalid prefix: ``" + key + "''");
      }
      for (byte b : keyBytes) {
        if (b == delimiter) {
          break;
        }
        if (!(('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') ||
            ('0' <= b && b <= '9') ||
            (b == '_') || (b == '-') || (b == '+') || (b == '.') ||
            (b == '{') || (b == '}'))) {
          throw new IllegalArgumentException(
              "Key contains invalid prefix: ``" + key + "''");
        }
      }
    }
  }

  /**
   * Validate cache keys.
   *
   * @param keyList the cache keys to validate
   * @throws IllegalArgumentException if the key list is null, empty, or contains invalid keys
   */
  public void validateKey(Collection<String> keyList) {
    if (keyList == null) {
      throw new IllegalArgumentException("Key list is null.");
    } else if (keyList.isEmpty()) {
      throw new IllegalArgumentException("Key list is empty.");
    }

    for (String key : keyList) {
      validateKey(key);
    }
  }

  /**
   * Validate map key.
   *
   * @param mkey the mkey to validate
   * @throws IllegalArgumentException if the mkey is invalid
   */
  public void validateMKey(String mkey) {
    if (mkey == null) {
      throw new IllegalArgumentException("mkey is null");
    }

    byte[] keyBytes = KeyUtil.getKeyBytes(mkey);
    if (keyBytes.length > MAX_MKEY_LENGTH) {
      throw new IllegalArgumentException("MKey is too long (maxlen = "
          + MAX_MKEY_LENGTH + ")");
    }
    if (keyBytes.length == 0) {
      throw new IllegalArgumentException("MKey must contain at least one character.");
    }
    // Validate the mkey
    for (byte b : keyBytes) {
      if (b == ' ' || b == '\n' || b == '\r' || b == 0) {
        throw new IllegalArgumentException("MKey contains invalid characters:  ``"
            + mkey + "''");
      }
    }
  }

  /**
   * Validate map keys.
   *
   * @param mkeyList the mkeys to validate
   * @throws IllegalArgumentException if the mkey list is null, empty, or contains invalid mkeys
   */
  public void validateMKey(Collection<String> mkeyList) {
    if (mkeyList == null) {
      throw new IllegalArgumentException("mkeyList is null.");
    } else if (mkeyList.isEmpty()) {
      throw new IllegalArgumentException("mkeyList is empty.");
    }

    for (String mkey : mkeyList) {
      validateMKey(mkey);
    }
  }

  /**
   * Validate byte type bkeys.
   *
   * @param bkeys the bkeys to validate
   * @throws IllegalArgumentException if the bkey is invalid
   */
  public static void validateBKey(byte[]... bkeys) {
    for (byte[] bkey : bkeys) {
      if (bkey == null) {
        throw new IllegalArgumentException("bkey is null");
      }
      if (bkey.length > MAX_BKEY_BYTE_ARRAY_LENGTH) {
        throw new IllegalArgumentException("bkey size exceeded 31");
      }
    }
  }

  /**
   * Validate long type bkeys.
   *
   * @param bkeys the bkeys to validate
   * @throws IllegalArgumentException if the bkey is invalid
   */
  public static void validateBKey(long... bkeys) {
    for (long bkey : bkeys) {
      if (bkey < 0) {
        throw new IllegalArgumentException(
            String.format("not supported unsigned long bkey : %s, use byte array bkey", bkey));
      }
    }
  }

  /**
   * Check duplicate keys in keyList.
   */
  public void checkDupKey(Collection<String> keyList) {
    HashSet<String> keySet = new HashSet<>();
    for (String key : keyList) {
      if (!keySet.add(key)) {
        throw new IllegalArgumentException("Duplicate keys exist in key list.");
      }
    }
  }
}
