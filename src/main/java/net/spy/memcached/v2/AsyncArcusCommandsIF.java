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
package net.spy.memcached.v2;

import java.util.List;
import java.util.Map;

public interface AsyncArcusCommandsIF<T> {

  /**
   * Set a value for the given key.
   *
   * @param key   the key
   * @param exp   expiration time in seconds
   * @param value the value to store
   * @return {@code Boolean.True} if stored, otherwise {@code Boolean.False}
   */
  ArcusFuture<Boolean> set(String key, int exp, T value);

  /**
   * Add a value for the given key if it does not exist.
   *
   * @param key   the key
   * @param exp   expiration time in seconds
   * @param value the value to store
   * @return {@code Boolean.True} if stored, otherwise {@code Boolean.False}
   */
  ArcusFuture<Boolean> add(String key, int exp, T value);

  /**
   * Replace a value for the given key if it exists.
   *
   * @param key   the key
   * @param exp   expiration time in seconds
   * @param value the value to store
   * @return {@code Boolean.True} if stored, otherwise {@code Boolean.False}
   */
  ArcusFuture<Boolean> replace(String key, int exp, T value);

  /**
   * Set values for multiple keys.
   *
   * @param keys  list of keys to store
   * @param exp   expiration time in seconds
   * @param value the value to store for all keys
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiSet(List<String> keys, int exp, T value);

  /**
   * Add values for multiple keys if they do not exist.
   *
   * @param keys  list of keys to store
   * @param exp   expiration time in seconds
   * @param value the value to store for all keys
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiAdd(List<String> keys, int exp, T value);

  /**
   * Replace values for multiple keys if they exist.
   *
   * @param keys  list of keys to store
   * @param exp   expiration time in seconds
   * @param value the value to store for all keys
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiReplace(List<String> keys, int exp, T value);

  /**
   * Get a value for the given key.
   *
   * @param key the key
   * @return the value, or {@code null} if not found
   */
  ArcusFuture<T> get(String key);

  /**
   * Get values for multiple keys.
   *
   * @param keys list of keys to get
   * @return Map of key to value
   */
  ArcusFuture<Map<String, T>> multiGet(List<String> keys);

  /**
   * Flush all items from all servers.
   *
   * @param delay delay in seconds before flushing
   * @return {@code Boolean.True} if flushed successfully, otherwise {@code Boolean.False}
   */
  ArcusFuture<Boolean> flush(int delay);

}
