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

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;
import net.spy.memcached.v2.vo.BTreeElements;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.SMGetElements;

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

  /**
   * Create a btree item.
   *
   * @param key        key to create
   * @param type       btree element value type
   * @param attributes collection attributes (must not be null)
   * @return {@code Boolean.True} if created, otherwise {@code Boolean.False}
   */
  ArcusFuture<Boolean> bopCreate(String key, ElementValueType type,
                                 CollectionAttributes attributes);

  /**
   * Insert an element into a btree item.
   *
   * @param key        key to insert
   * @param element    btree element to insert
   * @param attributes collection attributes for creation when the btree does not exist
   * @return {@code Boolean.True} if inserted, {@code Boolean.False} if element exists,
   * {@code null} if key is not found
   */
  ArcusFuture<Boolean> bopInsert(String key, BTreeElement<T> element,
                                 CollectionAttributes attributes);

  /**
   * Insert an element into a btree item.
   *
   * @param key     key to insert
   * @param element btree element to insert
   * @return {@code Boolean.True} if inserted, {@code Boolean.False} if element exists,
   * {@code null} if key is not found
   */
  ArcusFuture<Boolean> bopInsert(String key, BTreeElement<T> element);

  /**
   * Upsert an element into a btree item.
   *
   * @param key        key to upsert
   * @param element    btree element to upsert
   * @param attributes collection attributes for creation when the btree does not exist
   * @return {@code Boolean.True} if upserted, {@code Boolean.False} otherwise
   */
  ArcusFuture<Boolean> bopUpsert(String key, BTreeElement<T> element,
                                 CollectionAttributes attributes);

  /**
   * Upsert an element into a btree item.
   *
   * @param key     key to upsert
   * @param element btree element to upsert
   * @return {@code Boolean.True} if upserted, {@code Boolean.False} otherwise
   */
  ArcusFuture<Boolean> bopUpsert(String key, BTreeElement<T> element);

  /**
   * Insert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key        key to insert
   * @param element    btree element to insert
   * @param attributes collection attributes for creation when the btree does not exist
   * @return {@code Map.Entry} with insertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopInsertAndGetTrimmed(
          String key, BTreeElement<T> element, CollectionAttributes attributes);

  /**
   * Insert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key     key to insert
   * @param element btree element to insert
   * @return {@code Map.Entry} with insertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopInsertAndGetTrimmed(String key,
                                                                          BTreeElement<T> element);

  /**
   * Upsert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key        key to upsert
   * @param element    btree element to upsert
   * @param attributes collection attributes for creation when the btree does not exist
   * @return {@code Map.Entry} with upsertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopUpsertAndGetTrimmed(
          String key, BTreeElement<T> element, CollectionAttributes attributes);

  /**
   * Upsert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key     key to upsert
   * @param element btree element to upsert
   * @return {@code Map.Entry} with upsertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopUpsertAndGetTrimmed(String key,
                                                                          BTreeElement<T> element);

  /**
   * Get an element from a btree item.
   *
   * @param key  key to get
   * @param bKey BKey of the element to get
   * @param args arguments for get operation
   * @return {@code BTreeElement} if found, {@code BTreeElement} with null value and eFlag
   * if element is not found but key exists, {@code null} if key is not found
   */
  ArcusFuture<BTreeElement<T>> bopGet(String key, BKey bKey, BopGetArgs args);

  /**
   * Get elements from a btree item.
   *
   * @param key  key to get
   * @param from BKey range start
   * @param to   BKey range end
   * @param args arguments for get operation
   * @return {@code BTreeElements} that contains trimmed or not and elements.
   * If element is not found but key exists, {@code BTreeElements} with empty map will be returned.
   * If key is not found, {@code null} will be returned.
   */
  ArcusFuture<BTreeElements<T>> bopGet(String key, BKey from, BKey to, BopGetArgs args);

  /**
   * Get elements from multiple btree items.
   *
   * @param keys list of keys to get
   * @param from BKey range start
   * @param to   BKey range end
   * @param args arguments for get operation
   * @return Map of key to BTreeElements. If element is not found but key exists,
   * empty {@code BTreeElements} will be set for entry value. If key is not found,
   * the corresponding entry will not be present in the map.
   */
  ArcusFuture<Map<String, BTreeElements<T>>> bopMultiGet(List<String> keys,
                                                         BKey from, BKey to,
                                                         BopGetArgs args);

  /**
   * Get sort-merged elements from multiple btree items.
   *
   * @param keys   list of keys to get
   * @param from   BKey range start
   * @param to     BKey range end
   * @param unique whether to return unique elements only
   * @param args   arguments for get operation
   * @return {@code SMGetElements} containing sort-merged elements. Never return {@code null}.
   * If matching elements not exist, the elements list in the {@code SMGetElements} will be empty.
   */
  ArcusFuture<SMGetElements<T>> bopSortMergeGet(List<String> keys, BKey from, BKey to,
                                                boolean unique, BopGetArgs args);
}
