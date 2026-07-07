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

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.spy.memcached.CASValue;
import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.CreateAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;
import net.spy.memcached.v2.vo.BTreeGetResult;
import net.spy.memcached.v2.vo.BTreePositionElement;
import net.spy.memcached.v2.vo.BTreeSMGetResult;
import net.spy.memcached.v2.vo.BTreeUpdateElement;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.BopMultiGetArgs;
import net.spy.memcached.v2.vo.BopRangeGetArgs;
import net.spy.memcached.v2.vo.BopSMGetArgs;
import net.spy.memcached.v2.vo.GetMode;

public interface AsyncArcusCommandsIF<T> {

  /**
   * Set a value for the given key.
   *
   * @param key   the key
   * @param exp   expiration time in seconds
   * @param value the value to store
   * @return {@code true} if stored, otherwise {@code false}
   */
  ArcusFuture<Boolean> set(String key, long exp, T value);

  /**
   * Add a value for the given key if it does not exist.
   *
   * @param key   the key
   * @param exp   expiration time in seconds
   * @param value the value to store
   * @return {@code true} if stored, otherwise {@code false}
   */
  ArcusFuture<Boolean> add(String key, long exp, T value);

  /**
   * Replace a value for the given key if it exists.
   *
   * @param key   the key
   * @param exp   expiration time in seconds
   * @param value the value to store
   * @return {@code true} if stored, otherwise {@code false}
   */
  ArcusFuture<Boolean> replace(String key, long exp, T value);

  /**
   * Sets multiple key-value pairs.
   *
   * @param items map of keys and values to store
   * @param exp   expiration time in seconds
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiSet(Map<String, T> items, long exp);

  /**
   * Add multiple key-value pairs if they do not exist.
   *
   * @param items map of keys and values to store
   * @param exp   expiration time in seconds
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiAdd(Map<String, T> items, long exp);

  /**
   * Replace multiple key-value pairs if they exist.
   *
   * @param items map of keys and values to store
   * @param exp   expiration time in seconds
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiReplace(Map<String, T> items, long exp);

  /**
   * Prepend String or byte[] to an existing same type of value.
   *
   * @param key   the key
   * @param value the value to prepend
   * @return {@code true} if prepended, otherwise {@code false}
   */
  ArcusFuture<Boolean> prepend(String key, T value);

  /**
   * Append String or byte[] to an existing same type of value.
   *
   * @param key   the key
   * @param value the value to append
   * @return {@code true} if appended, otherwise {@code false}
   */
  ArcusFuture<Boolean> append(String key, T value);

  /**
   * Perform a compare-and-set operation for the given key.
   *
   * @param key   the key to set
   * @param exp   expiration time in seconds
   * @param value the new value to set if the CAS ID matches
   * @param casId the CAS ID obtained from {@link #gets(String)}
   * @return {@code true} if compared and set successfully,
   * {@code false} if the key does not exist or CAS ID does not match
   */
  ArcusFuture<Boolean> cas(String key, long exp, T value, long casId);

  /**
   * Increments a numeric value stored at the given key by {@code delta}.
   *
   * @param key   the key
   * @param delta the amount to increment (&gt; 0)
   * @return the new value after increment, or -1 if the key is not found
   */
  ArcusFuture<Long> incr(String key, int delta);

  /**
   * Increments a numeric value stored at the given key by {@code delta}.
   * If the key does not exist,
   * it is created with the value of {@code initial} and expiration time of {@code exp}.
   *
   * @param key     the key
   * @param delta   the amount to increment (&gt; 0)
   * @param initial the value to store if the key does not exist ({@code delta} is ignored) (&ge; 0)
   * @param exp     expiration time in seconds, applied only when a new key is created
   * @return the new value after increment, or {@code initial} if the key did not exist
   */
  ArcusFuture<Long> incr(String key, int delta, long initial, long exp);

  /**
   * Decrements a numeric value stored at the given key by {@code delta}.
   * <p>If the value is decremented below 0, it will be set to 0.</p>
   *
   * @param key   the key
   * @param delta the amount to decrement (&gt; 0)
   * @return the new value after decrement, or -1 if the key is not found
   */
  ArcusFuture<Long> decr(String key, int delta);

  /**
   * Decrements a numeric value stored at the given key by {@code delta}.
   * If the key does not exist,
   * it is created with the value of {@code initial} and expiration time of {@code exp}.
   * <p>If the value is decremented below 0, it will be set to 0.</p>
   *
   * @param key     the key
   * @param delta   the amount to decrement (&gt; 0)
   * @param initial the value to store if the key does not exist ({@code delta} is ignored) (&ge; 0)
   * @param exp     expiration time in seconds, applied only when a new key is created
   * @return the new value after decrement, or {@code initial} if the key did not exist
   */
  ArcusFuture<Long> decr(String key, int delta, long initial, long exp);

  /**
   * Get a value for the given key.
   *
   * @param key the key
   * @return the value, or {@code null} if not found
   */
  ArcusFuture<T> get(String key);

  /**
   * Get a value and its CAS ID for the given key.
   *
   * @param key the key
   * @return {@link CASValue}, {@code null} if not found
   */
  ArcusFuture<CASValue<T>> gets(String key);

  /**
   * Get values for multiple keys.
   *
   * @param keys list of keys to get
   * @return Map of key to value
   */
  ArcusFuture<Map<String, T>> multiGet(List<String> keys);


  /**
   * Get values with CAS for multiple keys.
   *
   * @param keys list of keys to get
   * @return Map of key to {@link CASValue}
   */
  ArcusFuture<Map<String, CASValue<T>>> multiGets(List<String> keys);

  /**
   * Delete a value for the given key.
   *
   * @param key the key
   * @return {@code true} if deleted, otherwise {@code false}
   */
  ArcusFuture<Boolean> delete(String key);


  /**
   * Delete values for multiple keys.
   *
   * @param keys list of keys to delete
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiDelete(List<String> keys);

  /**
   * Create a list with the given attributes.
   *
   * @param key        key of the list to create
   * @param type       element value type
   * @param attributes initial attributes of the list (must not be null)
   * @return {@code true} if created, {@code false} if the key already exists
   */
  ArcusFuture<Boolean> lopCreate(String key, ElementValueType type,
                                 CreateAttributes attributes);

  /**
   * Insert an element at the given index into a list.
   * Negative indexes are counted backward from the end of the list.
   *
   * @param key   key of the list
   * @param index index at which to insert the element
   * @param value the value to insert
   * @return {@code true} if the element was inserted, {@code null} if the key is not found
   */
  ArcusFuture<Boolean> lopInsert(String key, int index, T value);

  /**
   * Insert an element at the given index into a list.
   * Negative indexes are counted backward from the end of the list.
   * If the list does not exist, it is created with the given attributes.
   *
   * @param key        key of the list
   * @param index      index at which to insert the element
   * @param value      the value to insert
   * @param attributes attributes to use when creating the list, or {@code null} to not create
   * @return {@code true} if the element was inserted, {@code null} if the key is not found
   */
  ArcusFuture<Boolean> lopInsert(String key, int index, T value, CreateAttributes attributes);

  /**
   * Get an element at the given index from a list.
   *
   * @param key   key of the list
   * @param index index of the element to get
   * @param mode  get mode - {@link net.spy.memcached.v2.vo.GetMode}
   * @return the element value, {@code null} if the key or element is not found
   */
  ArcusFuture<T> lopGet(String key, int index, GetMode mode);

  /**
   * Get elements in an index range from a list.
   *
   * @param key  key of the list
   * @param from index range start (inclusive)
   * @param to   index range end (inclusive)
   * @param mode get mode - {@link net.spy.memcached.v2.vo.GetMode}
   * @return list of element values in order, an empty list if no elements are found in the range,
   * {@code null} if the key is not found
   */
  ArcusFuture<List<T>> lopGet(String key, int from, int to, GetMode mode);

  /**
   * Delete an element at the given index from a list.
   * Negative indexes are counted backward from the end of the list.
   *
   * @param key         key of the list
   * @param index       index of the element to delete
   * @param dropIfEmpty whether to delete the list if it becomes empty after deletion
   * @return {@code true} if the element was deleted,
   * {@code false} if the element is not found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> lopDelete(String key, int index, boolean dropIfEmpty);

  /**
   * Delete elements in an index range from a list.
   * Negative indexes are counted backward from the end of the list.
   *
   * @param key         key of the list
   * @param from        index range start (inclusive)
   * @param to          index range end (inclusive)
   * @param dropIfEmpty whether to delete the list if it becomes empty after deletion
   * @return {@code true} if at least one element was deleted,
   * {@code false} if no elements are found in the range,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> lopDelete(String key, int from, int to, boolean dropIfEmpty);

  /**
   * Create a set with the given attributes.
   *
   * @param key        key of the set to create
   * @param type       element value type
   * @param attributes initial attributes of the set (must not be null)
   * @return {@code true} if created, {@code false} if the key already exists
   */
  ArcusFuture<Boolean> sopCreate(String key, ElementValueType type,
                                 CreateAttributes attributes);

  /**
   * Insert an element into a set.
   *
   * @param key   key of the set
   * @param value the value to insert
   * @return {@code true} if the element was inserted,
   * {@code false} if the element already exists,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> sopInsert(String key, T value);

  /**
   * Insert an element into a set.
   * If the set does not exist, it is created with the given attributes.
   *
   * @param key        key of the set
   * @param value      the value to insert
   * @param attributes attributes to use when creating the set, or {@code null} to not create
   * @return {@code true} if the element was inserted,
   * {@code false} if the element already exists,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> sopInsert(String key, T value, CreateAttributes attributes);

  /**
   * Get elements randomly from a set.
   *
   * @param key   key of the set
   * @param count number of elements to retrieve randomly (0 means all elements, max 1000)
   * @param mode  get mode - {@link net.spy.memcached.v2.vo.GetMode}
   * @return set of element values, an empty set if no elements are found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Set<T>> sopGet(String key, int count, GetMode mode);

  /**
   * Check whether an element exists in a set.
   *
   * @param key   key of the set
   * @param value the value to check
   * @return {@code true} if the element exists,
   * {@code false} if the element is not found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> sopExist(String key, T value);

  /**
   * Delete an element from a set.
   *
   * @param key         key of the set
   * @param value       the value to delete
   * @param dropIfEmpty whether to delete the set if it becomes empty after deletion
   * @return {@code true} if the element was deleted,
   * {@code false} if the element is not found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> sopDelete(String key, T value, boolean dropIfEmpty);

  /**
   * Create an empty map with the given attributes.
   *
   * @param key        key of the map to create
   * @param type       element value type
   * @param attributes initial attributes of the map (must not be null)
   * @return {@code true} if created, {@code false} if the key already exists
   */
  ArcusFuture<Boolean> mopCreate(String key, ElementValueType type,
                                 CreateAttributes attributes);

  /**
   * Insert an element with the given mKey into a map.
   *
   * @param key   key of the map
   * @param mKey  mKey of the element to insert
   * @param value the value to insert
   * @return {@code true} if the element was inserted,
   * {@code false} if the mKey already exists,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopInsert(String key, String mKey, T value);

  /**
   * Insert an element with the given mKey into a map.
   * If the map does not exist, it is created with the given attributes.
   *
   * @param key        key of the map
   * @param mKey       mKey of the element to insert
   * @param value      the value to insert
   * @param attributes attributes to use when creating the map, or {@code null} to not create
   * @return {@code true} if the element was inserted,
   * {@code false} if the mKey already exists,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopInsert(String key, String mKey, T value,
                                 CreateAttributes attributes);

  /**
   * Upsert an element with the given mKey in a map.
   * If an element with the given mKey exists, it is replaced, otherwise a new element is inserted.
   *
   * @param key   key of the map
   * @param mKey  mKey of the element to upsert
   * @param value the value to insert or replace with
   * @return {@code true} if upserted, {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopUpsert(String key, String mKey, T value);

  /**
   * Upsert an element with the given mKey in a map.
   * If an element with the given mKey exists, it is replaced, otherwise a new element is inserted.
   * If the map does not exist, it is created with the given attributes.
   *
   * @param key        key of the map
   * @param mKey       mKey of the element to upsert
   * @param value      the value to insert or replace with
   * @param attributes attributes to use when creating the map, or {@code null} to not create
   * @return {@code true} if upserted, {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopUpsert(String key, String mKey, T value,
                                 CreateAttributes attributes);

  /**
   * Update the value of an element with the given mKey in a map.
   *
   * @param key   key of the map
   * @param mKey  mKey of the element to update
   * @param value the new value
   * @return {@code true} if the element was updated,
   * {@code false} if the mKey is not found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopUpdate(String key, String mKey, T value);

  /**
   * Get all elements from a map.
   *
   * @param key  key of the map
   * @param mode get mode - {@link net.spy.memcached.v2.vo.GetMode}
   * @return mKey to value,
   * empty map if no elements exist,
   * {@code null} if the key is not found
   */
  ArcusFuture<Map<String, T>> mopGet(String key, GetMode mode);

  /**
   * Get an element with the given mKey from a map.
   *
   * @param key  key of the map
   * @param mKey mKey of the element to get
   * @param mode get mode - {@link net.spy.memcached.v2.vo.GetMode}
   * @return the element value,
   * {@code null} if the key or mKey is not found
   */
  ArcusFuture<T> mopGet(String key, String mKey, GetMode mode);

  /**
   * Get elements with the mKeys from a map.
   *
   * @param key   key of the map
   * @param mKeys list of mKeys to get
   * @param mode  get mode - {@link net.spy.memcached.v2.vo.GetMode}
   * @return map of mKey to value for found elements,
   * empty map if no mKeys are found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Map<String, T>> mopGet(String key, List<String> mKeys, GetMode mode);

  /**
   * Delete all elements from a map.
   *
   * @param key         key of the map
   * @param dropIfEmpty whether to drop the map if it becomes empty after deletion
   * @return {@code true} if at least one element was deleted,
   * {@code false} if no elements exist,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopDelete(String key, boolean dropIfEmpty);

  /**
   * Delete an element with the given mKey from a map.
   *
   * @param key         key of the map
   * @param mKey        mKey of the element to delete
   * @param dropIfEmpty whether to drop the map if it becomes empty after deletion
   * @return {@code true} if the element was deleted,
   * {@code false} if the mKey is not found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopDelete(String key, String mKey, boolean dropIfEmpty);

  /**
   * Delete elements with the given mKeys from a map.
   *
   * @param key         key of the map
   * @param mKeys       mKeys of the elements to delete
   * @param dropIfEmpty whether to drop the map if it becomes empty after deletion
   * @return {@code true} if at least one element was deleted,
   * {@code false} if no mKeys are found,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> mopDelete(String key, List<String> mKeys, boolean dropIfEmpty);

  /**
   * Create a btree item.
   *
   * @param key        key of the btree to create
   * @param type       element value type
   * @param attributes initial attributes of the btree (must not be null)
   * @return {@code true} if created, otherwise {@code false}
   */
  ArcusFuture<Boolean> bopCreate(String key, ElementValueType type,
                                 CreateAttributes attributes);

  /**
   * Insert an element into a btree item.
   *
   * @param key     key of the btree
   * @param element btree element to insert
   * @return {@code true} if inserted,
   * {@code false} if element exists,
   * {@code null} if key is not found
   */
  ArcusFuture<Boolean> bopInsert(String key, BTreeElement<T> element);

  /**
   * Insert an element into a btree item.
   *
   * @param key        key of the btree
   * @param element    btree element to insert
   * @param attributes attributes to use when creating the btree, or {@code null} to not create
   * @return {@code true} if inserted,
   * {@code false} if element exists,
   * {@code null} if key is not found
   */
  ArcusFuture<Boolean> bopInsert(String key, BTreeElement<T> element,
                                 CreateAttributes attributes);

  /**
   * Insert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key     key of the btree
   * @param element btree element to insert
   * @return {@code Map.Entry} with insertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopInsertAndGetTrimmed(
      String key, BTreeElement<T> element);

  /**
   * Insert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key        key of the btree
   * @param element    btree element to insert
   * @param attributes attributes to use when creating the btree, or {@code null} to not create
   * @return {@code Map.Entry} with insertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopInsertAndGetTrimmed(
      String key, BTreeElement<T> element, CreateAttributes attributes);

  /**
   * Upsert an element into a btree item.
   *
   * @param key     key of the btree
   * @param element btree element to upsert
   * @return {@code true} if upserted, {@code null} if the key is not found
   */
  ArcusFuture<Boolean> bopUpsert(String key, BTreeElement<T> element);

  /**
   * Upsert an element into a btree item.
   *
   * @param key        key of the btree
   * @param element    btree element to upsert
   * @param attributes attributes to use when creating the btree, or {@code null} to not create
   * @return {@code true} if upserted, {@code null} if the key is not found
   */
  ArcusFuture<Boolean> bopUpsert(String key, BTreeElement<T> element,
                                 CreateAttributes attributes);

  /**
   * Upsert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key     key of the btree
   * @param element btree element to upsert
   * @return {@code Map.Entry} with upsertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopUpsertAndGetTrimmed(
      String key, BTreeElement<T> element);

  /**
   * Upsert an element into a btree item and get trimmed element if overflow trim occurs.
   *
   * @param key        key of the btree
   * @param element    btree element to upsert
   * @param attributes attributes to use when creating the btree, or {@code null} to not create
   * @return {@code Map.Entry} with upsertion result and trimmed element
   */
  ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopUpsertAndGetTrimmed(
      String key, BTreeElement<T> element, CreateAttributes attributes);

  /**
   * Update an element in a btree item
   *
   * @param key     key of the btree
   * @param element btree element to update
   * @return {@code true} if updated,
   * {@code false} if element does not exist,
   * {@code null} if key is not found
   */
  ArcusFuture<Boolean> bopUpdate(String key, BTreeUpdateElement<T> element);

  /**
   * Increments a numeric value of an element with the given bKey in a btree item by {@code delta}
   *
   * @param key   key of the btree
   * @param bKey  bKey of the element to increment
   * @param delta the amount to increment (&gt; 0)
   * @return the new value after increment, or {@code null} if the key or element is not found
   */
  ArcusFuture<Long> bopIncr(String key, BKey bKey, int delta);

  /**
   * Increments a numeric value of an element with the given bKey in a btree item by {@code delta}.
   * If the element does not exist, it is created with {@code initial} value and {@code eFlag}.
   *
   * @param key     key of the btree
   * @param bKey    bKey of the element to increment
   * @param delta   the amount to increment (&gt; 0)
   * @param initial the value to store if the element does not exist
   *                ({@code delta} is ignored) (&ge; 0)
   * @param eFlag   eFlag of the element to create, or {@code null} if not needed
   * @return the new value after increment, or {@code initial} if the element did not exist
   */
  ArcusFuture<Long> bopIncr(String key, BKey bKey, int delta, long initial, byte[] eFlag);

  /**
   * Decrements a numeric value of an element with the given bKey in a btree item by {@code delta}.
   * <p>If the value is decremented below 0, it will be set to 0.</p>
   *
   * @param key   key of the btree
   * @param bKey  bKey of the element to decrement
   * @param delta the amount to decrement (&gt; 0)
   * @return the new value after decrement, or {@code null} if the key or element is not found
   */
  ArcusFuture<Long> bopDecr(String key, BKey bKey, int delta);

  /**
   * Decrements a numeric value of an element with the given bKey in a btree item by {@code delta}.
   * If the element does not exist, it is created with {@code initial} value and {@code eFlag}.
   * <p>If the value is decremented below 0, it will be set to 0.</p>
   *
   * @param key     key of the btree
   * @param bKey    bKey of the element to decrement
   * @param delta   the amount to decrement (&gt; 0)
   * @param initial the value to store if the element does not exist
   *                ({@code delta} is ignored) (&ge; 0)
   * @param eFlag   eFlag of the element to create, or {@code null} if not needed
   * @return the new value after decrement, or {@code initial} if the element did not exist
   */
  ArcusFuture<Long> bopDecr(String key, BKey bKey, int delta, long initial, byte[] eFlag);

  /**
   * Get an element with the given bKey from a btree.
   *
   * @param key  key of the btree
   * @param bKey bKey of the element to get
   * @param args arguments for get operation
   * @return the {@code BTreeElement} if found,
   * {@code null} if the key or element is not found
   */
  ArcusFuture<BTreeElement<T>> bopGet(String key, BKey bKey, BopGetArgs args);

  /**
   * Get elements in a bKey range from a btree.
   *
   * @param key  key of the btree
   * @param from bKey range start
   * @param to   bKey range end
   * @param args arguments for get operation
   * @return {@code BTreeGetResult} with found elements,
   * empty {@code BTreeGetResult} if no elements are found in the range but key exists,
   * {@code null} if key is not found
   */
  ArcusFuture<BTreeGetResult<T>> bopGet(String key, BKey from, BKey to, BopRangeGetArgs args);

  /**
   * Get elements in a bKey range from multiple btrees.
   *
   * @param keys list of btree keys
   * @param from bKey range start
   * @param to   bKey range end
   * @param args arguments for get operation
   * @return map of requested key to {@code BTreeGetResult} for existing keys,
   * empty {@code BTreeGetResult} if no elements are found in the range but the key exists,
   * no mapping for a requested key if the key is not found
   */
  ArcusFuture<Map<String, BTreeGetResult<T>>> bopMultiGet(
      List<String> keys, BKey from, BKey to, BopMultiGetArgs args);

  /**
   * Get sort-merged elements in a bKey range from multiple btrees.
   *
   * @param keys list of btree keys
   * @param from bKey range start
   * @param to   bKey range end
   * @param args arguments for get operation {@link BopSMGetArgs}
   * @return {@code BTreeSMGetResult} containing sort-merged elements,
   * empty {@code BTreeSMGetResult} if no matching elements exist
   */
  ArcusFuture<BTreeSMGetResult<T>> bopSortMergeGet(
      List<String> keys, BKey from, BKey to, BopSMGetArgs args);

  /**
   * Get the position of an element with the given bKey from a btree.
   *
   * @param key   key of the btree
   * @param bKey  bKey of the element to find
   * @param order the order of the btree to determine position
   * @return the 0-based position of the element,
   * {@code null} if the key or element is not found
   */
  ArcusFuture<Integer> bopGetPosition(String key, BKey bKey, BTreeOrder order);

  /**
   * Get an element at the given position from a btree.
   *
   * @param key   key of the btree
   * @param pos   0-based position of the element to get
   * @param order the order of the btree to determine position
   * @return the {@code BTreeElement} at the given position,
   * {@code null} if the key or element is not found
   */
  ArcusFuture<BTreeElement<T>> bopGetByPosition(
      String key, int pos, BTreeOrder order);

  /**
   * Get elements in a position range from a btree.
   *
   * @param key   key of the btree
   * @param from  start position (inclusive)
   * @param to    end position (inclusive); must be greater than or equal to {@code from}
   * @param order the order of the btree to determine position
   * @return list of {@code BTreeElement} in the given position range, in traversal order,
   * empty list if no elements exist in the range,
   * {@code null} if the key is not found
   */
  ArcusFuture<List<BTreeElement<T>>> bopGetByPosition(
      String key, int from, int to, BTreeOrder order);

  /**
   * Get an element by bKey and its neighboring elements with position information.
   *
   * @param key   key of the btree
   * @param bKey  bKey of the element to find
   * @param count the number of neighboring elements to retrieve on each side
   *              (0 &le; count &le; 100)
   * @param order the order of the btree to determine position
   * @return list of {@code BTreePositionElement} in traversal order,
   * empty list if the element is not found,
   * {@code null} if the key is not found
   */
  ArcusFuture<List<BTreePositionElement<T>>> bopPositionWithGet(
      String key, BKey bKey, int count, BTreeOrder order);

  /**
   * Count elements in a bKey range from a btree.
   *
   * @param key         key of the btree
   * @param from        bKey range start (inclusive)
   * @param to          bKey range end (inclusive)
   * @param eFlagFilter eFlag filter condition, or {@code null} to count all elements in the range
   * @return the number of elements in the range (0 if none exist),
   * {@code null} if the key is not found
   */
  ArcusFuture<Long> bopCount(String key, BKey from, BKey to, ElementFlagFilter eFlagFilter);

  /**
   * Delete an element with the given bKey from a btree.
   *
   * @param key         key of the btree
   * @param bKey        bKey of the element to delete
   * @param eFlagFilter eFlag filter condition, or {@code ElementFlagFilter.DO_NOT_FILTER}
   *                    to delete the element without checking eFlag
   * @param dropIfEmpty whether to delete the btree item if it becomes empty after deletion
   * @return {@code true} if the element was deleted,
   * {@code false} if the element is not found or eFlag does not match,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> bopDelete(String key, BKey bKey,
                                 ElementFlagFilter eFlagFilter, boolean dropIfEmpty);

  /**
   * Delete elements in a bKey range from a btree item.
   * Elements are deleted in order from {@code from} to {@code to}.
   * If {@code count} is 0, all matching elements are deleted; otherwise,
   * up to {@code count} matching elements are deleted.
   *
   * @param key         key of the btree
   * @param from        bKey range start (inclusive)
   * @param to          bKey range end (inclusive)
   * @param eFlagFilter eFlag filter condition, or {@code ElementFlagFilter.DO_NOT_FILTER}
   *                    to delete the element without checking eFlag
   * @param count       maximum number of matching elements to delete; {@code 0} means no limit
   * @param dropIfEmpty whether to delete the btree item if it becomes empty after deletion
   * @return {@code true} if at least one element was deleted,
   * {@code false} if no elements are found in the range,
   * {@code null} if the key is not found
   */
  ArcusFuture<Boolean> bopDelete(String key, BKey from, BKey to,
                                 ElementFlagFilter eFlagFilter, int count,
                                 boolean dropIfEmpty);

  /**
   * Flush all items from all servers immediately.
   *
   * @return always {@code true}
   */
  ArcusFuture<Boolean> flush();

  /**
   * Flush all items from all servers after a given delay.
   *
   * @param delay delay in seconds before flushing. (&ge; -1)
   * @return always {@code true}
   */
  ArcusFuture<Boolean> flush(int delay);

  /**
   * Flush all items with the given prefix from all servers immediately.
   *
   * @param prefix the prefix of the items to flush. Use {@code ""} for items with no prefix.
   * @return {@code true} if flushed successfully,
   * {@code false} if no items with the given prefix exist
   */
  ArcusFuture<Boolean> flush(String prefix);

  /**
   * Flush all items with the given prefix from all servers after a given delay.
   *
   * @param prefix the prefix of the items to flush. Use {@code ""} for items with no prefix.
   * @param delay  delay in seconds before flushing. (&ge; -1)
   * @return {@code true} if flushed successfully,
   * {@code false} if no items with the given prefix exist
   */
  ArcusFuture<Boolean> flush(String prefix, int delay);

  /**
   * Get statistics from all connected servers.
   *
   * @return a map of each server's {@link java.net.SocketAddress} to its stats key-value pairs
   */
  ArcusFuture<Map<SocketAddress, Map<String, String>>> stats();

  /**
   * Get a specific set of statistics from all connected servers.
   *
   * @param arg the stats argument ({@link net.spy.memcached.v2.StatsArg})
   * @return a map of each server's {@link java.net.SocketAddress} to its stats key-value pairs
   */
  ArcusFuture<Map<SocketAddress, Map<String, String>>> stats(StatsArg arg);

  /**
   * Get the version string from all connected servers.
   *
   * @return a map of each server's {@link java.net.SocketAddress} to its version string
   */
  ArcusFuture<Map<SocketAddress, String>> versions();
}
