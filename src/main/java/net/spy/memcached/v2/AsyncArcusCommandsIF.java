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
import java.util.Set;

import net.spy.memcached.CASValue;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;
import net.spy.memcached.v2.vo.BTreeElements;
import net.spy.memcached.v2.vo.BTreeUpdateElement;
import net.spy.memcached.v2.vo.BopDeleteArgs;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.GetArgs;
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
   * Perform a compare-and-set operation for the given key.
   *
   * @param key   the key to set
   * @param exp   expiration time in seconds
   * @param value the new value to set if the CAS ID matches
   * @param casId the CAS ID obtained from {@link #gets(String)}
   * @return {@code Boolean.True} if compared and set successfully,
   * {@code Boolean.False} if the key does not exist or CAS ID does not match
   */
  ArcusFuture<Boolean> cas(String key, int exp, T value, long casId);

  /**
   * Append String or byte[] to an existing same type of value.
   *
   * @param key the key
   * @param val the value to append
   * @return {@code Boolean.True} if appended, otherwise {@code Boolean.False}
   */
  ArcusFuture<Boolean> append(String key, T val);

  /**
   * Prepend String or byte[] to an existing same type of value.
   *
   * @param key the key
   * @param val the value to prepend
   * @return {@code Boolean.True} if prepended, otherwise {@code Boolean.False}
   */
  ArcusFuture<Boolean> prepend(String key, T val);

  /**
   * Sets multiple key-value pairs.
   *
   * @param items map of keys and values to store
   * @param exp   expiration time in seconds
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiSet(Map<String, T> items, int exp);

  /**
   * Add multiple key-value pairs if they do not exist.
   *
   * @param items map of keys and values to store
   * @param exp   expiration time in seconds
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiAdd(Map<String, T> items, int exp);

  /**
   * Replace multiple key-value pairs if they exist.
   *
   * @param items map of keys and values to store
   * @param exp   expiration time in seconds
   * @return Map of key to Boolean result
   */
  ArcusFuture<Map<String, Boolean>> multiReplace(Map<String, T> items, int exp);

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
   * @return CASValue containing the value and CAS ID, or {@code null} if not found
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
  ArcusFuture<Long> incr(String key, int delta, long initial, int exp);

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
  ArcusFuture<Long> decr(String key, int delta, long initial, int exp);

  /**
   * Get values with CAS for multiple keys.
   *
   * @param keys list of keys to get
   * @return Map of key to value with CAS.
   */
  ArcusFuture<Map<String, CASValue<T>>> multiGets(List<String> keys);

  /**
   * Delete a value for the given key.
   *
   * @param key the key
   * @return {@code Boolean.True} if deleted, otherwise {@code Boolean.False}
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
   * Update an element in a btree item
   *
   * @param key     key to update
   * @param element btree element to update.
   * @return {@code Boolean.True} if updated, {@code Boolean.False} if element does not exist,
   * {@code null} if key is not found
   */
  ArcusFuture<Boolean> bopUpdate(String key, BTreeUpdateElement<T> element);

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

  /**
   * Increments a numeric value of an element with the given bKey in a btree item by {@code delta}
   *
   * @param key   key of the btree item
   * @param bKey  BKey of the element to increment
   * @param delta the amount to increment (&gt; 0)
   * @return the new value after increment, or {@code null} if the key or element is not found.
   */
  ArcusFuture<Long> bopIncr(String key, BKey bKey, int delta);

  /**
   * Increments a numeric value of an element with the given bKey in a btree item by {@code delta}.
   * If the element does not exist, it is created with {@code initial} value and {@code eFlag}.
   *
   * @param key     key of the btree item
   * @param bKey    BKey of the element to increment
   * @param delta   the amount to increment (&gt; 0)
   * @param initial the value to store if the element does not exist
   *                ({@code delta} is ignored) (&ge; 0)
   * @param eFlag   eFlag of the element to create, or {@code null} if not needed
   * @return the new value after increment,
   * or {@code initial} if the element did not exist.
   */
  ArcusFuture<Long> bopIncr(String key, BKey bKey, int delta, long initial, byte[] eFlag);

  /**
   * Decrements a numeric value of an element with the given bKey in a btree item by {@code delta}.
   * <p>If the value is decremented below 0, it will be set to 0.</p>
   *
   * @param key   key of the btree item
   * @param bKey  BKey of the element to decrement
   * @param delta the amount to decrement (&gt; 0)
   * @return the new value after decrement, or {@code null} if the key or element is not found.
   */
  ArcusFuture<Long> bopDecr(String key, BKey bKey, int delta);

  /**
   * Decrements a numeric value of an element with the given bKey in a btree item by {@code delta}.
   * If the element does not exist, it is created with {@code initial} value and {@code eFlag}.
   * <p>If the value is decremented below 0, it will be set to 0.</p>
   *
   * @param key     key of the btree item
   * @param bKey    BKey of the element to decrement
   * @param delta   the amount to decrement (&gt; 0)
   * @param initial the value to store if the element does not exist
   *                ({@code delta} is ignored) (&ge; 0)
   * @param eFlag   eFlag of the element to create, or {@code null} if not needed
   * @return the new value after decrement,
   * or {@code initial} if the element did not exist.
   */
  ArcusFuture<Long> bopDecr(String key, BKey bKey, int delta, long initial, byte[] eFlag);

  /**
   * Delete an element with the given bKey from a btree item.
   *
   * @param key  key of the btree item
   * @param bKey BKey of the element to delete
   * @param args delete arguments (eFlagFilter, dropIfEmpty)
   * @return {@code true} if the element was deleted,
   * {@code null} if the key is not found,
   * {@code false} if the element is not found.
   */
  ArcusFuture<Boolean> bopDelete(String key, BKey bKey, BopDeleteArgs args);

  /**
   * Delete elements in a bKey range from a btree item.
   * Elements are deleted in order from {@code from} to {@code to}.
   * <p>If {@code args.count} is 0 (default), all elements in the range are deleted. </p>
   * Otherwise, only the first {@code args.count} elements (in {@code from}-to-{@code to} order)
   * are deleted.
   *
   * @param key  key of the btree item
   * @param from BKey range start (inclusive)
   * @param to   BKey range end (inclusive)
   * @param args delete arguments (count, eFlagFilter, dropIfEmpty)
   * @return {@code true} if at least one element was deleted,
   * {@code null} if the key is not found,
   * {@code false} if no elements are found in the range.
   */
  ArcusFuture<Boolean> bopDelete(String key, BKey from, BKey to, BopDeleteArgs args);

  /**
   * Count elements in a bKey range from a btree item.
   *
   * @param key         key of the btree item
   * @param from        BKey range start (inclusive)
   * @param to          BKey range end (inclusive)
   * @param eFlagFilter eFlag filter condition, or {@code null} to count all elements in the range
   * @return the number of elements in the range (0 if none exist),
   * or {@code null} if the key is not found.
   */
  ArcusFuture<Long> bopCount(String key, BKey from, BKey to, ElementFlagFilter eFlagFilter);

  /**
   * Create a list with the given attributes.
   *
   * @param key        key of the list to create
   * @param type       element value type
   * @param attributes initial attributes of the list
   * @return {@code true} if created, {@code false} if the key already exists.
   */
  ArcusFuture<Boolean> lopCreate(String key, ElementValueType type,
                                 CollectionAttributes attributes);

  /**
   * Insert an element at the given index into a list.
   *
   * @param key   key of the list
   * @param index index at which to insert the element
   * @param value the value to insert
   * @return {@code true} if the element was inserted, {@code null} if the key is not found.
   */
  ArcusFuture<Boolean> lopInsert(String key, int index, T value);

  /**
   * Insert an element at the given index into a list.
   * If the list does not exist, it is created with the given attributes.
   *
   * @param key        key of the list
   * @param index      index at which to insert the element
   * @param value      the value to insert
   * @param attributes attributes to use when creating the list, or {@code null} to not create
   * @return {@code true} if the element was inserted, {@code null} if the key is not found.
   */
  ArcusFuture<Boolean> lopInsert(String key, int index, T value, CollectionAttributes attributes);

  /**
   * Get an element at the given index from a list.
   *
   * @param key   key of the list
   * @param index index of the element to get
   * @param args  arguments for get operation
   * @return the element value, {@code null} if the key or element is not found.
   */
  ArcusFuture<T> lopGet(String key, int index, GetArgs args);

  /**
   * Get elements in an index range from a list.
   *
   * @param key  key of the list
   * @param from index range start (inclusive)
   * @param to   index range end (inclusive)
   * @param args arguments for get operation
   * @return list of element values in order, an empty list if no elements are found in the range,
   * {@code null} if the key is not found.
   */
  ArcusFuture<List<T>> lopGet(String key, int from, int to, GetArgs args);

  /**
   * Delete an element at the given index from a list.
   *
   * @param key         key of the list
   * @param index       index of the element to delete
   * @param dropIfEmpty whether to delete the list if it becomes empty after deletion
   * @return {@code true} if the element was deleted, {@code null} if the key is not found,
   * {@code false} if the element is not found.
   */
  ArcusFuture<Boolean> lopDelete(String key, int index, boolean dropIfEmpty);

  /**
   * Delete elements in an index range from a list.
   *
   * @param key         key of the list
   * @param from        index range start (inclusive)
   * @param to          index range end (inclusive)
   * @param dropIfEmpty whether to delete the list if it becomes empty after deletion
   * @return {@code true} if at least one element was deleted, {@code null} if the key is not found,
   * {@code false} if no elements are found in the range.
   */
  ArcusFuture<Boolean> lopDelete(String key, int from, int to, boolean dropIfEmpty);

  /**
   * Create a set with the given attributes.
   *
   * @param key        key of the set to create
   * @param type       element value type
   * @param attributes initial attributes of the set
   * @return {@code true} if created, {@code false} if the key already exists.
   */
  ArcusFuture<Boolean> sopCreate(String key, ElementValueType type,
                                 CollectionAttributes attributes);

  /**
   * Insert an element into a set.
   *
   * @param key   key of the set
   * @param value the value to insert
   * @return {@code true} if the element was inserted, {@code false} if the element already exists,
   * {@code null} if the key is not found.
   */
  ArcusFuture<Boolean> sopInsert(String key, T value);

  /**
   * Insert an element into a set.
   * If the set does not exist, it is created with the given attributes.
   *
   * @param key        key of the set
   * @param value      the value to insert
   * @param attributes attributes to use when creating the set, or {@code null} to not create
   * @return {@code true} if the element was inserted, {@code false} if the element already exists,
   * {@code null} if the key is not found.
   */
  ArcusFuture<Boolean> sopInsert(String key, T value, CollectionAttributes attributes);

  /**
   * Check whether an element exists in a set.
   *
   * @param key   key of the set
   * @param value the value to check
   * @return {@code true} if the element exists, {@code false} if the element is not found,
   * {@code null} if the key is not found.
   */
  ArcusFuture<Boolean> sopExist(String key, T value);

  /**
   * Get elements randomly from a set.
   *
   * @param key   key of the set
   * @param count number of elements to retrieve randomly (0 means all elements, max 1000)
   * @param args  arguments for get operation
   * @return set of element values, an empty set if no elements are found,
   * {@code null} if the key is not found.
   */
  ArcusFuture<Set<T>> sopGet(String key, int count, GetArgs args);

  /**
   * Delete an element from a set.
   *
   * @param key         key of the set
   * @param value       the value to delete
   * @param dropIfEmpty whether to delete the set if it becomes empty after deletion
   * @return {@code true} if the element was deleted, {@code false} if the element is not found,
   * {@code null} if the key is not found.
   */
  ArcusFuture<Boolean> sopDelete(String key, T value, boolean dropIfEmpty);
}
