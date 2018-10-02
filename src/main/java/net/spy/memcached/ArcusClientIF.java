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
package net.spy.memcached;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import net.spy.memcached.collection.Attributes;
import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.BTreeStoreAndGetFuture;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.CollectionGetBulkFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Interface for Arcus specific commands
 */
public interface ArcusClientIF {

  /**
   * Sets attributes (metadata) associated with each key
   * of collections including lists, sets, maps, and B+ trees.
   *
   * @param key   key of a collection (list, set, map, B+ tree)
   * @param attrs a collectionAttribute object to set
   * @return whether or not the operation was performed
   */
  public abstract CollectionFuture<Boolean> asyncSetAttr(String key,
                                                         Attributes attrs);

  /**
   * Gets attributes (metadata) associated with each key
   * of collections including lists, sets, maps, and B+ trees.
   *
   * @param key key of a collection (list, set, map, B+ tree)
   * @return a CollectionAttributes object containing attributes
   */
  public abstract CollectionFuture<CollectionAttributes> asyncGetAttr(
          final String key);


  /**
   * Checks an item membership in a set.
   *
   * @param <T>
   * @param key   key of a set
   * @param value value of an item
   * @param tc    a transcoder to encode the value
   * @return whether or not the item exists in the set
   */
  public abstract <T> CollectionFuture<Boolean> asyncSopExist(String key,
                                                              T value, Transcoder<T> tc);

  /**
   * Checks an item membership in a set using the default transcoder.
   *
   * @param key   key of a set
   * @param value value of an item
   * @return whether or not the item exists in the set
   */
  public abstract CollectionFuture<Boolean> asyncSopExist(String key,
                                                          Object value);

  /**
   * Set an object in the cache on each key.
   *
   * <h2>Basic usage</h2>
   * <pre>
   * 	ArcusClient c = getClientFromPool();
   *
   * 	List&lt;String&gt; keys = new ArrayList();
   * 	keys.add("KEY1");
   * 	keys.add("KEY2");
   *
   * 	// The object to store
   * 	Object value = "VALUE";
   *
   * 	// Get customized transcoder
   * 	Transcoder myTranscoder = getTranscoder();
   *
   * 	// Store a value (async) on each keys for one hour using multiple memcached client.
   * 	c.setBulk(keys, 3600, value, transcoder);
   * 	</pre>
   *
   * @param <T>
   * @param key the key list which this object should be added
   * @param exp the expiration of this object
   * @param o   the object to store on each keys
   * @param tc  the transcoder to serialize and unserialize the value
   * @return a future that will hold the list of failed
   */
  public abstract <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          List<String> key, int exp, T o, Transcoder<T> tc);

  /**
   * Set an object in the cache on each key using specified memcached client
   *
   * @param key the key list which this object should be added
   * @param exp the expiration of this object
   * @param o   the object to store on each keys
   * @return a future that will hold the list of failed
   */
  public abstract Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          List<String> key, int exp, Object o);

  /**
   * Set an object in the cache on each key using specified memcached client
   *
   * @param o   the map that has keys and values to store
   * @param exp the expiration of this object
   * @param tc  the transcoder to serialize and unserialize the value
   * @return a future that will hold the list of failed
   */
  public abstract <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          Map<String, T> o, int exp, Transcoder<T> tc);

  /**
   * Set an object in the cache on each key using specified memcached client
   *
   * @param o   the map that has keys and values to store
   * @param exp the expiration of this object
   * @return a future that will hold the list of failed
   */
  public abstract Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          Map<String, Object> o, int exp);

  /**
   * Insert one item into multiple b+trees at once.
   *
   * @param keyList             key list of b+tree
   * @param bkey                key of a b+tree element.
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if
   *                            this value is null, Arcus don't assign element flag.
   * @param value               value of element. this value can't be null.
   * @param attributesForCreate create a b+tree with this attributes, if given key is not
   *                            exists.
   * @param tc                  transcoder to encode value
   * @return a future indicating success
   */
  public abstract <T> Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, T value, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Insert one item into multiple b+trees at once.
   *
   * @param keyList             key list of b+tree
   * @param bkey                key of a b+tree element.
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if
   *                            this value is null, Arcus don't assign element flag.
   * @param value               value of element. this value can't be null.
   * @param attributesForCreate create a b+tree with this attributes, if given key is not
   *                            exists.
   * @return a future indicating success
   */
  public abstract Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate);

  /**
   * Insert one item into multiple map at once.
   *
   * @param keyList             key list of map
   * @param mkey                mkey of map.
   * @param value               value of map
   * @param attributesForCreate create a map with this attributes, if given key is not
   *                            exists.
   * @param tc                  transcoder to encode value
   * @return a future indicating success
   */
  public abstract <T> Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, T value, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Insert one item into multiple map at once.
   *
   * @param keyList             key list of map
   * @param mkey                mkey of map.
   * @param value               value of map
   * @param attributesForCreate create a map with this attributes, if given key is not
   *                            exists.
   * @return a future indicating success
   */
  public abstract Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, Object value, CollectionAttributes attributesForCreate);

  /**
   * Insert a value into each list
   *
   * <pre>
   * Note to the index
   *     The item will be inserted before the element with the given index except below
   *     -1:append, 0:prepend
   * </pre>
   *
   * @param <T>
   * @param keyList             a key list of list
   * @param index               list index (the item will be inserted before the element with the given index)
   * @param value               a value to insert into each list
   * @param attributesForCreate if not true, a list should be created when key does not exist
   * @param tc                  transcoder to encode value
   * @return a future that will indicate the failure list of each operation
   */
  public abstract <T> Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, T value, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Insert a value into each list
   * The value will be encoded by the default transcoder (SerializeTranscoder)
   *
   * <pre>
   * Note to the index
   *     The item will be inserted before the element with the given index except below
   *     -1:append, 0:prepend
   * </pre>
   *
   * <h2>Basic usage</h2>
   *
   * <pre>
   * 	ArcusClient client = getClientFromPool();
   *
   * 	List&lt;String&gt; keyList = getKeyListShouldHaveValue();
   * 	String value = "Some-value";
   * 	int index = 0;
   * 	boolean createKeyIfNotExists = true;
   *
   * 	Future&lt;Map&lt;String, CollectionOperationStatus&gt;&gt; future = client.asyncLopInsertBulk(keyList,
   * 			index, value, createKeyIfNotExists);
   *
   * 	Map&lt;String, CollectionOperationStatus&gt; failedList = null;
   * 	try {
   * 		failedList = future.get(1000L, TimeUnit.MILLISECONDS);
   *    } catch (TimeoutException e) {
   * 		future.cancel(true);
   * 		// Handle error here
   *    } catch (InterruptedException e) {
   * 		future.cancel(true);
   * 		// Handle error here
   *    } catch (ExecutionException e) {
   * 		future.cancel(true);
   * 		// Handle error here
   *    }
   * 	handleFailure(failedList);
   * </pre>
   *
   * @param keyList             a key list of the list
   * @param index               list index (the item will be inserted before the element with the given index)
   * @param value               a value to insert into each list
   * @param attributesForCreate if not null, a list should be created when key does not exist
   * @return a future that will indicate the failure list of each operation
   */
  public abstract Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, Object value, CollectionAttributes attributesForCreate);

  /**
   * Insert a value into each set
   *
   * @param <T>
   * @param keyList             a key list of the set
   * @param value               a value to insert into each set
   * @param attributesForCreate if not null, a list should be created when key does not exist
   * @param tc                  transcoder to encode value
   * @return a future that will indicate the failure list of each operation
   */
  public abstract <T> Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, T value, CollectionAttributes attributesForCreate, Transcoder<T> tc);

  /**
   * Insert a value into each set
   *
   * <h2>Basic usage</h2>
   *
   * <pre>
   * 	ArcusClient client = getClientFromPool();
   *
   * 	List&lt;String&gt; keyList = getKeyListShouldHaveValue();
   * 	String value = "Some-value";
   * 	boolean createKeyIfNotExists = true;
   *
   * 	Future&lt;Map&lt;String, CollectionOperationStatus&gt;&gt; future = client.asyncSopInsertBulk(keyList,
   * 			value, createKeyIfNotExists);
   *
   * 	Map&lt;String, CollectionOperationStatus&gt; failedList = null;
   * 	try {
   * 		failedList = future.get(1000L, TimeUnit.MILLISECONDS);
   *    } catch (TimeoutException e) {
   * 		future.cancel(true);
   * 		// Handle error here
   *    } catch (InterruptedException e) {
   * 		future.cancel(true);
   * 		// Handle error here
   *    } catch (ExecutionException e) {
   * 		future.cancel(true);
   * 		// Handle error here
   *    }
   * 	handleFailure(failedList);
   * </pre>
   *
   * @param keyList             a key list of set
   * @param value               a value to insert into each set
   * @param attributesForCreate if not null, a list should be created when key does not exist
   * @return a future that will indicate the failure list of each operation
   */
  public abstract Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, Object value, CollectionAttributes attributesForCreate);

  /**
   * Get maximum possible piped bulk insert item count.
   *
   * @return Get maximum possible piped bulk insert item count.
   */
  public abstract int getMaxPipedItemCount();

  /**
   * Create an empty b+ tree
   *
   * @param key        key of a b+ tree
   * @param valueType  element data type of the b+ tree
   * @param attributes attributes of the b+ tree
   * @return a future indicating success, false if there was a key
   */
  public CollectionFuture<Boolean> asyncBopCreate(String key,
                                                  ElementValueType valueType, CollectionAttributes attributes);

  /**
   * Create an empty map
   *
   * @param key        key of a map
   * @param type       element data type of the map
   * @param attributes attributes of the map
   * @return a future indicating success, false if there was a key
   */
  public CollectionFuture<Boolean> asyncMopCreate(String key,
                                                  ElementValueType type, CollectionAttributes attributes);

  /**
   * Create an empty set
   *
   * @param key        key of a set
   * @param type       element data type of the set
   * @param attributes attributes of the set
   * @return a future indicating success, false if there was a key
   */
  public CollectionFuture<Boolean> asyncSopCreate(String key,
                                                  ElementValueType type, CollectionAttributes attributes);

  /**
   * Create an empty list
   *
   * @param key        key of a list
   * @param type       element data type of the list
   * @param attributes attributes of the list
   * @return a future indicating success, false if there was a key
   */
  public CollectionFuture<Boolean> asyncLopCreate(String key,
                                                  ElementValueType type, CollectionAttributes attributes);

  /**
   * Retrieves an item on given bkey in the b+tree.
   *
   * @param key         key of a b+tree
   * @param bkey        bkey
   * @param eFlagFilter element flag filter
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+
   *                    tree will remain empty even if all the elements are removed
   * @return a future that will hold the return value map of the fetch.
   */
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long bkey, ElementFlagFilter eFlagFilter, boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves count number of items in given bkey range(from..to)
   * from offset in the b+tree.
   * The returned map from the future should be sorted by the given range.
   * <pre>
   * 	from >= to : in descending order
   * 	from < to  : in ascending order
   * </pre>
   *
   * @param key         key of a b+tree
   * @param from        the first bkey
   * @param to          the last bkey
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset
   * @param count       number of returning values (0 to all)
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+ tree will remain empty even if all the elements are removed
   * @return a future that will hold the return value map of the fetch
   */
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long from, long to, ElementFlagFilter eFlagFilter, int offset, int count,
                                                                  boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves an item on given bkey in the b+tree.
   *
   * @param <T>
   * @param key         key of a b+tree
   * @param bkey        bkey
   * @param eFlagFilter element flag filter
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+ tree will remain empty even if all the elements are removed
   * @param tc          a transcoder to decode returned values
   * @return a future that will hold the return value map of the fetch.
   */
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long bkey, ElementFlagFilter eFlagFilter, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Retrieves count number of items in given bkey range(from..to)
   * from offset in the b+tree.
   * The returned map from the future should be sorted by the given range.
   * <pre>
   * 	from >= to : in descending order
   * 	from < to  : in ascending order
   * </pre>
   *
   * @param <T>
   * @param key         key of a b+tree
   * @param from        the first bkey
   * @param to          the last bkey
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset
   * @param count       number of returning values (0 to all)
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+ tree will remain empty even if all the elements are removed
   * @param tc          a transcoder to decode returned values
   * @return a future that will hold the return value map of the fetch
   */
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long from, long to, ElementFlagFilter eFlagFilter, int offset, int count,
                                                                 boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Retrieves all items from the map
   *
   * @param key         key of a map
   * @param withDelete  true to remove the returned item in the map
   * @param dropIfEmpty true to remove the key when all elements are removed. false map will remain empty even if all the elements are removed
   * @return a future that will hold the return value map of the fetch
   */
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves an item on given mkey in the map.
   *
   * @param key         key of a map
   * @param mkey        mkey of a map
   * @param withDelete  true to remove the returned item in the map
   * @param dropIfEmpty true to remove the key when all elements are removed. false map will remain empty even if all the elements are removed
   * @return a future that will hold the return value map of the fetch
   */
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           String mkey, boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves items on given mkey list in the map.
   *
   * @param key         key of a map
   * @param mkeyList    mkeyList
   * @param withDelete  true to remove the returned item in the map
   * @param dropIfEmpty true to remove the key when all elements are removed. false map will remain empty even if all the elements are removed
   * @return a future that will hold the return value map of the fetch.
   */
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           List<String> mkeyList, boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves all items from the map
   *
   * @param <T>
   * @param key         key of a map
   * @param withDelete  true to remove the returned item in the map
   * @param dropIfEmpty true to remove the key when all elements are removed. false map will remain empty even if all the elements are removed
   * @param tc          a transcoder to decode returned values
   * @return a future that will hold the return value map of the fetch
   */
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Retrieves an item on given mkey in the map.
   *
   * @param <T>
   * @param key         key of a map
   * @param mkey        mkey of a map
   * @param withDelete  true to remove the returned item in the map
   * @param dropIfEmpty true to remove the key when all elements are removed. false map will remain empty even if all the elements are removed
   * @param tc          a transcoder to decode returned values
   * @return a future that will hold the return value map of the fetch
   */
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          String mkey, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Retrieves items on given mkey list in the map.
   *
   * @param <T>
   * @param key         key of a map
   * @param mkeyList    mkeyList
   * @param withDelete  true to remove the returned item in the map
   * @param dropIfEmpty true to remove the key when all elements are removed. false map will remain empty even if all the elements are removed
   * @param tc          a transcoder to decode returned values
   * @return a future that will hold the return value map of the fetch.
   */
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          List<String> mkeyList, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  public CollectionFuture<List<Object>> asyncRangeGet(String frkey, String tokey,
                                                      int count);

  /**
   * Retrieves an item on given index in the list.
   *
   * @param key         key of a list
   * @param index       list index
   * @param withDelete  true to remove the returned item in the list
   * @param dropIfEmpty true to remove the key when all elements are removed. false list will remain empty even if all the elements are removed
   * @return a future that will hold the return value list of the fetch
   */
  public CollectionFuture<List<Object>> asyncLopGet(String key, int index,
                                                    boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves items on given index range(from..to) in the list.
   *
   * @param key         key of a list
   * @param from        the first index to delete
   * @param to          the last index to delete
   * @param withDelete  true to remove the returned items in the list
   * @param dropIfEmpty true to remove the key when all elements are removed. false list will remain empty even if all the elements are removed
   * @return a future that will hold the return value list of the fetch
   */
  public CollectionFuture<List<Object>> asyncLopGet(String key, int from,
                                                    int to, boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves an item on given index in the list.
   *
   * @param <T>
   * @param key         key of a list
   * @param index       list index
   * @param withDelete  true to remove the returned item in the list
   * @param dropIfEmpty true to remove the key when all elements are removed. false list will remain empty even if all the elements are removed
   * @param tc          a tranacoder to decode returned value
   * @return a future that will hold the return value list of the fetch
   */
  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int index,
                                                   boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Retrieves items on given index range(from..to) in the list. (Arcus 1.6 and above)
   *
   * @param <T>
   * @param key         key of a list
   * @param from        the first index to delete
   * @param to          the last index to delete
   * @param withDelete  true to remove the returned items in the list
   * @param dropIfEmpty true to remove the key when all elements are removed. false list will remain empty even if all the elements are removed
   * @param tc          a transcoder to decode the returned values
   * @return a future that will hold the return value list of the fetch
   */
  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int from,
                                                   int to, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Retrieves count number of random items in the set.
   *
   * @param key         key of a set
   * @param count       number of items to fetch
   * @param withDelete  true to remove the returned item in the set
   * @param dropIfEmpty true to remove the key when all elements are removed. false set will remain empty even if all the elements are removed
   * @return a future that will hold the return value set of the fetch
   */
  public CollectionFuture<Set<Object>> asyncSopGet(String key, int count,
                                                   boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves count number of random items in the set.
   *
   * @param <T>
   * @param key         key of a set
   * @param count       number of items to fetch
   * @param withDelete  true to remove the returned item in the set
   * @param dropIfEmpty true to remove the key when all elements are removed. false set will remain empty even if all the elements are removed
   * @param tc          a tranacoder to decode returned value
   * @return a future that will hold the return value set of the fetch
   */
  public <T> CollectionFuture<Set<T>> asyncSopGet(String key, int count,
                                                  boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Deletes an item with given bkey in the b+tree. (Arcus 1.6 or above)
   *
   * @param key         key of a b+tree
   * @param bkey        bkey of an item to delete
   * @param eFlagFilter element flag filter
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+ tree will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncBopDelete(String key, long bkey,
                                                  ElementFlagFilter eFlagFilter, boolean dropIfEmpty);

  /**
   * Deletes count number of items in given bkey range(from..to) in the b+tree (Arcus 1.6 or above)
   *
   * @param key         key of a b+tree
   * @param from        the first bkey to delete
   * @param to          the last bkey to delete
   * @param eFlagFilter element flag filter
   * @param count       number of returning values (0 to all)
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+ tree will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncBopDelete(String key, long from,
                                                  long to, ElementFlagFilter eFlagFilter, int count, boolean dropIfEmpty);

  /**
   * Deletes count number of items in given bkey range(from..to) in the b+tree (Arcus 1.6 or above)
   *
   * @param key         key of a b+tree
   * @param from        the first bkey to delete
   * @param to          the last bkey to delete
   * @param eFlagFilter element flag filter
   * @param count       number of returning values (0 to all)
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+ tree will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncBopDelete(String key,
                                                  byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int count,
                                                  boolean dropIfEmpty);

  /**
   * Deletes count number of items in given bkey range(from..to) in the b+tree (Arcus 1.6 or above)
   *
   * @param key         key of a b+tree
   * @param bkey        bkey to delete
   * @param eFlagFilter element flag filter
   * @param dropIfEmpty true to remove the key when all elements are removed. false b+ tree will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncBopDelete(String key,
                                                  byte[] bkey, ElementFlagFilter eFlagFilter, boolean dropIfEmpty);

  /**
   * Deletes an item on given index in the map.
   *
   * @param key         key of a map
   * @param dropIfEmpty false to remove the key when all elements are removed. true b+ tree will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncMopDelete(String key,
                                                  boolean dropIfEmpty);

  /**
   * Deletes an item on given index in the map.
   *
   * @param key         key of a map
   * @param mkey        mkey of a map
   * @param dropIfEmpty false to remove the key when all elements are removed. true b+ tree will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncMopDelete(String key, String mkey,
                                                  boolean dropIfEmpty);

  /**
   * Deletes an item on given index in the list.
   *
   * @param key         key of a list
   * @param index       list index
   * @param dropIfEmpty true to remove the key when all elements are removed. false list will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncLopDelete(String key, int index,
                                                  boolean dropIfEmpty);

  /**
   * Deletes items on given index range(from..to) in the list.
   *
   * @param key         key of a list
   * @param from        the first index to delete
   * @param to          the last index to delete
   * @param dropIfEmpty true to remove the key when all elements are removed. false list will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncLopDelete(String key, int from,
                                                  int to, boolean dropIfEmpty);

  /**
   * Deletes an item in the set.
   *
   * @param key         key of a set
   * @param value       value of an item
   * @param dropIfEmpty true to remove the key when all elements are removed. false set will remain empty even if all the elements are removed
   * @return whether or not the operation was performed
   */
  public CollectionFuture<Boolean> asyncSopDelete(String key, Object value,
                                                  boolean dropIfEmpty);

  /**
   * Deletes an item in the set.
   *
   * @param <T>
   * @param key         key of a set
   * @param value       value of an item
   * @param dropIfEmpty true to remove the key when all elements are removed. false set will remain empty even if all the elements are removed
   * @param tc          a transcoder to encode the value
   * @return whether or not the operation was performed
   */
  public <T> CollectionFuture<Boolean> asyncSopDelete(String key, T value,
                                                      boolean dropIfEmpty, Transcoder<T> tc);

  /**
   * Get count of elements in given bkey range(from..to) and eFlagFilter.
   *
   * @param key         key of a b+tree
   * @param from        the first bkey
   * @param to          the last bkey
   * @param eFlagFilter element flag filter
   * @return a future that will hold the count of exists element
   */
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        long from, long to, ElementFlagFilter eFlagFilter);

  /**
   * Inserts an item into the b+tree.
   *
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree node
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if this argument is null, Arcus don't assign element flag.
   * @param value               a value to insert into the b+tree
   * @param attributesForCreate attributes of the key
   * @return a future indicating success, false if there was no key and
   * attributesForCreate is null
   */
  public CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                  byte[] eFlag, Object value, CollectionAttributes attributesForCreate);

  /**
   * Inserts an item into the map
   *
   * @param key                 key of a map
   * @param mkey                key of a map node
   * @param value               a value to insert into the map
   * @param attributesForCreate attributes of the key
   * @return a future indicating success, false if there was no key and
   * attributesForCreate is null
   */
  public CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                  Object value, CollectionAttributes attributesForCreate);

  /**
   * Insert a value into each list
   *
   * <pre>
   * Note to the index
   *     The item will be inserted before the element with the given index except below
   *     -1:append, 0:prepend
   * </pre>
   *
   * @param index               list index (the item will be inserted before the element with the given index)
   * @param value               a value to insert into each list
   * @param attributesForCreate attributes of the key
   * @return a future that will indicate the failure list of each operation
   */
  public CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                  Object value, CollectionAttributes attributesForCreate);

  /**
   * Inserts an item into the set.
   *
   * @param key                 key of a set
   * @param value               a value to insert into the set
   * @param attributesForCreate attributes of the key
   * @return a future indicating success, false if there was no key
   * and attributesForCreate parameter is null.
   */
  public CollectionFuture<Boolean> asyncSopInsert(String key, Object value,
                                                  CollectionAttributes attributesForCreate);

  /**
   * Inserts an item into the b+tree.
   *
   * @param <T>
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree node
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if this argument is null, Arcus don't assign element flag.
   * @param value               a value to insert into the b+tree
   * @param attributesForCreate attributes of the key
   * @param tc                  a trancoder to encode the value
   * @return a future indicating success, false if there was no key
   * and attributesForCreate parameter is null.
   */
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                      byte[] eFlag, T value, CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc);

  /**
   * Inserts an item into the map
   *
   * @param <T>
   * @param key                 key of a map
   * @param mkey                key of a map node
   * @param value               a value to insert into the map
   * @param attributesForCreate attributes of the key
   * @param tc                  a trancoder to encode the value
   * @return a future indicating success, false if there was no key
   * and attributesForCreate parameter is null.
   */
  public <T> CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                      T value, CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc);

  /**
   * Insert a value into each list
   *
   * <pre>
   * Note to the index
   *     The item will be inserted before the element with the given index except below
   *     -1:append, 0:prepend
   * </pre>
   *
   * @param index               list index (the item will be inserted before the element with the given index)
   * @param value               a value to insert into each list
   * @param attributesForCreate attributes of the key
   * @param tc                  a transcoder to encode the value
   * @return a future that will indicate the failure list of each operation
   */
  public <T> CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                      T value, CollectionAttributes attributesForCreate, Transcoder<T> tc);

  /**
   * Inserts an item into the set.
   *
   * @param <T>
   * @param key                 key of a set
   * @param value               a value to insert into the set
   * @param tc                  a transcoder to encode the value
   * @param attributesForCreate attributes of the key
   * @return a future indicating success, false if there was no key
   * and attributesForCreate parameter is null
   */
  public <T> CollectionFuture<Boolean> asyncSopInsert(String key, T value,
                                                      CollectionAttributes attributesForCreate, Transcoder<T> tc);

  /**
   * Insert values into a b+ tree
   *
   * @param key                 a key list of b+ tree
   * @param elements
   * @param attributesForCreate attributes of the key
   * @return a future that will indicate the failure list of each operation
   */
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, Object> elements, CollectionAttributes attributesForCreate);

  /**
   * Insert values into a map
   *
   * @param key                 a key list of map
   * @param elements            mkey and value list of map
   * @param attributesForCreate attributes of the key
   * @return a future that will indicate the failure list of each operation
   */
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, Object> elements, CollectionAttributes attributesForCreate);

  /**
   * Insert values into a list
   *
   * <pre>
   * Note to the index
   *     The item will be inserted before the element with the given index except below
   *     -1:append, 0:prepend
   * </pre>
   *
   * @param key                 a key of the list
   * @param index               list index (the item will be inserted before the element with the given index)
   * @param valueList           valuses to insert into the set
   * @param attributesForCreate attributes of the key
   * @return a future that will indicate the failure list of each operation
   */
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<Object> valueList, CollectionAttributes attributesForCreate);

  /**
   * Insert values into a set
   *
   * @param key                 key of a set
   * @param valueList           valuses to insert into the set
   * @param attributesForCreate attributes of the key
   * @return a future that will indicate the failure list of each operation
   */
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<Object> valueList, CollectionAttributes attributesForCreate);

  /**
   * Insert values into a b+ tree
   *
   * @param <T>
   * @param key                 a key list of b+ tree
   * @param elements
   * @param attributesForCreate attributes of the key
   * @param tc                  transcoder to encode value
   * @return a future that will indicate the failure list of each operation
   */
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, T> elements, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Insert values into a map
   *
   * @param <T>
   * @param key                 a key list of map
   * @param elements            mkey and value list of map
   * @param attributesForCreate attributes of the key
   * @param tc                  transcoder to encode value
   * @return a future that will indicate the failure list of each operation
   */
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, T> elements, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Insert values into a list
   *
   * <pre>
   * Note to the index
   *     The item will be inserted before the element with the given index except below
   *     -1:append, 0:prepend
   * </pre>
   *
   * @param <T>
   * @param key                 a key of the list
   * @param index               list index (the item will be inserted before the element with the given index)
   * @param valueList           valuses to insert into the set
   * @param attributesForCreate attributes of the key
   * @param tc                  transcoder to encode value
   * @return a future that will indicate the failure list of each operation
   */
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<T> valueList, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Insert values into a set
   *
   * @param <T>
   * @param key                 key of a set
   * @param valueList           valuses to insert into the set
   * @param attributesForCreate attributes of the key
   * @param tc                  transcoder to encode value
   * @return a future that will indicate the failure list of each operation
   */
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<T> valueList, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Flush all items that starts with given prefix from all servers.
   *
   * @param prefix prefix of the keys
   * @return whether or not the operation was accepted
   */
  public OperationFuture<Boolean> flush(final String prefix);

  /**
   * Flush all items that starts with given prefix from all servers with a delay of application.
   *
   * @param prefix prefix of the keys
   * @param delay  the period of time to delay, in seconds
   * @return whether or not the operation was accepted
   */
  public OperationFuture<Boolean> flush(final String prefix, final int delay);

  /**
   * Get elements that matched both filter and bkey range criteria from
   * multiple b+tree. The result is sorted by order of bkey.
   *
   * @param keyList     b+ tree key list
   * @param from        bkey index from
   * @param to          bkey index to
   * @param eFlagFilter element flag filter
   * @param offset      0-base offset
   * @param count       number of returning values (0 to all)
   * @return a future that will hold the return value list of the fetch.
   */
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter,
          int offset, int count);

  /**
   * Get elements that matched both filter and bkey range criteria from
   * multiple b+tree. The result is sorted by order of bkey.
   *
   * @param keyList     b+ tree key list
   * @param from        bkey index from
   * @param to          bkey index to
   * @param eFlagFilter element flag filter
   * @param count       number of returning values (0 to all)
   * @param smgetMode   smgetMode
   * @return a future that will hold the return value list of the fetch.
   */
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter,
          int count, SMGetMode smgetMode);

  /**
   * Update or insert an element.
   *
   * Element that matched both key and bkey criteria will updated.
   * If element is not exists and attributesForCreate argument is not null.
   * Create the tree that has an attribute of 'attributesForCreate' and insert the element that has elementFlag and value.
   *
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree element
   * @param elementFlag         flag of element
   * @param value               value of element
   * @param attributesForCreate create a b+tree with this attributes, if given key of b+tree
   *                            is not exists.
   * @return a future indicating success, false if there was no key and
   * attributesForCreate argument is null.
   */
  public CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                  byte[] elementFlag, Object value, CollectionAttributes attributesForCreate);

  /**
   * Update or insert an element.
   *
   * Element that matched both key and bkey criteria will updated.
   * If element is not exists and attributesForCreate argument is not null.
   * Create the tree that has an attribute of 'attributesForCreate' and insert the element that has elementFlag and value.
   *
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree element
   * @param elementFlag         flag of element
   * @param value               value of element
   * @param attributesForCreate create a b+tree with this attributes, if given key of b+tree
   *                            is not exists.
   * @param tc                  transcoder to encode value
   * @return a future indicating success, false if there was no key and
   * attributesForCreate argument is null.
   */
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                      byte[] elementFlag, T value, CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc);

  /**
   * Update an element from the b+tree
   *
   * @param key         key of a b+tree
   * @param bkey        key of a b+tree element
   * @param eFlagUpdate new flag of element.
   *                    do not update the eflag if this argument is null.
   * @param value       new value of element.
   *                    do not update the value if this argument is null.
   * @return a future indicating success
   */
  public CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                  ElementFlagUpdate eFlagUpdate, Object value);

  /**
   * Update an element from the b+tree
   *
   * @param key         key of a b+tree
   * @param bkey        key of a b+tree element
   * @param eFlagUpdate new flag of element.
   *                    do not update the eflag if this argument is null.
   * @param value       new value of element.
   *                    do not update the value if this argument is null.
   * @param tc          a transcoder to encode the value of element
   * @return a future indicating success
   */
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                      ElementFlagUpdate eFlagUpdate, T value, Transcoder<T> tc);

  /**
   * Update an element from the map
   *
   * @param key   key of a map
   * @param mkey  key of a map element
   * @param value new value of element.
   * @return a future indicating success
   */
  public CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                  Object value);

  /**
   * Update an element from the map
   *
   * @param key   key of a map
   * @param mkey  key of a map element
   * @param value new value of element.
   * @param tc    a transcoder to encode the value of element
   * @return a future indicating success
   */
  public <T> CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                      T value, Transcoder<T> tc);

  /**
   * Update elements from the b+tree
   *
   * @param key      key of a b+tree
   * @param elements list of b+tree elements
   * @return a future indicating success
   */
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<Object>> elements);

  /**
   * Update elements from the b+tree
   *
   * @param key      key of a b+tree
   * @param elements list of b+tree elements
   * @param tc       a transcoder to encode the value of element
   * @return a future indicating success
   */
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<T>> elements, Transcoder<T> tc);

  /**
   * Update elements from the map
   *
   * @param key      key of a map
   * @param elements Map of map element
   * @return a future indicating success
   */
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, Object> elements);

  /**
   * Update elements from the map
   *
   * @param key      key of a map
   * @param elements Map of map element
   * @param tc       a transcoder to encode the value of element
   * @return a future indicating success
   */
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, T> elements, Transcoder<T> tc);

  /**
   * Insert an item into the b+tree
   *
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree element
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if
   *                            this value is null, we don't assign element flag.
   * @param value               new value of element. this value can't be null.
   * @param attributesForCreate create a b+tree with this attributes, if given key is not
   *                            exists.
   * @return a future indicating success, false if there was no key and attributesForCreate argument is null
   */
  public CollectionFuture<Boolean> asyncBopInsert(String key,
                                                  byte[] bkey, byte[] eFlag, Object value,
                                                  CollectionAttributes attributesForCreate);

  /**
   * Insert an item into the b+tree
   *
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree element
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if
   *                            this value is null, we don't assign element flag.
   * @param value               new value of element. do not update the value if this argument
   *                            is null. this value can't be null.
   * @param attributesForCreate create a b+tree with this attributes, if given key is not
   *                            exists.
   * @param tc                  transcoder to encode value
   * @return a future indicating success, false if there was no key and attributesForCreate argument is null
   */
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key,
                                                      byte[] bkey, byte[] eFlag, T value,
                                                      CollectionAttributes attributesForCreate, Transcoder<T> tc);

  /**
   * Retrieves count number of items in given bkey range(from..to)
   * from offset in the b+tree.
   * The returned map from the future should be sorted by the given range.
   * <pre>
   * 	from >= to : in descending order
   * 	from < to  : in ascending order
   * </pre>
   *
   * @param key         key of a b+tree
   * @param from        the first bkey
   * @param to          the last bkey
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset
   * @param count       number of returning values (0 to all)
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty false to remove the key when all elements are removed. true b+ tree will remain empty even if all the elements are removed
   * @return a future that will hold the return value map of the fetch
   */
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset,
          int count, boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves count number of items in given bkey range(from..to)
   * from offset in the b+tree.
   * The returned map from the future should be sorted by the given range.
   * <pre>
   * 	from >= to : in descending order
   * 	from < to  : in ascending order
   * </pre>
   *
   * @param key         key of a b+tree
   * @param from        the first bkey
   * @param to          the last bkey
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset
   * @param count       number of returning values (0 to all)
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty false to remove the key when all elements are removed. true b+ tree will remain empty even if all the elements are removed
   * @param tc          transcoder to decode value
   * @return a future that will hold the return value map of the fetch
   */
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset,
          int count, boolean withDelete, boolean dropIfEmpty,
          Transcoder<T> tc);

  /**
   * Update or insert an element.
   *
   * Element that matched both key and bkey criteria will updated. If element
   * is not exists and attributesForCreate argument is not null. Create the
   * tree that has an attribute of 'attributesForCreate' and insert the
   * element that has elementFlag and value.
   *
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree element
   * @param elementFlag         flag of element
   * @param value               value of element
   * @param attributesForCreate create a b+tree with this attributes, if given key of b+tree
   *                            is not exists.
   * @return a future indicating success, false if there was no key and
   * attributesForCreate argument is null.
   */
  public CollectionFuture<Boolean> asyncBopUpsert(String key,
                                                  byte[] bkey, byte[] elementFlag, Object value,
                                                  CollectionAttributes attributesForCreate);

  /**
   * Update or insert an element.
   *
   * Element that matched both key and bkey criteria will updated. If element
   * is not exists and attributesForCreate argument is not null. Create the
   * tree that has an attribute of 'attributesForCreate' and insert the
   * element that has elementFlag and value.
   *
   * @param key                 key of a b+tree
   * @param bkey                key of a b+tree element
   * @param elementFlag         flag of element
   * @param value               value of element
   * @param attributesForCreate create a b+tree with this attributes, if given key of b+tree
   *                            is not exists.
   * @param tc                  transcoder to encode value
   * @return a future indicating success, false if there was no key and
   * attributesForCreate argument is null.
   */
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key,
                                                      byte[] bkey, byte[] elementFlag, T value,
                                                      CollectionAttributes attributesForCreate, Transcoder<T> tc);

  /**
   * Get count of elements in given bkey range(from..to) and eFlagFilter.
   *
   * @param key         key of a b+tree
   * @param from        the first bkey
   * @param to          the last bkey
   * @param eFlagFilter element flag filter
   * @return a future that will hold the count of exists element
   */
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        byte[] from, byte[] to, ElementFlagFilter eFlagFilter);

  /**
   * Update an element from the b+tree
   *
   * @param key         key of a b+tree
   * @param bkey        key of a b+tree element
   * @param eFlagUpdate new flag of element. do not update the eflag if this argument
   *                    is null.
   * @param value       new value of element. do not update the value if this argument
   *                    is null.
   * @return a future indicating success
   */
  public CollectionFuture<Boolean> asyncBopUpdate(String key,
                                                  byte[] bkey, ElementFlagUpdate eFlagUpdate, Object value);

  /**
   * Update an element from the b+tree
   *
   * @param key         key of a b+tree
   * @param bkey        key of a b+tree element
   * @param eFlagUpdate new flag of element. do not update the eflag if this argument
   *                    is null.
   * @param value       new value of element. do not update the value if this argument
   *                    is null.
   * @param tc          transcoder to encode value
   * @return a future indicating success
   */
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key,
                                                      byte[] bkey, ElementFlagUpdate eFlagUpdate, T value, Transcoder<T> tc);

  /**
   * Checks multiple items' membership in a set using the default transcoder.
   *
   * @param key    key of set
   * @param values value list to check membership
   * @return a future indicating the map that represent existence of each
   * values
   */
  public CollectionFuture<Map<Object, Boolean>> asyncSopPipedExistBulk(
          String key, List<Object> values);

  /**
   * Checks multiple items' membership in a set using the default transcoder.
   *
   * @param key    key of set
   * @param values value list to check membership
   * @param tc     transcoder to decode each value
   * @return a future indicating the map that represent existence of each
   * value
   */
  public <T> CollectionFuture<Map<T, Boolean>> asyncSopPipedExistBulk(
          String key, List<T> values, Transcoder<T> tc);

  /**
   * Insert elements into a b+tree
   *
   * @param key                 a key list of b+ tree
   * @param elements            element list which insert into b+tree
   * @param attributesForCreate create a b+tree with this attributes, if given key is not
   *                            exists.
   * @return a future that will hold the index of iteration sequence which
   * failed elements and result code.
   */
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<Object>> elements,
          CollectionAttributes attributesForCreate);

  /**
   * Insert elements into a b+tree
   *
   * @param key                 a key list of b+ tree
   * @param elements            element list which insert into b+tree
   * @param attributesForCreate create a b+tree with this attributes, if given key is not exists.
   * @param tc                  transcoder to decode value
   * @return a future that will hold the index of iteration sequence which
   * failed elements and result code.
   */
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<T>> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc);

  /**
   * Retrieves count number of items in given bkey in the b+tree.
   *
   * @param key         key of a b+tree
   * @param bkey        bkey of an element
   * @param eFlagFilter element flag filter
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty false to remove the key when all elements are removed. true b+
   *                    tree will remain empty even if all the elements are removed
   * @return a future that will hold the return value map of the fetch
   */
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter, boolean withDelete, boolean dropIfEmpty);

  /**
   * Retrieves count number of items in given bkey in the b+tree.
   *
   * @param key         key of a b+tree
   * @param bkey        bkey of an element
   * @param eFlagFilter element flag filter
   * @param withDelete  true to remove the returned item in the b+tree
   * @param dropIfEmpty false to remove the key when all elements are removed. true b+
   *                    tree will remain empty even if all the elements are removed
   * @param tc          transcoder to decode value
   * @return a future that will hold the return value map of the fetch
   */
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter, boolean withDelete, boolean dropIfEmpty,
          Transcoder<T> tc);

  /**
   * Get elements that matched both filter and bkey range criteria from
   * multiple b+tree. The result is sorted by order of bkey.
   *
   * @param keyList     b+ tree key list
   * @param from        bkey index from
   * @param to          bkey index to
   * @param eFlagFilter element flag filter
   * @param offset      0-base offset
   * @param count       number of returning values (0 to all)
   * @return a future that will hold the return value list of the fetch.
   */
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
          int offset, int count);

  /**
   * Get elements that matched both filter and bkey range criteria from
   * multiple b+tree. The result is sorted by order of bkey.
   *
   * @param keyList     b+ tree key list
   * @param from        bkey index from
   * @param to          bkey index to
   * @param eFlagFilter element flag filter
   * @param count       number of returning values (0 to all)
   * @param smgetMode   smgetMode
   * @return a future that will hold the return value list of the fetch.
   */
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
          int count, SMGetMode smgetMode);

  /**
   * Insert one item into multiple b+trees at once.
   *
   * @param keyList             key list of b+tree
   * @param bkey                key of a b+tree element.
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if
   *                            this value is null, Arcus don't assign element flag.
   * @param value               value of element. this value can't be null.
   * @param attributesForCreate create a b+tree with this attributes, if given key is not
   *                            exists.
   * @return a future indicating success
   */
  public abstract Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, byte[] bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate);

  /**
   * Insert one item into multiple b+trees at once.
   *
   * @param keyList             key list of b+tree
   * @param bkey                key of a b+tree element.
   * @param eFlag               element flag. Length of element flag is between 1 and 31. if
   *                            this value is null, Arcus don't assign element flag.
   * @param value               value of element. this value can't be null.
   * @param attributesForCreate create a b+tree with this attributes, if given key is not
   *                            exists.
   * @param tc                  transcoder to encode value
   * @return a future indicating success
   */
  public abstract <T> Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, byte[] bkey, byte[] eFlag, T value, CollectionAttributes attributesForCreate,
          Transcoder<T> tc);

  /**
   * Get elements from each b+tree.
   *
   * @param keyList     key list of b+tree
   * @param from        bkey from
   * @param to          bkey to
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset (max = 50)
   * @param count       number of returning values (0 to all) (max = 200)
   * @return future indicating result of each b+tree
   */
  public CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> asyncBopGetBulk(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count);

  /**
   * Get elements from each b+tree.
   *
   * @param keyList     key list of b+tree
   * @param from        bkey from
   * @param to          bkey to
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset (max = 50)
   * @param count       number of returning values (0 to all) (max = 200)
   * @param tc          transcoder to decode value
   * @return future indicating result of each b+tree
   */
  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, T>>> asyncBopGetBulk(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count,
          Transcoder<T> tc);

  /**
   * Get elements from each b+tree.
   *
   * @param keyList     key list of b+tree
   * @param from        bkey from
   * @param to          bkey to
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset (max = 50)
   * @param count       number of returning values (0 to all) (max = 200)
   * @return future indicating result of each b+tree
   */
  public CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count);

  /**
   * Get elements from each b+tree.
   *
   * @param keyList     key list of b+tree
   * @param from        bkey from
   * @param to          bkey to
   * @param eFlagFilter element flag filter
   * @param offset      0-based offset (max = 50)
   * @param count       number of returning values (0 to all) (max = 200)
   * @param tc          transcoder to decode value
   * @return future indicating result of each b+tree
   */
  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, T>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count,
          Transcoder<T> tc);

  /**
   * Increment the element's value in b+tree.
   *
   * @param key    b+tree item's key
   * @param subkey element's key
   * @param by     increment amount
   * @return future holding the incremented value
   */
  public CollectionFuture<Long> asyncBopIncr(String key, long subkey, int by);

  /**
   * Increment the element's value in b+tree.
   *
   * @param key    b+tree item's key
   * @param subkey element's key (byte-array type key)
   * @param by     increment amount
   * @return future holding the incremented value
   */
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] subkey, int by);

  /**
   * Increment the element's value in b+tree.
   *
   * @param key     b+tree item's key
   * @param subkey  element's key (byte-array type key)
   * @param by      increment amount
   * @param initial optional element's initial value
   * @param eFlag   optional element flag
   * @return future holding the incremented value
   */
  public CollectionFuture<Long> asyncBopIncr(String key, long subkey,
                                             int by, long initial, byte[] eFlag);

  /**
   * Increment the element's value in b+tree.
   *
   * @param key     b+tree item's key
   * @param subkey  element's key (byte-array type key)
   * @param by      increment amount
   * @param initial optional element's initial value
   * @param eFlag   optional element flag
   * @return future holding the incremented value
   */
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] subkey,
                                             int by, long initial, byte[] eFlag);

  /**
   * Decrement the element's value in b+tree.
   *
   * @param key    b+tree item's key
   * @param subkey element's key
   * @param by     decrement amount
   * @return future holding the decremented value
   */
  public CollectionFuture<Long> asyncBopDecr(String key, long subkey, int by);

  /**
   * Decrement the element's value in b+tree.
   *
   * @param key    b+tree item's key
   * @param subkey element's key (byte-array type key)
   * @param by     decrement amount
   * @return future holding the decremented value
   */
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] subkey, int by);

  /**
   * Decrement the element's value in b+tree.
   *
   * @param key     b+tree item's key
   * @param subkey  element's key (byte-array type key)
   * @param by      decrement amount
   * @param initial optional element's initial value
   * @param eFlag   optional element flag
   * @return future holding the decremented value
   */
  public CollectionFuture<Long> asyncBopDecr(String key, long subkey,
                                             int by, long initial, byte[] eFlag);

  /**
   * Decrement the element's value in b+tree.
   *
   * @param key     b+tree item's key
   * @param subkey  element's key (byte-array type key)
   * @param by      decrement amount
   * @param initial optional element's initial value
   * @param eFlag   optional element flag
   * @return future holding the decremented value
   */
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] subkey,
                                             int by, long initial, byte[] eFlag);

  /**
   * Get an element from b+tree using its position.
   *
   * @param key   b+tree item's key
   * @param order ascending/descending order
   * @param pos   element's position
   * @return future holding the map of the element and its position
   */
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int pos);

  /**
   * Get an element from b+tree using its position.
   *
   * @param key   b+tree item's key
   * @param order ascending/descending order
   * @param pos   element's position
   * @param tc    transcoder to serialize and unserialize value
   * @return future holding the map of the element and its position
   */
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int pos, Transcoder<T> tc);

  /**
   * Get multiple elements from b+tree using positions.
   *
   * @param key   b+tree item's key
   * @param order ascending/descending order
   * @param from  start position
   * @param to    end position
   * @return future holding the map of the elements and their positions
   */
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int from, int to);

  /**
   * Get multiple elements from b+tree using positions.
   *
   * @param key   b+tree item's key
   * @param order ascending/descending order
   * @param from  start position
   * @param to    end position
   * @param tc    transcoder to serialize and unserialize value
   * @return future holding the map of the elements and their positions
   */
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int from, int to, Transcoder<T> tc);

  /**
   * Get the position of the element in b+tree.
   *
   * @param key      b+tree item's key
   * @param longBKey element's key
   * @param order    ascending/descending order
   * @return future holding the element's position
   */
  public CollectionFuture<Integer> asyncBopFindPosition(
          String key, long longBKey, BTreeOrder order);


  /**
   * Get the position of the element in b+tree.
   *
   * @param key           b+tree item's key
   * @param byteArrayBKey element's key (byte-array type key)
   * @param order         ascending/descending order
   * @return future holding the element's position
   */
  public CollectionFuture<Integer> asyncBopFindPosition(
          String key, byte[] byteArrayBKey, BTreeOrder order);

  /**
   * Get the position, element, and neighbor elements of given bkey in b+tree.
   *
   * @param key      b+tree item's key
   * @param longBKey element's bkey (long type bkey)
   * @param order    ascending/descending order
   * @param count    number of elements requested in both direction
   * @return future holding the element's position
   */
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopFindPositionWithGet(
          String key, long longBKey, BTreeOrder order, int count);

  /**
   * Get the position, element, and neighbor elements of given bkey in b+tree.
   *
   * @param key      b+tree item's key
   * @param longBKey element's bkey (long type bkey)
   * @param order    ascending/descending order
   * @param count    number of elements requested in both direction
   * @param tc       a transcoder to decode returned values
   * @return future holding the element's position
   */
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, long longBKey, BTreeOrder order, int count, Transcoder<T> tc);

  /**
   * Get the position, element, and neighbor elements of given bkey in b+tree.
   *
   * @param key           b+tree item's key
   * @param byteArrayBKey element's bkey (byte-array type bkey)
   * @param order         ascending/descending order
   * @param count         number of elements requested in both direction
   * @return future holding the element's position
   */
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopFindPositionWithGet(
          String key, byte[] byteArrayBKey, BTreeOrder order, int count);

  /**
   * Get the position, element, and neighbor elements of given bkey in b+tree.
   *
   * @param key           b+tree item's key
   * @param byteArrayBKey element's bkey (byte-array type bkey)
   * @param order         ascending/descending order
   * @param count         number of elements requested in both direction
   * @param tc            a transcoder to decode returned values
   * @return future holding the element's position
   */
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, byte[] byteArrayBKey, BTreeOrder order, int count, Transcoder<T> tc);

  /**
   * Insert an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @return future holding the success/error of the operation and the trimmed element
   */
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate);

  /**
   * Insert an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @param transcoder          transcoder to serialize and unserialize value
   * @return future holding the success/error of the operation and the trimmed element
   */
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder);

  /**
   * Insert an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key (byte-array type key)
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @return future holding the success/error of the operation and the trimmed element
   */
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate);

  /**
   * Insert an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key (byte-array type key)
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @param transcoder          transcoder to serialize and unserialize value
   * @return future holding the success/error of the operation and the trimmed element
   */
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder);

  /**
   * Upsert (update if element exists, insert otherwise) an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @return future holding the success/error of the operation and the trimmed element
   */
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate);

  /**
   * Upsert (update if element exists, insert otherwise) an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @param transcoder          transcoder to serialize and unserialize value
   * @return future holding the success/error of the operation and the trimmed element
   */
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder);

  /**
   * Upsert (update if element exists, insert otherwise) an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key (byte-array type key)
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @return future holding the success/error of the operation and the trimmed element
   */
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate);

  /**
   * Upsert (update if element exists, insert otherwise) an element into b+tree and also get the "trimmed" element if any.
   *
   * @param key                 b+tree item's key
   * @param bkey                element's key (byte-array type key)
   * @param eFlag               optional element flag
   * @param value               element's value
   * @param attributesForCreate optional attributes used for creating b+tree item
   * @param transcoder          transcoder to serialize and unserialize value
   * @return future holding the success/error of the operation and the trimmed element
   */
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder);

}
