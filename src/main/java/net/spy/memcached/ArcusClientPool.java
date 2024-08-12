/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2021 JaM2in Co., Ltd.
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

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.Attributes;
import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionPipedInsert;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.BTreeStoreAndGetFuture;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.CollectionGetBulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Bags for ArcusClient
 */
public class ArcusClientPool implements MemcachedClientIF, ArcusClientIF {

  private final int poolSize;
  private final ArcusClient[] client;
  private final Random rand;

  public ArcusClientPool(int poolSize, ArcusClient[] client) {

    this.poolSize = poolSize;
    this.client = client;
    rand = new Random();
  }

  /**
   * Returns single ArcusClient
   *
   * @return ArcusClient
   */
  ArcusClient getClient() {
    return client[rand.nextInt(poolSize)];
  }

  /**
   * Returns all ArcusClient in pool
   *
   * @return ArcusClient array
   */
  ArcusClient[] getAllClients() {
    return client;
  }

  @Override
  public void shutdown() {
    for (ArcusClient ac : client) {
      ac.shutdown();
    }
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit unit) {
    boolean result = true;

    for (ArcusClient ac : client) {
      result = ac.shutdown(timeout, unit) && result;
    }

    return result;
  }

  @Override
  public Collection<SocketAddress> getAvailableServers() {
    return this.getClient().getAvailableServers();
  }

  @Override
  public Collection<SocketAddress> getUnavailableServers() {
    return this.getClient().getUnavailableServers();
  }

  @Override
  public Transcoder<Object> getTranscoder() {
    return this.getClient().getTranscoder();
  }

  @Override
  public NodeLocator getNodeLocator() {
    return this.getClient().getNodeLocator();
  }

  @Override
  public OperationFuture<Boolean> append(long cas, String key, Object val) {
    return this.getClient().append(cas, key, val);
  }

  @Override
  public <T> OperationFuture<Boolean> append(long cas, String key, T val,
                                             Transcoder<T> tc) {
    return this.getClient().append(cas, key, val, tc);
  }

  @Override
  public OperationFuture<Boolean> prepend(long cas, String key, Object val) {
    return this.getClient().prepend(cas, key, val);
  }

  @Override
  public <T> OperationFuture<Boolean> prepend(long cas, String key, T val,
                                              Transcoder<T> tc) {
    return this.getClient().prepend(cas, key, val, tc);
  }

  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, T value,
                                                   Transcoder<T> tc) {
    return this.getClient().asyncCAS(key, casId, value, tc);
  }

  @Override
  public OperationFuture<CASResponse> asyncCAS(String key, long casId, Object value) {

    return this.getClient().asyncCAS(key, casId, value);
  }

  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, int exp, T value,
                                          Transcoder<T> tc) {
    return this.getClient().asyncCAS(key, casId, exp, value, tc);
  }

  @Override
  public OperationFuture<CASResponse> asyncCAS(String key, long casId, int exp, Object value) {
    return this.getClient().asyncCAS(key, casId, exp, value);
  }

  @Override
  public <T> CASResponse cas(String key, long casId, int exp, T value, Transcoder<T> tc)
      throws OperationTimeoutException {
    return this.getClient().cas(key, casId, exp, value, tc);
  }

  @Override
  public CASResponse cas(String key, long casId, int exp, Object value)
      throws OperationTimeoutException {
    return this.getClient().cas(key, casId, exp, value);
  }

  @Override
  public <T> CASResponse cas(String key, long casId, T value, Transcoder<T> tc)
          throws OperationTimeoutException {
    return this.getClient().cas(key, casId, value, tc);
  }

  @Override
  public CASResponse cas(String key, long casId, Object value)
          throws OperationTimeoutException {
    return this.getClient().cas(key, casId, value);
  }

  @Override
  public <T> OperationFuture<Boolean> add(String key, int exp, T o, Transcoder<T> tc) {
    return this.getClient().add(key, exp, o, tc);
  }

  @Override
  public OperationFuture<Boolean> add(String key, int exp, Object o) {
    return this.getClient().add(key, exp, o);
  }

  @Override
  public <T> OperationFuture<Boolean> set(String key, int exp, T o, Transcoder<T> tc) {
    return this.getClient().set(key, exp, o, tc);
  }

  @Override
  public OperationFuture<Boolean> set(String key, int exp, Object o) {
    return this.getClient().set(key, exp, o);
  }

  @Override
  public <T> OperationFuture<Boolean> replace(String key, int exp, T o,
                                              Transcoder<T> tc) {
    return this.getClient().replace(key, exp, o, tc);
  }

  @Override
  public OperationFuture<Boolean> replace(String key, int exp, Object o) {
    return this.getClient().replace(key, exp, o);
  }

  @Override
  public <T> GetFuture<T> asyncGet(String key, Transcoder<T> tc) {
    return this.getClient().asyncGet(key, tc);
  }

  @Override
  public GetFuture<Object> asyncGet(String key) {
    return this.getClient().asyncGet(key);
  }

  @Override
  public <T> GetFuture<CASValue<T>> asyncGets(String key, Transcoder<T> tc) {
    return this.getClient().asyncGets(key, tc);
  }

  @Override
  public GetFuture<CASValue<Object>> asyncGets(String key) {
    return this.getClient().asyncGets(key);
  }

  @Override
  public <T> CASValue<T> gets(String key, Transcoder<T> tc)
          throws OperationTimeoutException {
    return this.getClient().gets(key, tc);
  }

  @Override
  public CASValue<Object> gets(String key) throws OperationTimeoutException {
    return this.getClient().gets(key);
  }

  @Override
  public <T> T get(String key, Transcoder<T> tc)
          throws OperationTimeoutException {
    return this.getClient().get(key, tc);
  }

  @Override
  public Object get(String key) throws OperationTimeoutException {
    return this.getClient().get(key);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Iterator<Transcoder<T>> tcs) {
    return this.getClient().asyncGetBulk(keys, tcs);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Transcoder<T> tc) {
    return this.getClient().asyncGetBulk(keys, tc);
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
    return this.getClient().asyncGetBulk(keys);
  }

  @Deprecated
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc,
                                                     String... keys) {
    return this.getClient().asyncGetBulk(tc, keys);
  }

  @Deprecated
  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
    return this.getClient().asyncGetBulk(keys);
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
                                                                Iterator<Transcoder<T>> tcs) {
    return this.getClient().asyncGetsBulk(keys, tcs);
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
                                                                Transcoder<T> tc) {
    return this.getClient().asyncGetsBulk(keys, tc);
  }

  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(Collection<String> keys) {
    return this.getClient().asyncGetsBulk(keys);
  }

  @Deprecated
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Transcoder<T> tc,
                                                                String... keys) {
    return this.getClient().asyncGetsBulk(tc, keys);
  }

  @Deprecated
  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(String... keys) {
    return this.getClient().asyncGetsBulk(keys);
  }

  @Override
  public <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc)
          throws OperationTimeoutException {
    return this.getClient().getBulk(keys, tc);
  }

  @Override
  public Map<String, Object> getBulk(Collection<String> keys)
          throws OperationTimeoutException {
    return this.getClient().getBulk(keys);
  }

  @Deprecated
  @Override
  public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys)
          throws OperationTimeoutException {
    return this.getClient().getBulk(tc, keys);
  }

  @Deprecated
  @Override
  public Map<String, Object> getBulk(String... keys)
          throws OperationTimeoutException {
    return this.getClient().getBulk(keys);
  }

  @Override
  public <T> Map<String, CASValue<T>> getsBulk(Collection<String> keys, Transcoder<T> tc)
          throws OperationTimeoutException {
    return this.getClient().getsBulk(keys, tc);
  }

  @Override
  public Map<String, CASValue<Object>> getsBulk(Collection<String> keys)
          throws OperationTimeoutException {
    return this.getClient().getsBulk(keys);
  }

  @Deprecated
  @Override
  public <T> Map<String, CASValue<T>> getsBulk(Transcoder<T> tc, String... keys)
          throws OperationTimeoutException {
    return this.getClient().getsBulk(tc, keys);
  }

  @Deprecated
  @Override
  public Map<String, CASValue<Object>> getsBulk(String... keys)
          throws OperationTimeoutException {
    return this.getClient().getsBulk(keys);
  }

  @Override
  public Map<SocketAddress, String> getVersions() {
    return this.getClient().getVersions();
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats() {
    return this.getClient().getStats();
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats(String prefix) {
    return this.getClient().getStats(prefix);
  }

  @Override
  public long incr(String key, int by) throws OperationTimeoutException {
    return this.getClient().incr(key, by);
  }

  @Override
  public long decr(String key, int by) throws OperationTimeoutException {
    return this.getClient().decr(key, by);
  }

  @Override
  public long incr(String key, int by, long def)
          throws OperationTimeoutException {
    return this.getClient().incr(key, by, def);
  }

  @Override
  public long incr(String key, int by, long def, int exp)
          throws OperationTimeoutException {
    return this.getClient().incr(key, by, def, exp);
  }

  @Override
  public long decr(String key, int by, long def)
          throws OperationTimeoutException {
    return this.getClient().decr(key, by, def);
  }

  @Override
  public long decr(String key, int by, long def, int exp)
          throws OperationTimeoutException {
    return this.getClient().decr(key, by, def, exp);
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, int by) {
    return this.getClient().asyncIncr(key, by);
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, int by, long def, int exp) {
    return this.getClient().asyncIncr(key, by, def, exp);
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, int by) {
    return this.getClient().asyncDecr(key, by);
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, int by, long def, int exp) {
    return this.getClient().asyncDecr(key, by, def, exp);
  }

  @Override
  public OperationFuture<Boolean> delete(String key) {
    return this.getClient().delete(key);
  }

  @Override
  public Future<Boolean> flush(int delay) {
    return this.getClient().flush(delay);
  }

  @Override
  public Future<Boolean> flush() {
    return this.getClient().flush();
  }

  @Override
  public boolean addObserver(ConnectionObserver obs) {
    return this.getClient().addObserver(obs);
  }

  @Override
  public boolean removeObserver(ConnectionObserver obs) {
    return this.getClient().removeObserver(obs);
  }

  @Override
  public Set<String> listSaslMechanisms() {
    return this.getClient().listSaslMechanisms();
  }

  @Override
  public CollectionFuture<Boolean> asyncSetAttr(String key, Attributes attrs) {
    return this.getClient().asyncSetAttr(key, attrs);
  }

  @Override
  public CollectionFuture<CollectionAttributes> asyncGetAttr(String key) {
    return this.getClient().asyncGetAttr(key);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncSopExist(String key, T value,
                                                     Transcoder<T> tc) {
    return this.getClient().asyncSopExist(key, value, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopExist(String key, Object value) {
    return this.getClient().asyncSopExist(key, value);
  }

  @Deprecated
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          List<String> key, int exp, T o, Transcoder<T> tc) {
    return this.getClient().asyncSetBulk(key, exp, o, tc);
  }

  @Deprecated
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          List<String> key, int exp, Object o) {
    return this.getClient().asyncSetBulk(key, exp, o);
  }

  @Deprecated
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          Map<String, T> o, int exp, Transcoder<T> tc) {
    return this.getClient().asyncSetBulk(o, exp, tc);
  }

  @Deprecated
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSetBulk(
          Map<String, Object> o, int exp) {
    return this.getClient().asyncSetBulk(o, exp);
  }


  @Override
  public <T> Future<Map<String, OperationStatus>> asyncStoreBulk(
          StoreType type, List<String> key, int exp, T o, Transcoder<T> tc) {
    return this.getClient().asyncStoreBulk(type, key, exp, o, tc);
  }

  @Override
  public Future<Map<String, OperationStatus>> asyncStoreBulk(
          StoreType type, List<String> key, int exp, Object o) {
    return this.getClient().asyncStoreBulk(type, key, exp, o);
  }

  @Override
  public <T> Future<Map<String, OperationStatus>> asyncStoreBulk(
          StoreType type, Map<String, T> o, int exp, Transcoder<T> tc) {
    return this.getClient().asyncStoreBulk(type, o, exp, tc);
  }

  @Override
  public Future<Map<String, OperationStatus>> asyncStoreBulk(
          StoreType type, Map<String, Object> o, int exp) {
    return this.getClient().asyncStoreBulk(type, o, exp);
  }

  @Override
  public Future<Map<String, OperationStatus>> asyncDeleteBulk(
          List<String> key) {
    return this.getClient().asyncDeleteBulk(key);
  }

  @Deprecated
  @Override
  public Future<Map<String, OperationStatus>> asyncDeleteBulk(
          String... key) {
    return this.getClient().asyncDeleteBulk(key);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncBopInsertBulk(keyList, bkey, eFlag, value,
            attributesForCreate, tc);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopInsertBulk(keyList, bkey, eFlag, value,
            attributesForCreate);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncMopInsertBulk(keyList, mkey, value,
            attributesForCreate, tc);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncMopInsertBulk(keyList, mkey, value,
            attributesForCreate);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncLopInsertBulk(keyList, index, value,
            attributesForCreate, tc);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncLopInsertBulk(keyList, index, value,
            attributesForCreate);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncSopInsertBulk(keyList, value,
            attributesForCreate, tc);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncSopInsertBulk(keyList, value,
            attributesForCreate);
  }

  @Override
  public int getMaxPipedItemCount() {
    return CollectionPipedInsert.MAX_PIPED_ITEM_COUNT;
  }

  @Override
  public CollectionFuture<Boolean> asyncBopCreate(String key,
                                                  ElementValueType valueType,
                                                  CollectionAttributes attributes) {
    return this.getClient().asyncBopCreate(key, valueType, attributes);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopCreate(String key,
                                                  ElementValueType type,
                                                  CollectionAttributes attributes) {
    return this.getClient().asyncMopCreate(key, type, attributes);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopCreate(String key,
                                                  ElementValueType type,
                                                  CollectionAttributes attributes) {
    return this.getClient().asyncSopCreate(key, type, attributes);
  }

  @Override
  public CollectionFuture<Boolean> asyncLopCreate(String key,
                                                  ElementValueType type,
                                                  CollectionAttributes attributes) {
    return this.getClient().asyncLopCreate(key, type, attributes);
  }

//  @Override
//  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
//                                                                  long bkey,
//                                                                  ElementFlagFilter eFlagFilter) {
//    return this.getClient().asyncBopGet(key, bkey, eFlagFilter);
//  }

  @Override
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long bkey,
                                                                  ElementFlagFilter eFlagFilter,
                                                                  boolean withDelete,
                                                                  boolean dropIfEmpty) {
    return this.getClient().asyncBopGet(key, bkey, eFlagFilter, withDelete,
            dropIfEmpty);
  }

//  @Override
//  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
//                                                                  long from, long to,
//                                                                  ElementFlagFilter eFlagFilter,
//                                                                  int offset, int count) {
//    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
//            count);
//  }

  @Override
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long from, long to,
                                                                  ElementFlagFilter eFlagFilter,
                                                                  int offset, int count,
                                                                  boolean withDelete,
                                                                  boolean dropIfEmpty) {
    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
            count, withDelete, dropIfEmpty);
  }

//  @Override
//  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
//                                                                 long bkey,
//                                                                 ElementFlagFilter eFlagFilter,
//                                                                 Transcoder<T> tc) {
//    return this.getClient().asyncBopGet(key, bkey, eFlagFilter, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long bkey,
                                                                 ElementFlagFilter eFlagFilter,
                                                                 boolean withDelete,
                                                                 boolean dropIfEmpty,
                                                                 Transcoder<T> tc) {
    return this.getClient().asyncBopGet(key, bkey, eFlagFilter, withDelete,
            dropIfEmpty, tc);
  }

//  @Override
//  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
//                                                                 long from, long to,
//                                                                 ElementFlagFilter eFlagFilter,
//                                                                 int offset, int count,
//                                                                 Transcoder<T> tc) {
//    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
//            count, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long from, long to,
                                                                 ElementFlagFilter eFlagFilter,
                                                                 int offset, int count,
                                                                 boolean withDelete,
                                                                 boolean dropIfEmpty,
                                                                 Transcoder<T> tc) {
    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
            count, withDelete, dropIfEmpty, tc);
  }

//  @Override
//  public CollectionFuture<Map<String, Object>> asyncMopGet(String key) {
//    return this.getClient().asyncMopGet(key);
//  }

  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           boolean withDelete,
                                                           boolean dropIfEmpty) {
    return this.getClient().asyncMopGet(key, withDelete, dropIfEmpty);
  }

//  @Override
//  public CollectionFuture<Map<String, Object>> asyncMopGet(String key, String mkey) {
//    return this.getClient().asyncMopGet(key, mkey);
//  }

  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key, String mkey,
                                                           boolean withDelete,
                                                           boolean dropIfEmpty) {
    return this.getClient().asyncMopGet(key, mkey, withDelete, dropIfEmpty);
  }

//  @Override
//  public CollectionFuture<Map<String, Object>> asyncMopGet(String key, List<String> mkeyList) {
//    return this.getClient().asyncMopGet(key, mkeyList);
//  }

  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key, List<String> mkeyList,
                                                           boolean withDelete,
                                                           boolean dropIfEmpty) {
    return this.getClient().asyncMopGet(key, mkeyList, withDelete, dropIfEmpty);
  }

//  @Override
//  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, Transcoder<T> tc) {
//    return this.getClient().asyncMopGet(key, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          boolean withDelete,
                                                          boolean dropIfEmpty,
                                                          Transcoder<T> tc) {
    return this.getClient().asyncMopGet(key, withDelete, dropIfEmpty, tc);
  }

//  @Override
//  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, String mkey,
//                                                          Transcoder<T> tc) {
//    return this.getClient().asyncMopGet(key, mkey, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, String mkey,
                                                          boolean withDelete,
                                                          boolean dropIfEmpty,
                                                          Transcoder<T> tc) {
    return this.getClient().asyncMopGet(key, mkey, withDelete, dropIfEmpty, tc);
  }

//  @Override
//  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, List<String> mkeyList,
//                                                          Transcoder<T> tc) {
//    return this.getClient().asyncMopGet(key, mkeyList, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, List<String> mkeyList,
                                                          boolean withDelete,
                                                          boolean dropIfEmpty,
                                                          Transcoder<T> tc) {
    return this.getClient().asyncMopGet(key, mkeyList, withDelete, dropIfEmpty, tc);
  }

//  @Override
//  public CollectionFuture<List<Object>> asyncLopGet(String key, int index) {
//    return this.getClient()
//            .asyncLopGet(key, index);
//  }

  @Override
  public CollectionFuture<List<Object>> asyncLopGet(String key, int index,
                                                    boolean withDelete,
                                                    boolean dropIfEmpty) {
    return this.getClient()
            .asyncLopGet(key, index, withDelete, dropIfEmpty);
  }

//  @Override
//  public CollectionFuture<List<Object>> asyncLopGet(String key, int from, int to) {
//    return this.getClient().asyncLopGet(key, from, to);
//  }

  @Override
  public CollectionFuture<List<Object>> asyncLopGet(String key,
                                                    int from, int to,
                                                    boolean withDelete,
                                                    boolean dropIfEmpty) {
    return this.getClient().asyncLopGet(key, from, to, withDelete,
            dropIfEmpty);
  }

//  @Override
//  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int index, Transcoder<T> tc) {
//    return this.getClient().asyncLopGet(key, index, tc);
//  }

  @Override
  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int index,
                                                   boolean withDelete,
                                                   boolean dropIfEmpty,
                                                   Transcoder<T> tc) {
    return this.getClient().asyncLopGet(key, index, withDelete,
            dropIfEmpty, tc);
  }

//  @Override
//  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int from, int to,
//  Transcoder<T> tc) {
//    return this.getClient().asyncLopGet(key, from, to, tc);
//  }

  @Override
  public <T> CollectionFuture<List<T>> asyncLopGet(String key,
                                                   int from, int to,
                                                   boolean withDelete,
                                                   boolean dropIfEmpty,
                                                   Transcoder<T> tc) {
    return this.getClient().asyncLopGet(key, from, to, withDelete,
            dropIfEmpty, tc);
  }

//  @Override
//  public CollectionFuture<Set<Object>> asyncSopGet(String key, int count) {
//    return this.getClient()
//            .asyncSopGet(key, count);
//  }

  @Override
  public CollectionFuture<Set<Object>> asyncSopGet(String key,
                                                   int count,
                                                   boolean withDelete,
                                                   boolean dropIfEmpty) {
    return this.getClient()
            .asyncSopGet(key, count, withDelete, dropIfEmpty);
  }

//  @Override
//  public <T> CollectionFuture<Set<T>> asyncSopGet(String key, int count, Transcoder<T> tc) {
//    return this.getClient().asyncSopGet(key, count, tc);
//  }

  @Override
  public <T> CollectionFuture<Set<T>> asyncSopGet(String key,
                                                  int count,
                                                  boolean withDelete,
                                                  boolean dropIfEmpty,
                                                  Transcoder<T> tc) {
    return this.getClient().asyncSopGet(key, count, withDelete,
            dropIfEmpty, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key, long bkey,
                                                  ElementFlagFilter eFlagFilter,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncBopDelete(key, bkey, eFlagFilter,
            dropIfEmpty);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key,
                                                  long from, long to,
                                                  ElementFlagFilter eFlagFilter,
                                                  int count,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncBopDelete(key, from, to, eFlagFilter,
            count, dropIfEmpty);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopDelete(String key,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncMopDelete(key, dropIfEmpty);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopDelete(String key, String mkey,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncMopDelete(key, mkey, dropIfEmpty);
  }

//  @Override
//  public CollectionFuture<Boolean> asyncMopDelete(String key, List<String> mkeyList,
//                                                  boolean dropIfEmpty) {
//    return this.getClient().asyncMopDelete(key, mkeyList, dropIfEmpty);
//  }

  @Override
  public CollectionFuture<Boolean> asyncLopDelete(String key, int index,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncLopDelete(key, index, dropIfEmpty);
  }

  @Override
  public CollectionFuture<Boolean> asyncLopDelete(String key, int from,
                                                  int to, boolean dropIfEmpty) {
    return this.getClient().asyncLopDelete(key, from, to, dropIfEmpty);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopDelete(String key, Object value,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncSopDelete(key, value, dropIfEmpty);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncSopDelete(String key, T value,
                                                      boolean dropIfEmpty, Transcoder<T> tc) {
    return this.getClient().asyncSopDelete(key, value, dropIfEmpty, tc);
  }

  @Override
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        long from, long to,
                                                        ElementFlagFilter eFlagFilter) {
    return this.getClient()
            .asyncBopGetItemCount(key, from, to, eFlagFilter);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                  byte[] eFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopInsert(key, bkey, eFlag, value,
            attributesForCreate);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                  Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncMopInsert(key, mkey, value,
            attributesForCreate);
  }

  @Override
  public CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                  Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncLopInsert(key, index, value,
            attributesForCreate);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopInsert(String key, Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncSopInsert(key, value, attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                      byte[] eFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncBopInsert(key, bkey, eFlag, value,
            attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                      T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncMopInsert(key, mkey, value,
            attributesForCreate, tc);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                      T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncLopInsert(key, index, value,
            attributesForCreate, tc);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncSopInsert(String key, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncSopInsert(key, value, attributesForCreate,
            tc);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, Object> elements,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopPipedInsertBulk(key, elements,
            attributesForCreate);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, Object> elements,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncMopPipedInsertBulk(key, elements,
            attributesForCreate);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<Object> valueList,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncLopPipedInsertBulk(key, index, valueList,
            attributesForCreate);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<Object> valueList,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncSopPipedInsertBulk(key, valueList,
            attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, T> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncBopPipedInsertBulk(key, elements,
            attributesForCreate, tc);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, T> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncMopPipedInsertBulk(key, elements,
            attributesForCreate, tc);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<T> valueList,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncLopPipedInsertBulk(key, index, valueList,
            attributesForCreate, tc);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<T> valueList,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncSopPipedInsertBulk(key, valueList,
            attributesForCreate, tc);
  }

  @Override
  public OperationFuture<Boolean> flush(String prefix) {
    return this.getClient().flush(prefix);
  }

  @Override
  public OperationFuture<Boolean> flush(String prefix, int delay) {
    return this.getClient().flush(prefix, delay);
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    return this.getClient().asyncBopSortMergeGet(keyList, from, to,
            eFlagFilter, offset, count);
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int count, SMGetMode smgetMode) {
    return this.getClient().asyncBopSortMergeGet(keyList, from, to,
            eFlagFilter, count, smgetMode);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                  byte[] elementFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopUpsert(key, bkey, elementFlag, value,
            attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                      byte[] elementFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncBopUpsert(key, bkey, elementFlag, value,
            attributesForCreate, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopUpsert(String key, String mkey, Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncMopUpsert(key, mkey, value, attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncMopUpsert(String key, String mkey, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncMopUpsert(key, mkey, value, attributesForCreate, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopInsert(String key, byte[] bkey,
                                                  byte[] eFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopInsert(key, bkey, eFlag, value,
            attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key,
                                                      byte[] bkey, byte[] eFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncBopInsert(key, bkey, eFlag, value,
            attributesForCreate, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key, byte[] from,
                                                  byte[] to, ElementFlagFilter eFlagFilter,
                                                  int count,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncBopDelete(key, from, to, eFlagFilter,
            count, dropIfEmpty);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key, byte[] bkey,
                                                  ElementFlagFilter eFlagFilter,
                                                  boolean dropIfEmpty) {
    return this.getClient().asyncBopDelete(key, bkey, eFlagFilter,
            dropIfEmpty);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpsert(String key, byte[] bkey,
                                                  byte[] elementFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopUpsert(key, bkey, elementFlag, value,
            attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key,
                                                      byte[] bkey, byte[] elementFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncBopUpsert(key, bkey, elementFlag, value,
            attributesForCreate, tc);
  }

  @Override
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        byte[] from, byte[] to,
                                                        ElementFlagFilter eFlagFilter) {
    return this.getClient()
            .asyncBopGetItemCount(key, from, to, eFlagFilter);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                  ElementFlagUpdate eFlagUpdate, Object value) {
    return this.getClient().asyncBopUpdate(key, bkey, eFlagUpdate, value);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                      ElementFlagUpdate eFlagUpdate, T value,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncBopUpdate(key, bkey, eFlagUpdate, value,
            tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpdate(String key, byte[] bkey,
                                                  ElementFlagUpdate eFlagUpdate, Object value) {
    return this.getClient().asyncBopUpdate(key, bkey, eFlagUpdate, value);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key,
                                                      byte[] bkey, ElementFlagUpdate eFlagUpdate,
                                                      T value,
                                                      Transcoder<T> tc) {
    return this.getClient().asyncBopUpdate(key, bkey, eFlagUpdate, value,
            tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                  Object value) {
    return this.getClient().asyncMopUpdate(key, mkey, value);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                      T value, Transcoder<T> tc) {
    return this.getClient().asyncMopUpdate(key, mkey, value, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<Object>> elements) {
    return this.getClient().asyncBopPipedUpdateBulk(key, elements);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<T>> elements, Transcoder<T> tc) {
    return this.getClient().asyncBopPipedUpdateBulk(key, elements, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, Object> elements) {
    return this.getClient().asyncMopPipedUpdateBulk(key, elements);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, T> elements, Transcoder<T> tc) {
    return this.getClient().asyncMopPipedUpdateBulk(key, elements, tc);
  }

  @Override
  public CollectionFuture<Map<Object, Boolean>> asyncSopPipedExistBulk(
          String key, List<Object> values) {
    return this.getClient().asyncSopPipedExistBulk(key, values);
  }

  @Override
  public <T> CollectionFuture<Map<T, Boolean>> asyncSopPipedExistBulk(
          String key, List<T> values, Transcoder<T> tc) {
    return this.getClient().asyncSopPipedExistBulk(key, values, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<Object>> elements,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopPipedInsertBulk(key, elements,
            attributesForCreate);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<T>> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncBopPipedInsertBulk(key, elements,
            attributesForCreate, tc);
  }

//  @Override
//  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
//          String key, byte[] bkey, ElementFlagFilter eFlagFilter) {
//    return this.getClient().asyncBopGet(key, bkey, eFlagFilter);
//  }

  @Override
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter,
          boolean withDelete, boolean dropIfEmpty) {
    return this.getClient().asyncBopGet(key, bkey, eFlagFilter, withDelete,
            dropIfEmpty);
  }

//  @Override
//  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
//          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
//          int offset, int count) {
//    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
//            count);
//  }

  @Override
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
          int offset, int count, boolean withDelete, boolean dropIfEmpty) {
    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
            count, withDelete, dropIfEmpty);
  }

//  @Override
//  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
//          String key, byte[] bkey, ElementFlagFilter eFlagFilter, Transcoder<T> tc) {
//    return this.getClient().asyncBopGet(key, bkey, eFlagFilter, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter,
          boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    return this.getClient().asyncBopGet(key, bkey, eFlagFilter, withDelete,
            dropIfEmpty, tc);
  }

//  @Override
//  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
//          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
//          int offset, int count, Transcoder<T> tc) {
//    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
//            count, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
          int offset, int count, boolean withDelete, boolean dropIfEmpty,
          Transcoder<T> tc) {
    return this.getClient().asyncBopGet(key, from, to, eFlagFilter, offset,
            count, withDelete, dropIfEmpty, tc);
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    return this.getClient().asyncBopSortMergeGet(keyList, from, to,
            eFlagFilter, offset, count);
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int count, SMGetMode smgetMode) {
    return this.getClient().asyncBopSortMergeGet(keyList, from, to,
            eFlagFilter, count, smgetMode);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopInsertBulk(keyList, bkey, eFlag, value,
            attributesForCreate);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, byte[] bkey, byte[] eFlag, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    return this.getClient().asyncBopInsertBulk(keyList, bkey, eFlag, value,
            attributesForCreate, tc);
  }

  @Override
  public CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>>
      asyncBopGetBulk(List<String> keyList, byte[] from, byte[] to,
                      ElementFlagFilter eFlagFilter, int offset, int count) {
    return this.getClient().asyncBopGetBulk(keyList, from, to, eFlagFilter,
            offset, count);
  }

  @Override
  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, T>>> asyncBopGetBulk(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count,
          Transcoder<T> tc) {
    return this.getClient().asyncBopGetBulk(keyList, from, to, eFlagFilter,
            offset, count, tc);
  }

  @Override
  public CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    return this.getClient().asyncBopGetBulk(keyList, from, to, eFlagFilter,
            offset, count);
  }

  @Override
  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, T>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count,
          Transcoder<T> tc) {
    return this.getClient().asyncBopGetBulk(keyList, from, to, eFlagFilter,
            offset, count, tc);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, long bkey, int by) {
    return this.getClient().asyncBopIncr(key, bkey, by);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] bkey, int by) {
    return this.getClient().asyncBopIncr(key, bkey, by);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, long bkey,
                                             int by, long initial, byte[] eFlag) {
    return this.getClient().asyncBopIncr(key, bkey, by, initial, eFlag);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] bkey,
                                             int by, long initial, byte[] eFlag) {
    return this.getClient().asyncBopIncr(key, bkey, by, initial, eFlag);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, long bkey, int by) {
    return this.getClient().asyncBopDecr(key, bkey, by);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] bkey, int by) {
    return this.getClient().asyncBopDecr(key, bkey, by);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, long bkey,
                                             int by, long initial, byte[] eFlag) {
    return this.getClient().asyncBopDecr(key, bkey, by, initial, eFlag);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] bkey,
                                             int by, long initial, byte[] eFlag) {
    return this.getClient().asyncBopDecr(key, bkey, by, initial, eFlag);
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int pos) {
    return this.getClient().asyncBopGetByPosition(key, order, pos);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int pos, Transcoder<T> tc) {
    return this.getClient().asyncBopGetByPosition(key, order, pos, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int from, int to) {
    return this.getClient().asyncBopGetByPosition(key, order, from, to);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int from, int to, Transcoder<T> tc) {
    return this.getClient().asyncBopGetByPosition(key, order, from, to, tc);
  }

  @Override
  public CollectionFuture<Integer> asyncBopFindPosition(String key,
                                                        long longBKey, BTreeOrder order) {
    return this.getClient().asyncBopFindPosition(key, longBKey, order);
  }

  @Override
  public CollectionFuture<Integer> asyncBopFindPosition(String key,
                                                        byte[] byteArrayBKey, BTreeOrder order) {
    return this.getClient().asyncBopFindPosition(key, byteArrayBKey, order);
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopFindPositionWithGet(
          String key, long longBKey, BTreeOrder order, int count) {
    return this.getClient().asyncBopFindPositionWithGet(key, longBKey, order, count);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, long longBKey, BTreeOrder order, int count, Transcoder<T> tc) {
    return this.getClient().asyncBopFindPositionWithGet(key, longBKey, order, count, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopFindPositionWithGet(
          String key, byte[] byteArrayBKey, BTreeOrder order, int count) {
    return this.getClient().asyncBopFindPositionWithGet(key, byteArrayBKey, order, count);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, byte[] byteArrayBKey, BTreeOrder order, int count, Transcoder<T> tc) {
    return this.getClient().asyncBopFindPositionWithGet(key, byteArrayBKey, order, count, tc);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopInsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    return this.getClient().asyncBopInsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopInsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    return this.getClient().asyncBopInsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopUpsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    return this.getClient().asyncBopUpsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    return this.getClient().asyncBopUpsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    return this.getClient().asyncBopUpsertAndGetTrimmed(key, bkey, eFlag,
            value, attributesForCreate, transcoder);
  }

}
