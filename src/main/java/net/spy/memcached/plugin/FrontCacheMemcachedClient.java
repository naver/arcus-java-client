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
package net.spy.memcached.plugin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Front cache for some Arcus commands.
 * For now, it supports get commands.  The front cache stores the value from a get operation.
 * A subsequent get operation first checks the cache.  If the key is found in the cache,
 * it is returned from the front cache.  If not, the get command goes to the server as usual.
 *
 * Cache parameters (name, size, expiration time) are from ConnectionFactory.
 *
 * @see net.spy.memcached.ConnectionFactoryBuilder
 * @see net.spy.memcached.plugin.LocalCacheManager
 */
public class FrontCacheMemcachedClient extends MemcachedClient {

  protected LocalCacheManager localCacheManager = null;

  /**
   * Create the memcached client and the front cache.
   *
   * @param cf    the connection factory to configure connections for this client
   * @param name  client name
   * @param addrs the socket addresses for the memcached servers
   * @throws IOException if connections cannot be established
   */
  public FrontCacheMemcachedClient(ConnectionFactory cf,
                                   String name, List<InetSocketAddress> addrs) throws IOException {
    super(cf, name, addrs);

    if (cf.getMaxFrontCacheElements() > 0) {
      String cacheName = cf.getFrontCacheName();
      int maxElements = cf.getMaxFrontCacheElements();
      int timeToLiveSeconds = cf.getFrontCacheExpireTime();

      localCacheManager = new LocalCacheManager(cacheName, maxElements, timeToLiveSeconds);
    }
  }

  /**
   * Get the value of the key.
   * Check the local cache first. If the key is not found, send the command to the server.
   *
   * @param key the key to fetch
   * @param tc  the transcoder to serialize and deserialize value
   * @return a future that will hold the value of the key
   */
  @Override
  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc) {
    if (localCacheManager == null) {
      return super.asyncGet(key, tc);
    }

    final T t = localCacheManager.get(key);
    if (t != null) {
      return new GetFuture<T>(null, 0) {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return false;
        }
        @Override
        public boolean isCancelled() {
          return false;
        }
        @Override
        public boolean isDone() {
          return true;
        }
        @Override
        public T get() {
          return t;
        }
        @Override
        public T get(long timeout, TimeUnit unit) {
          return t;
        }
        @Override
        public OperationStatus getStatus() {
          return new OperationStatus(true, "END", StatusCode.SUCCESS);
        }
      };
    }
    GetFuture<T> parent = super.asyncGet(key, tc);
    return new FrontCacheGetFuture<>(localCacheManager, key, parent);
  }

  /**
   * Asynchronously gets (with CAS support) a bunch of objects from the cache.
   * If used with front cache, the front cache is checked first.
   * @param <T>
   * @param keys    the keys to request
   * @param tcIter an iterator of transcoders to serialize and
   *                unserialize values; the transcoders are matched with
   *                the keys in the same order.  The minimum of the key
   *                collection length and number of transcoders is used
   *                and no exception is thrown if they do not match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue
   *                               is too full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Iterator<Transcoder<T>> tcIter) {
    /*
    * Case 1. local cache is not used.
    * All data from Arcus server.
    * */
    if (localCacheManager == null) {
      return super.asyncGetBulk(keys, tcIter);
    }
    /*
    * Case 2. local cache is used.
    * 1. Check the local cache first.
    * */
    final Map<String, T> frontCacheHit = new HashMap<>();
    final Map<String, Transcoder<T>> frontCacheMiss =
            new HashMap<>();

    Iterator<String> keyIter = keys.iterator();
    while (keyIter.hasNext() && tcIter.hasNext()) {
      String key = keyIter.next();
      Transcoder<T> tc = tcIter.next();
      T value = localCacheManager.get(key);
      if (value != null) {
        frontCacheHit.put(key, value);
        continue;
      }
      frontCacheMiss.put(key, tc);
    }
    /*
    * 2. Send the cache miss keys to Arcus server.
    * */
    BulkGetFuture<T> parent = (BulkGetFuture<T>) super.asyncGetBulk(
            frontCacheMiss.keySet(), frontCacheMiss.values().iterator());

    return new FrontCacheBulkGetFuture<>(localCacheManager, parent, frontCacheHit);
  }

  /**
   * Delete the key.
   * Delete the key from the local cache before sending the command to the server.
   *
   * @param key the key to delete
   * @return a future that will hold success/error status of the operation
   */
  @Override
  public OperationFuture<Boolean> delete(String key) {
    if (localCacheManager != null) {
      localCacheManager.delete(key);
    }
    return super.delete(key);
  }

  public LocalCacheManager getLocalCacheManager() {
    return localCacheManager;
  }
}
