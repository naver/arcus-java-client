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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Map;
import java.util.Iterator;
import java.util.Arrays;
import java.util.HashMap;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.internal.SingleElementInfiniteIterator;
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
      boolean copyOnRead = cf.getFrontCacheCopyOnRead();
      boolean copyOnWrite = cf.getFrontCacheCopyOnWrite();
      // TODO add an additional option
      // int timeToIdleSeconds = timeToLiveSeconds;

      localCacheManager = new LocalCacheManager(cacheName, maxElements,
              timeToLiveSeconds, copyOnRead, copyOnWrite);
    }
  }

  /**
   * Get with a single key and decode using the default transcoder.
   *
   * @param key the key to get
   * @return the result from the cache (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *                                   exceeded
   * @throws IllegalStateException     in the rare circumstance where queue
   *                                   is too full to accept any more requests
   */
  public Object get(String key) {
    return get(key, transcoder);
  }

  /**
   * Get with a single key.
   *
   * @param <T> Type of object to get.
   * @param key the key to get
   * @param tc  the transcoder to serialize and unserialize value
   * @return the result from the cache (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *                                   exceeded
   * @throws IllegalStateException     in the rare circumstance where queue
   *                                   is too full to accept any more requests
   */

  public <T> T get(String key, Transcoder<T> tc) {
    Future<T> future = asyncGet(key, tc);
    try {
      return future.get(operationTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      future.cancel(true);
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      future.cancel(true);
      throw new RuntimeException("Exception waiting for value", e);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new OperationTimeoutException(e);
    }
  }

  /**
   * Get the given key asynchronously and decode with the default
   * transcoder.
   *
   * @param key the key to fetch
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue
   *                               is too full to accept any more requests
   */
  public GetFuture<Object> asyncGet(final String key) {
    return asyncGet(key, transcoder);
  }

  /**
   * Get the value of the key.
   * Check the local cache first. If the key is not found, send the command to the server.
   *
   * @param key the key to fetch
   * @param tc  the transcoder to serialize and unserialize value
   * @return a future that will hold the value of the key
   */
  @Override
  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc) {
    if (localCacheManager == null) {
      return super.asyncGet(key, tc);
    }

    final T t = localCacheManager.get(key, tc);
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
    return new FrontCacheGetFuture<T>(localCacheManager, key, parent);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param keys the keys
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *                                   exceeded
   * @throws IllegalStateException     in the rare circumstance where queue
   *                                   is too full to accept any more requests
   */
  public Map<String, Object> getBulk(Collection<String> keys) {
    return getBulk(keys, transcoder);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param <T>
   * @param tc   the transcoder to serialize and unserialize value
   * @param keys the keys
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *                                   exceeded
   * @throws IllegalStateException     in the rare circumstance where queue
   *                                   is too full to accept any more requests
   */
  public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) {
    return getBulk(Arrays.asList(keys), tc);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param keys the keys
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *                                   exceeded
   * @throws IllegalStateException     in the rare circumstance where queue
   *                                   is too full to accept any more requests
   */
  public Map<String, Object> getBulk(String... keys) {
    return getBulk(Arrays.asList(keys), transcoder);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param <T>
   * @param keys the keys
   * @param tc   the transcoder to serialize and unserialize value
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *                                   exceeded
   * @throws IllegalStateException     in the rare circumstance where queue
   *                                   is too full to accept any more requests
   */
  public <T> Map<String, T> getBulk(Collection<String> keys,
                                    Transcoder<T> tc) {
    BulkFuture<Map<String, T>> future = asyncGetBulk(keys, tc);
    try {
      return future.get(operationTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      future.cancel(true);
      throw new RuntimeException("Interrupted getting bulk values", e);
    } catch (ExecutionException e) {
      future.cancel(true);
      throw new RuntimeException("Failed getting bulk values", e);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new OperationTimeoutException(e);
    }
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tc   the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue
   *                               is too full to accept any more requests
   */
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Transcoder<T> tc) {
    return asyncGetBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(tc));
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them
   * with the given transcoder.
   *
   * @param keys the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue
   *                               is too full to accept any more requests
   */
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
    return asyncGetBulk(keys, transcoder);
  }

  /**
   * Varargs wrapper for asynchronous bulk get.
   *
   * @param <T>
   * @param tc   the transcoder to serialize and unserialize value
   * @param keys one more keys to get
   * @return the future values of those keys
   * @throws IllegalStateException in the rare circumstance where queue
   *                               is too full to accept any more requests
   */
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc,
                                                     String... keys) {
    return asyncGetBulk(Arrays.asList(keys), tc);
  }

  /**
   * Varargs wrapper for asynchronous bulk get with the default transcoder.
   *
   * @param keys one more keys to get
   * @return the future values of those keys
   * @throws IllegalStateException in the rare circumstance where queue
   *                               is too full to accept any more requests
   */
  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
    return asyncGetBulk(Arrays.asList(keys), transcoder);
  }

  /**
   * Asynchronously gets (with CAS support) a bunch of objects from the cache.
   * If used with front cache, the front cache is checked first.
   * @param <T>
   * @param keys    the keys to request
   * @param tc_iter an iterator of transcoders to serialize and
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
                                                     Iterator<Transcoder<T>> tc_iter) {
    /*
    * Case 1. local cache is not used.
    * All data from Arcus server.
    * */
    if (localCacheManager == null) {
      return super.asyncGetBulk(keys, tc_iter);
    }
    /*
    * Case 2. local cache is used.
    * 1. Check the local cache first.
    * */
    final Map<String, T> frontCacheHit = new HashMap<String, T>();
    final Map<String, Transcoder<T>> frontCacheMiss =
            new HashMap<String, Transcoder<T>>();

    Iterator<String> keyIter = keys.iterator();
    while (keyIter.hasNext() && tc_iter.hasNext()) {
      String key = keyIter.next();
      Transcoder<T> tc = tc_iter.next();
      T value = localCacheManager.get(key, tc);
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

    return new FrontCacheBulkGetFuture<T>(localCacheManager, parent, frontCacheHit);
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
