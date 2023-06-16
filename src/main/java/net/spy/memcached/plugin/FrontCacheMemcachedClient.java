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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;
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

}
