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
import java.util.concurrent.Future;

import net.sf.ehcache.Element;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
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
   * @param addrs the socket addresses for the memcached servers
   * @throws IOException if connections cannot be established
   */
  public FrontCacheMemcachedClient(ConnectionFactory cf,
                                   List<InetSocketAddress> addrs) throws IOException {
    super(cf, addrs);

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
   * Get the value of the key.
   * Check the local cache first. If the key is not found, send the command to the server.
   *
   * @param key the key to fetch
   * @param tc  the transcoder to serialize and unserialize value
   * @return a future that will hold the value of the key
   */
  @Override
  public <T> Future<T> asyncGet(final String key, final Transcoder<T> tc) {
    Element frontElement = null;

    if (localCacheManager != null) {
      frontElement = localCacheManager.getElement(key);
    }

    if (frontElement == null) {
      return super.asyncGet(key, tc);
    } else {
      return new FrontCacheGetFuture<T>(frontElement);
    }
  }

  /**
   * Delete the key.
   * Delete the key from the local cache before sending the command to the server.
   *
   * @param key the key to delete
   * @return a future that will hold success/error status of the operation
   */
  @Override
  public Future<Boolean> delete(String key) {
    if (localCacheManager != null) {
      localCacheManager.delete(key);
    }
    return super.delete(key);
  }

}
