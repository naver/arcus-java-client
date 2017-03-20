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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Local cache storage based on ehcache.
 */
public class LocalCacheManager {

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	protected Cache cache;
	protected String name;

	public LocalCacheManager() {
		this("DEFAULT_ARCUS_LOCAL_CACHE");
	}
	
	public LocalCacheManager(String name) {
		this.name = name;
		// create a undecorated Cache object.
		this.cache = CacheManager.getInstance().getCache(name);
	}
	
	public LocalCacheManager(String name, int max, int exptime, boolean copyOnRead, boolean copyOnWrite) {
		this.cache = CacheManager.getInstance().getCache(name);
		if (cache == null) {
			CacheConfiguration config =
							new CacheConfiguration(name, max)
							.copyOnRead(copyOnRead)
							.copyOnWrite(copyOnWrite)
							.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
							.eternal(false)
							.timeToLiveSeconds(exptime)
							.timeToIdleSeconds(exptime)
							.diskExpiryThreadIntervalSeconds(60)
							.persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE));
			this.cache = new Cache(config, null, null);
			CacheManager.getInstance().addCache(cache);
			
			if (logger.isInfoEnabled()) {
				logger.info("Arcus k/v local cache is enabled : %s", cache.toString());
			}
		}
	}
	
	public <T> T get(String key, Transcoder<T> tc) {
		if (cache == null) {
			return null;
		}
		
		try {
			Element element = cache.get(key);
			if(null != element) {
				if (logger.isDebugEnabled()) {
					logger.debug("ArcusFrontCache: local cache hit for %s", key);
				}
				@SuppressWarnings("unchecked") T ret = (T) element.getObjectValue();
				return ret;
			}
		} catch (Exception e) {
			logger.info("failed to get from the local cache : %s", e.getMessage());
			return null;
		}

		return null;
	}
	
	public <T> Future<T> asyncGet(final String key, final Transcoder<T> tc) {
		Task<T> task = new Task<T>(new Callable<T>() {
			public T call() throws Exception {
				return get(key, tc);
			}
		});
		return task;
	}

	public Element getElement(String key) {
		Element element = cache.get(key);
		if (logger.isDebugEnabled()) {
			if (null != element) {
				logger.debug("ArcusFrontCache: local cache hit for %s", key);
			}
		}
		return element;
	}
	
	public <T> boolean put(String k, T v) {
		if (v == null) {
			return false;
		}
		
		try {
			cache.put(new Element(k, v));
			return true;
		} catch (Exception e) {
			if (logger.isInfoEnabled()) {
				logger.info("failed to put to the local cache : %s", e.getMessage());
			}
			return false;
		}
	}
	
	public <T> boolean put(String k, Future<T> future, long timeout) {
		if (future == null) {
			return false;
		}

		try {
			T v = future.get(timeout, TimeUnit.MILLISECONDS);
			return put(k, v);
		} catch (Exception e) {
			logger.info("failed to put to the local cache : %s", e.getMessage());
			return false;
		}
	}
	
	public void delete(String k) {
		try {
			cache.remove(k);
		} catch (Exception e) {
			logger.info("failed to remove the locally cached item : %s", e.getMessage());
		}
	}
	
	public static class Task<T> extends FutureTask<T> {
		private final AtomicBoolean isRunning = new AtomicBoolean(false);

		public Task(Callable<T> callable) {
			super(callable);
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			this.run();
			return super.get();
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException,
				ExecutionException, TimeoutException {
			this.run();
			return super.get(timeout, unit);
		}

		@Override
		public void run() {
			if (this.isRunning.compareAndSet(false, true)) {
				super.run();
			}
		}
	}
	
	@Override
	public String toString() {
		return cache.toString();
	}
	
}
