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
package net.spy.memcached.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.plugin.LocalCacheManager;

/**
 * Future for handling results from bulk gets.
 *
 * Not intended for general use.
 *
 * @param <T> types of objects returned from the GET
 */
public class BulkGetFuture<T> implements BulkFuture<Map<String, T>> {
	private final Map<String, Future<T>> rvMap;
	private final Collection<Operation> ops;
	private final CountDownLatch latch;
	private boolean cancelled = false;
	private boolean timeout = false;

	// FIXME right position?
	private LocalCacheManager localCacheManager;

	public BulkGetFuture(Map<String, Future<T>> m,
			Collection<Operation> getOps, CountDownLatch l) {
		super();
		rvMap = m;
		ops = getOps;
		latch = l;
	}

	public BulkGetFuture(Map<String, Future<T>> m,
			Collection<Operation> getOps, CountDownLatch l,
			LocalCacheManager lcm) {
		super();
		rvMap = m;
		ops = getOps;
		latch = l;
		localCacheManager = lcm;
	}

	public boolean cancel(boolean ign) {
		boolean rv = false;
		for (Operation op : ops) {
			rv |= op.getState() == OperationState.WRITING;
			op.cancel("by application.");
		}
		for (Future<T> v : rvMap.values()) {
			v.cancel(ign);
		}
		cancelled = true;
		return rv;
	}

	public Map<String, T> get() throws InterruptedException, ExecutionException {
		try {
			return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new RuntimeException("Timed out waiting forever", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.spy.memcached.internal.BulkFuture#getSome(long,
	 * java.util.concurrent.TimeUnit)
	 */
	public Map<String, T> getSome(long to, TimeUnit unit)
			throws InterruptedException, ExecutionException {
		Collection<Operation> timedoutOps = new HashSet<Operation>();
		Map<String, T> ret = internalGet(to, unit, timedoutOps);
		if (timedoutOps.size() > 0) {
			timeout = true;
			LoggerFactory.getLogger(getClass()).warn(
					new CheckedOperationTimeoutException(
							"Operation timed out: ", timedoutOps).getMessage());
		}
		return ret;

	}

	/*
	 * get all or nothing: timeout exception is thrown if all the data could not
	 * be retrieved
	 * 
	 * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
	 */
	public Map<String, T> get(long to, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		Collection<Operation> timedoutOps = new HashSet<Operation>();
		Map<String, T> ret = internalGet(to, unit, timedoutOps);
		if (timedoutOps.size() > 0) {
			this.timeout = true;
			throw new CheckedOperationTimeoutException("Operation timed out.",
					timedoutOps);
		}
		return ret;
	}

	/**
	 * refactored code common to both get(long, TimeUnit) and getSome(long,
	 * TimeUnit)
	 */
	private Map<String, T> internalGet(long to, TimeUnit unit,
			Collection<Operation> timedoutOps) throws InterruptedException,
			ExecutionException {
		if (!latch.await(to, unit)) {
			for (Operation op : ops) {
				if (op.getState() != OperationState.COMPLETE) {
					MemcachedConnection.opTimedOut(op);
					timedoutOps.add(op);
				} else {
					MemcachedConnection.opSucceeded(op);
				}
			}
		}
		for (Operation op : ops) {
			if (op.isCancelled()) {
				throw new ExecutionException(new RuntimeException(op.getCancelCause()));
			}
			if (op.hasErrored()) {
				throw new ExecutionException(op.getException());
			}
		}
		Map<String, T> m = new HashMap<String, T>();
		for (Map.Entry<String, Future<T>> me : rvMap.entrySet()) {
			String key = me.getKey();
			Future<T> future = me.getValue();
			T value = future.get();

			// put the key into the result map.
			m.put(key, value);

			// cache the key locally
			if (localCacheManager != null) {
				// iff it is from the remote cache.
				if (!(future instanceof LocalCacheManager.Task)) {
					localCacheManager.put(key, value);
				}
			}
		}
		return m;
	}

	public boolean isCancelled() {
		return cancelled;
	}

	public boolean isDone() {
		return latch.getCount() == 0;
	}

	/*
	 * set to true if timeout was reached.
	 * 
	 * @see net.spy.memcached.internal.BulkFuture#isTimeout()
	 */
	public boolean isTimeout() {
		return timeout;
	}
}
