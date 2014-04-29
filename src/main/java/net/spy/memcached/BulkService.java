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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.BasicThreadFactory;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;

class BulkService extends SpyObject {

	private static int DEFAULT_LOOP_LIMIT;
	private final ExecutorService executor;
	private final long singleOpTimeout;

	BulkService(int loopLimit, int threadCount, long singleOpTimeout) {
		this.executor = new ThreadPoolExecutor(threadCount, threadCount, 60L,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
				new BasicThreadFactory("bulk-service", true),
				new ThreadPoolExecutor.AbortPolicy());
		BulkService.DEFAULT_LOOP_LIMIT = loopLimit;
		this.singleOpTimeout = singleOpTimeout;
	}

	<T> Future<Map<String, CollectionOperationStatus>> setBulk(
			List<String> keys, int exp, T value, Transcoder<T> transcoder,
			ArcusClient[] client) {
		assert !executor.isShutdown() : "Pool has already shut down.";
		BulkSetWorker<T> w = new BulkSetWorker<T>(keys, exp, value, transcoder,
				client, singleOpTimeout);
		BulkService.Task<Map<String, CollectionOperationStatus>> task = new BulkService.Task<Map<String, CollectionOperationStatus>>(
				w);
		executor.submit(task);
		return task;
	}

	<T> Future<Map<String, CollectionOperationStatus>> setBulk(
			Map<String, T> o, int exp, Transcoder<T> transcoder,
			ArcusClient[] client) {
		assert !executor.isShutdown() : "Pool has already shut down.";
		BulkSetWorker<T> w = new BulkSetWorker<T>(o, exp, transcoder, client,
				singleOpTimeout);
		BulkService.Task<Map<String, CollectionOperationStatus>> task = new BulkService.Task<Map<String, CollectionOperationStatus>>(
				w);
		executor.submit(task);
		return task;
	}

	void shutdown() {
		try {
			executor.shutdown();
		} catch (Exception e) {
			getLogger().warn("exception while shutting down bulk set service.",
					e);
		}
	}

	private static class Task<T> extends FutureTask<T> {
		private final BulkWorker worker;

		public Task(BulkWorker w) {
			super((Callable) w);
			this.worker = w;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return worker.cancel() && super.cancel(mayInterruptIfRunning);
		}
	}

	/**
	 * Bulk operation worker
	 */
	private abstract static class BulkWorker<T> extends SpyObject implements
			Callable<Map<String, CollectionOperationStatus>> {

		protected final ArcusClient[] clientList;
		protected final Future<Boolean>[] future;
		protected final long operationTimeout;
		protected final AtomicBoolean isRunnable = new AtomicBoolean(true);
		protected final Map<String, CollectionOperationStatus> errorList;

		protected final int totalCount;
		protected final int fromIndex;
		protected final int toIndex;

		public BulkWorker(int keySize, long timeout, Transcoder<T> tc,
				ArcusClient[] clientList) {
			this.future = new Future[keySize];
			this.operationTimeout = timeout;
			this.clientList = getOptimalClients(clientList);
			this.errorList = new HashMap<String, CollectionOperationStatus>();

			fromIndex = 0;
			toIndex = keySize - 1;
			totalCount = toIndex - fromIndex + 1;
		}

		public boolean cancel() {
			if (!isRunnable()) {
				return false;
			}

			isRunnable.set(false);

			boolean ret = true;
			
			for (Future<Boolean> f : future) {
				if (f == null) {
					continue;
				}
				if (f.isCancelled() || f.isDone()) {
					continue;
				}
				ret &= f.cancel(true);
								
				if (getLogger().isDebugEnabled()) {
					getLogger().debug("Cancel the future. " + f);
				}
			}
			getLogger().info("Cancel, bulk set worker.");
			return ret;
		}

		private ArcusClient[] getOptimalClients(ArcusClient[] clientList) {
			return clientList;
		}

		protected boolean isRunnable() {
			return isRunnable.get() && !Thread.currentThread().isInterrupted();
		}

		protected void setErrorOpStatus(String key, int indexOfFuture) {
			errorList.put(key,
					((CollectionFuture<Boolean>) future[indexOfFuture])
							.getOperationStatus());
		}

		public abstract Future<Boolean> processItem(int index);

		public abstract void awaitProcessResult(int index);

		public abstract boolean isDataExists();

		public Map<String, CollectionOperationStatus> call() throws Exception {
			if (!isDataExists()) {
				return errorList;
			}

			for (int pos = fromIndex; isRunnable() && pos <= toIndex; pos++) {
				if ((pos - fromIndex) > 0
						&& (pos - fromIndex) % DEFAULT_LOOP_LIMIT == 0) {
					for (int i = pos - DEFAULT_LOOP_LIMIT; isRunnable()
							&& i < pos; i++) {
						awaitProcessResult(i);
					}
				}
				try {
					if (isRunnable()) {
						future[pos] = processItem(pos);
					}
				} catch (IllegalStateException e) {
					if (Thread.currentThread().isInterrupted()) {
						break;
					} else {
						throw e;
					}
				}
			}
			for (int i = toIndex
					- (totalCount % DEFAULT_LOOP_LIMIT == 0 ? DEFAULT_LOOP_LIMIT
							: totalCount % DEFAULT_LOOP_LIMIT) + 1; isRunnable()
					&& i <= toIndex; i++) {
				awaitProcessResult(i);
			}
			return errorList;
		}
	}

	/**
	 * Bulk set operation worker
	 */
	private static class BulkSetWorker<T> extends BulkWorker<T> {
		private final List<String> keys;
		private final int exp;
		private final int cntCos;
		private List<CachedData> cos;

		public BulkSetWorker(List<String> keys, int exp, T value,
				Transcoder<T> transcoder, ArcusClient[] clientList,
				long timeout) {
			super(keys.size(), timeout, transcoder, clientList);
			this.keys = keys;
			this.exp = exp;
			this.cos = new ArrayList<CachedData>();
			this.cos.add(transcoder.encode(value));
			this.cntCos = 1;
		}

		public BulkSetWorker(Map<String, T> o, int exp,
				Transcoder<T> transcoder, ArcusClient[] clientList, long timeout) {

			super(o.keySet().size(), timeout, transcoder, clientList);

			this.keys = new ArrayList<String>(o.keySet());
			this.exp = exp;

			this.cos = new ArrayList<CachedData>();
			for (String key : keys) {
				this.cos.add(transcoder.encode(o.get(key)));
			}
			this.cntCos = this.cos.size();
		}

		@Override
		public Future<Boolean> processItem(int index) {
			return clientList[index % clientList.length].asyncStore(
					StoreType.set, keys.get(index), exp,
					(this.cntCos > 1 ? cos.get(index) : cos.get(0)));
		}

		@Override
		public void awaitProcessResult(int index) {
			try {
				boolean success = future[index].get(operationTimeout,
						TimeUnit.MILLISECONDS);
				if (!success) {
					errorList.put(
							keys.get(index),
							new CollectionOperationStatus(false, String
									.valueOf(success), CollectionResponse.END));
				}
			} catch (Exception e) {
				future[index].cancel(true);
				errorList.put(keys.get(index), new CollectionOperationStatus(
						false, e.getMessage(), CollectionResponse.EXCEPTION));
			}
		}

		@Override
		public boolean isDataExists() {
			return (keys != null && keys.size() > 0);
		}
	}
}
