/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2020 JaM2in Co., Ltd.
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

import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.BasicThreadFactory;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    BulkService.Task<Map<String, CollectionOperationStatus>> task =
            new BulkService.Task<Map<String, CollectionOperationStatus>>(w);
    executor.submit(task);
    return task;
  }

  <T> Future<Map<String, CollectionOperationStatus>> setBulk(
          Map<String, T> o, int exp, Transcoder<T> transcoder,
          ArcusClient[] client) {
    assert !executor.isShutdown() : "Pool has already shut down.";
    BulkSetWorker<T> w = new BulkSetWorker<T>(o, exp, transcoder, client,
            singleOpTimeout);
    BulkService.Task<Map<String, CollectionOperationStatus>> task =
            new BulkService.Task<Map<String, CollectionOperationStatus>>(w);
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
    private final BulkWorker<T> worker;

    public Task(Callable<T> callable) {
      super(callable);
      this.worker = (BulkWorker<T>) callable;
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
          Callable<T> {

    protected final ArcusClient[] clientList;
    protected final ArrayList<Future<Boolean>> future;
    protected final long operationTimeout;
    protected final AtomicBoolean isRunnable = new AtomicBoolean(true);
    protected T errorList = null;

    protected final int keyCount;

    public BulkWorker(Collection keys, long timeout, ArcusClient[] clientList) {
      this.future = new ArrayList<Future<Boolean>>(keys.size());
      this.operationTimeout = timeout;
      this.clientList = clientList;

      keyCount = keys.size();
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
      return ret;
    }

    protected boolean isRunnable() {
      return isRunnable.get() && !Thread.currentThread().isInterrupted();
    }

    protected abstract Future<Boolean> processItem(int index);

    protected abstract void awaitProcessResult(int index);

    public T call() throws Exception {
      int numActiveOperations = 0;
      int posResponseReceived = 0;

      for (int pos = 0; isRunnable() && pos < keyCount; pos++) {
        try {
          if (isRunnable()) {
            future.add(pos, processItem(pos));
          }
        } catch (IllegalStateException e) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          } else {
            throw e;
          }
        }
        numActiveOperations++;

        if (numActiveOperations >= DEFAULT_LOOP_LIMIT) {
          awaitProcessResult(posResponseReceived);
          posResponseReceived++;
          numActiveOperations--;
        }
      }

      while (numActiveOperations > 0) {
        awaitProcessResult(posResponseReceived);
        posResponseReceived++;
        numActiveOperations--;
      }
      return errorList;
    }
  }

  /**
   * Bulk set operation worker
   */
  private static class BulkSetWorker<T> extends BulkWorker<Map<String, CollectionOperationStatus>> {
    private final List<String> keys;
    private final int exp;
    private final int cntCos;
    private List<CachedData> cos;

    public BulkSetWorker(List<String> keys, int exp, T value,
                         Transcoder<T> transcoder, ArcusClient[] clientList,
                         long timeout) {
      super(keys, timeout, clientList);
      this.keys = keys;
      this.exp = exp;
      this.cos = new ArrayList<CachedData>();
      this.cos.add(transcoder.encode(value));
      this.cntCos = 1;
      this.errorList = new HashMap<String, CollectionOperationStatus>();
    }

    public BulkSetWorker(Map<String, T> o, int exp,
                         Transcoder<T> transcoder, ArcusClient[] clientList, long timeout) {

      super(o.keySet(), timeout, clientList);

      this.keys = new ArrayList<String>(o.keySet());
      this.exp = exp;

      this.cos = new ArrayList<CachedData>();
      for (String key : keys) {
        this.cos.add(transcoder.encode(o.get(key)));
      }
      this.cntCos = this.cos.size();
      this.errorList = new HashMap<String, CollectionOperationStatus>();
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
        boolean success = future.get(index).get(operationTimeout,
                TimeUnit.MILLISECONDS);
        if (!success) {
          errorList.put(
                  keys.get(index),
                  new CollectionOperationStatus(false, String
                          .valueOf(success), CollectionResponse.END));
        }
      } catch (Exception e) {
        future.get(index).cancel(true);
        errorList.put(keys.get(index), new CollectionOperationStatus(
                false, e.getMessage(), CollectionResponse.EXCEPTION));
      }
    }
  }
}
