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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.internal.result.GetResult;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

/**
 * Future for handling results from bulk gets.
 *
 * Not intended for general use.
 *
 * @param <T> types of objects returned from the GET
 */
public class BulkGetFuture<T> implements BulkFuture<Map<String, T>> {
  private final Map<String, GetResult<T>> rvMap;
  private final Collection<Operation> ops;
  private final CountDownLatch latch;
  private final long timeout;
  private AtomicBoolean isTimeout = new AtomicBoolean(false);

  public BulkGetFuture(Map<String, GetResult<T>> rvMap, Collection<Operation> ops,
                       CountDownLatch latch, Long timeout) {
    super();
    this.rvMap = rvMap;
    this.ops = ops;
    this.latch = latch;
    this.timeout = timeout;
  }

  public BulkGetFuture(BulkGetFuture<T> other) {
    super();
    rvMap = other.rvMap;
    ops = other.ops;
    latch = other.latch;
    timeout = other.timeout;
    isTimeout = other.isTimeout;
  }

  @Override
  public boolean cancel(boolean ign) {
    boolean rv = false;
    for (Operation op : ops) {
      rv |= op.cancel("by application.");
    }
    return rv;
  }

  @Override
  public Map<String, T> get() throws InterruptedException, ExecutionException {
    try {
      return get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException(e);
    }
  }

  @Override
  public Map<String, T> getSome(long duration, TimeUnit unit)
          throws InterruptedException, ExecutionException {

    try {
      return internalGet(duration, unit, false);
    } catch (TimeoutException e) {
      throw new AssertionError("Something went wrong...", e);
    }
  }

  /*
   * get all or nothing: timeout exception is thrown if all the data could not
   * be retrieved
   *
   * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public Map<String, T> get(long duration, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {

    return internalGet(duration, unit, true);
  }

  @Override
  public boolean isCancelled() {
    for (Operation op : ops) {
      if (op.isCancelled()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isDone() {
    return latch.getCount() == 0;
  }

  @Override
  public int getOpCount() {
    return ops.size();
  }

  /**
   * refactored code common to both get(long, TimeUnit) and getSome(long,
   * TimeUnit)
   */
  private Map<String, T> internalGet(long to, TimeUnit unit, boolean throwOnTimeout)
           throws InterruptedException, ExecutionException, TimeoutException {

    long beforeAwait = System.currentTimeMillis();
    if (!latch.await(to, unit)) {
      Collection<Operation> timedOutOps = new HashSet<>();
      for (Operation op : ops) {
        if (op.getState() != OperationState.COMPLETE) {
          timedOutOps.add(op);
        } else {
          MemcachedConnection.opSucceeded(op);
        }
      }
      if (!timedOutOps.isEmpty()) {
        MemcachedConnection.opsTimedOut(timedOutOps);
        isTimeout.set(true);

        long elapsed = System.currentTimeMillis() - beforeAwait;
        TimeoutException e = new CheckedOperationTimeoutException(to, unit, elapsed, timedOutOps);
        if (throwOnTimeout) {
          throw e;
        }
        LoggerFactory.getLogger(getClass()).warn(e.getMessage());
      }
    } else {
      MemcachedConnection.opsSucceeded(ops);
    }
    for (Operation op : ops) {
      if (op.isCancelled()) {
        throw new ExecutionException(new RuntimeException(op.getCancelCause()));
      }
      if (op.hasErrored()) {
        throw new ExecutionException(op.getException());
      }
    }

    Map<String, T> resultMap = new HashMap<>();
    for (Map.Entry<String, GetResult<T>> me : rvMap.entrySet()) {
      String key = me.getKey();
      T value = me.getValue().getDecodedValue();

      // put the key into the result map.
      resultMap.put(key, value);
    }
    return resultMap;
  }
  /*
   * set to true if timeout was reached.
   *
   * @see net.spy.memcached.internal.BulkFuture#isTimeout()
   */
  public boolean isTimeout() {
    return isTimeout.get();
  }
}
