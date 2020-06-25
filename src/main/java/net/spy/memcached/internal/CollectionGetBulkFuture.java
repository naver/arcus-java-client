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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;

public class CollectionGetBulkFuture<T> implements Future<T> {

  private final Collection<Operation> ops;
  private final long timeout;
  private final CountDownLatch latch;
  private final T result;

  public CollectionGetBulkFuture(CountDownLatch latch, Collection<Operation> ops, T result,
                                 long timeout) {
    this.latch = latch;
    this.ops = ops;
    this.result = result;
    this.timeout = timeout;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      return get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new RuntimeException("Timed out waiting for smget operation", e);
    }
  }

  @Override
  public T get(long duration, TimeUnit units)
      throws InterruptedException, TimeoutException, ExecutionException {
    if (!latch.await(duration, units)) {
      for (Operation op : ops) {
        MemcachedConnection.opTimedOut(op);
      }
      throw new CheckedOperationTimeoutException(
          "Timed out waiting for b+tree get bulk operation", ops);
    } else {
      for (Operation op : ops) {
        MemcachedConnection.opSucceeded(op);
      }
    }

    for (Operation op : ops) {
      if (op != null && op.hasErrored()) {
        throw new ExecutionException(op.getException());
      }
      if (op != null && op.isCancelled()) {
        throw new ExecutionException(new RuntimeException(op.getCancelCause()));
      }
    }

    return result;
  }

  @Override
  public boolean cancel(boolean ign) {
    boolean rv = false;
    for (Operation op : ops) {
      op.cancel("by application.");
      rv |= op.getState() == OperationState.WRITE_QUEUED;
    }
    return rv;
  }

  @Override
  public boolean isCancelled() {
    boolean rv = false;
    for (Operation op : ops) {
      rv |= op.isCancelled();
    }
    return rv;
  }

  @Override
  public boolean isDone() {
    boolean rv = true;
    for (Operation op : ops) {
      rv &= op.getState() == OperationState.COMPLETE;
    }
    return rv || isCancelled();
  }

  public CollectionOperationStatus getOperationStatus() {
    if (isCancelled()) {
      return new CollectionOperationStatus(new OperationStatus(false, "CANCELED"));
    }

    return new CollectionOperationStatus(new OperationStatus(true, "END"));
  }
}
