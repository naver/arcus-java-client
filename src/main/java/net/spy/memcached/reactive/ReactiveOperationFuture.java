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
package net.spy.memcached.reactive;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

/**
 * Managed future for operations.
 *
 * Not intended for general use.
 *
 * @param <T> Type of object returned from this future.
 */
public class ReactiveOperationFuture<T> extends SpyCompletableFuture<T> {

  protected final CountDownLatch latch;
  protected final AtomicReference<T> objRef;
  protected OperationStatus status;
  protected final long timeout;
  protected Operation op;

  private Long operationSetAt = null;
  private Long completedAt = null;

  public ReactiveOperationFuture(CountDownLatch l, long opTimeout) {
    this.latch = l;
    this.timeout = opTimeout;
    this.objRef = new AtomicReference<>(null);
  }

  public boolean cancel(boolean ign) {
    assert op != null : "No operation";
    op.cancel("by application.");
    // This isn't exactly correct, but it's close enough.  If we're in
    // a writing state, we *probably* haven't started.
    return op.getState() == OperationState.WRITE_QUEUED;
  }

  public T get() throws InterruptedException, ExecutionException {
    try {
      return get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException(e);
    }
  }

  public T get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {

    Exception exception = createException(
            !latch.await(duration, units), TimeUnit.MILLISECONDS.convert(duration, units));

    if (exception instanceof TimeoutException) {
      throw (TimeoutException) exception;
    }
    if (exception instanceof ExecutionException) {
      throw (ExecutionException) exception;
    }

    return objRef.get();
  }

  public OperationStatus getStatus() {
    if (status == null) {
      try {
        get();
      } catch (InterruptedException e) {
        status = new OperationStatus(false, "Interrupted", StatusCode.INTERRUPTED);
      } catch (ExecutionException e) {
        getLogger().warn("Error getting status of operation", e);
      }
    }
    return status;
  }

  public void setStatus(OperationStatus s) {
    status = s;
  }

  public void setOperation(Operation to) {
    if (operationSetAt == null) {
      operationSetAt = System.currentTimeMillis();
    }

    op = to;
  }

  @Override
  public boolean complete(T value) {
    if (completedAt == null) {
      completedAt = System.currentTimeMillis();
    }

    objRef.set(value);
    Exception exception = createException(false, timeout);

    if (exception != null) {
      this.completeExceptionally(exception);
      return false;
    }

    return super.complete(value);
  }

  private Exception createException(boolean forceTimeout, long timeoutMillis) {
    Exception timeoutException = createTimeoutException(forceTimeout, timeoutMillis);
    if (timeoutException != null) {
      return timeoutException;
    }
    return createExecutionException();
  }

  private TimeoutException createTimeoutException(boolean forceTimeout, long timeoutMillis) {
    if (forceTimeout || completedAt - operationSetAt > timeoutMillis) {
      // whenever timeout occurs, continuous timeout counter will increase by 1.
      MemcachedConnection.opTimedOut(op);
      return new CheckedOperationTimeoutException(timeoutMillis, TimeUnit.MILLISECONDS, op);
    } else {
      // continuous timeout counter will be reset
      MemcachedConnection.opSucceeded(op);
    }
    return null;
  }

  private ExecutionException createExecutionException() {
    if (op != null && op.hasErrored()) {
      return new ExecutionException(op.getException());
    }
    if (op != null && op.isCancelled()) {
      return new ExecutionException(new RuntimeException(op.getCancelCause()));
    }
    return null;
  }

  public boolean isCancelled() {
    assert op != null : "No operation";
    return op.isCancelled();
  }

  public boolean isDone() {
    assert op != null : "No operation";
    return latch.getCount() == 0 ||
            op.isCancelled() || op.getState() == OperationState.COMPLETE;
  }
}
