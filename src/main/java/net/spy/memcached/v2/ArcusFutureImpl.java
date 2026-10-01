/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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
package net.spy.memcached.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.CompositeException;
import net.spy.memcached.ops.Operation;

/**
 * {@link ArcusFuture} for a single Arcus operation.
 *
 * @param <T> result type
 */
public class ArcusFutureImpl<T> extends CompletableFuture<T> implements ArcusFuture<T> {

  private static final String TIMEOUT_CANCEL_CAUSE = "by operation timeout";

  private Operation op;

  private final ArcusResult<?> arcusResult;
  private final Function<Object, T> decoder;
  private volatile ScheduledFuture<?> timeoutFuture;

  /**
   * Use only when the result needs to be decoded.
   */
  @SuppressWarnings("unchecked")
  public <R> ArcusFutureImpl(ArcusResult<R> arcusResult, Function<R, T> decoder) {
    this.arcusResult = arcusResult;
    this.decoder = (Function<Object, T>) decoder;
  }

  /**
   * Use only when the result doesn't need to be decoded.
   */
  public ArcusFutureImpl(ArcusResult<T> arcusResult) {
    this.arcusResult = arcusResult;
    this.decoder = null;
  }

  /**
   * Called by the operation callback when the operation is completed.
   *
   * <p>
   * Stops the operation timeout and resets the node timeout count,
   * then completes this future. An error, or a result that needs no decoding,
   * completes it on the calling thread.
   * Otherwise, the result is decoded and completed on the completion executor.
   * Does nothing if this future is already done by cancellation or timeout.
   * </p>
   */
  @Override
  public void complete() {
    if (this.isDone()) {
      return;
    }

    cancelTimeoutTask();
    MemcachedConnection.opSucceeded(op);

    Exception exception = getError();
    if (exception != null) {
      this.completeExceptionally(exception);
      return;
    }

    if (decoder == null) {
      @SuppressWarnings("unchecked")
      T result = (T) this.arcusResult.get();
      this.complete(result);
      return;
    }

    ArcusExecutors.COMPLETION_EXECUTOR.execute(() -> {
      try {
        T result = decoder.apply(this.arcusResult.get());
        this.complete(result);
      } catch (Exception e) {
        this.completeExceptionally(e);
      }
    });
  }

  /**
   * Checks if there are errors in Operation or ArcusResult
   * and returns an Exception object if there are errors.
   * If there are multiple errors, they are bundled and returned
   * as a CompositeException object.
   * Returns null if there are no errors.
   *
   * @return Exception or null
   */
  private Exception getError() {
    List<Exception> exceptions = new ArrayList<>();

    /*
     * TYPE_MISMATCH / BKEY_MISMATCH / OVERFLOWED / OUT_OF_RANGE / UNREADABLE
     */
    if (this.arcusResult.hasError()) {
      exceptions.addAll(this.arcusResult.getError());
    }

    /*
     * SERVER_ERROR / CLIENT_ERROR / ERROR
     */
    if (op.hasErrored()) {
      exceptions.add(op.getException());
    }

    if (exceptions.size() > 1) {
      return new CompositeException(exceptions);
    } else if (exceptions.size() == 1) {
      return exceptions.get(0);
    } else {
      return null;
    }
  }

  /**
   * Cancel this future and the related operation.
   * The operation can no longer be cancelled once its complete response has arrived.
   *
   * @param mayInterruptIfRunning this value has no effect in this implementation
   * @return {@code true} if the operation was cancelled,
   * {@code false} if this future is already done or the response has already arrived.
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (this.isDone()) {
      return false;
    }

    return op.cancel("by application.");
  }

  /**
   * Called by the operation callback when the operation is cancelled by the application,
   * the operation timeout, or the connection, for example on connection loss.
   * Completes this future with {@link OperationTimeoutException} if cancelled by the
   * operation timeout, or cancels this future otherwise.
   */
  void internalCancel() {
    cancelTimeoutTask();

    String cause = op.getCancelCause();
    if (cause != null && cause.contains(TIMEOUT_CANCEL_CAUSE)) {
      OperationTimeoutException exception = new OperationTimeoutException(
          op.getAPIType() + " operation timed out. " + cause);
      super.completeExceptionally(exception);
    } else {
      super.cancel(true);
    }
  }

  /**
   * Sets the operation backing this future. Must be called before the operation is submitted.
   */
  void setOp(Operation op) {
    this.op = op;
  }

  /**
   * Starts the operation timeout. Called once, right after the operation is submitted.
   * Does nothing if the operation was already cancelled while being submitted.
   *
   * @param timeoutMillis operation timeout in milliseconds
   */
  void scheduleTimeout(long timeoutMillis) {
    if (this.isDone()) {
      return;
    }

    timeoutFuture = ArcusExecutors.TIMEOUT_SCHEDULER.schedule(
        () -> timeout(timeoutMillis),
        timeoutMillis,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Cancels the operation if its response has not arrived yet.
   * The node timeout count is updated only when the cancellation succeeds,
   * because this future is not done yet while the result is being decoded.
   */
  private void timeout(long timeoutMillis) {
    if (this.isDone()) {
      return;
    }

    String timeoutCause = TIMEOUT_CANCEL_CAUSE + " after " + timeoutMillis + " milliseconds";
    if (op.cancel(timeoutCause)) {
      MemcachedConnection.opTimedOut(op);
    }
  }

  /**
   * Cancels the timeout task if it exists.
   */
  private void cancelTimeoutTask() {
    ScheduledFuture<?> task = timeoutFuture;
    if (task != null) {
      task.cancel(false);
    }
  }
}
