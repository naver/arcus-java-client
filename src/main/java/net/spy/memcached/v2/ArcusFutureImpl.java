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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.CompositeException;
import net.spy.memcached.ops.Operation;

import static net.spy.memcached.v2.ArcusFutureState.DECIDED;
import static net.spy.memcached.v2.ArcusFutureState.RESPONSE_RECEIVED;
import static net.spy.memcached.v2.ArcusFutureState.WAITING_RESPONSE;

/**
 * {@link ArcusFuture} for a single Arcus operation.
 *
 * @param <T> result type
 */
public class ArcusFutureImpl<T> extends CompletableFuture<T> implements ArcusFuture<T> {

  private Operation op;

  private final ArcusResult<?> arcusResult;
  private final Function<Object, T> decoder;
  private final AtomicReference<ScheduledFuture<?>> timeoutFuture = new AtomicReference<>();
  private final AtomicReference<ArcusFutureState> state = new AtomicReference<>(WAITING_RESPONSE);

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
   * Called by the operation callback when the complete response has arrived.
   *
   * <p>
   * Stops the operation timeout and resets the node timeout count,
   * then completes this future. An error, or a result that needs no decoding,
   * completes it on the calling thread.
   * Otherwise, the result is decoded and completed on the completion executor.
   * Does nothing if a timeout or cancellation came first.
   * </p>
   */
  @Override
  public void complete() {
    if (!state.compareAndSet(WAITING_RESPONSE, RESPONSE_RECEIVED)) {
      return;
    }

    cancelTimeoutTask();
    MemcachedConnection.opSucceeded(op);

    Exception error = getError();
    if (error != null) {
      failResponse(error);
      return;
    }

    if (decoder == null) {
      @SuppressWarnings("unchecked")
      T value = (T) this.arcusResult.get();
      finishResponse(value);
      return;
    }

    ArcusExecutors.COMPLETION_EXECUTOR.execute(this::decodeResponse);
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

  private void finishResponse(T value) {
    if (state.compareAndSet(RESPONSE_RECEIVED, DECIDED)) {
      super.complete(value);
    }
  }

  private void failResponse(Exception exception) {
    if (state.compareAndSet(RESPONSE_RECEIVED, DECIDED)) {
      super.completeExceptionally(exception);
    }
  }

  private void decodeResponse() {
    // Skip decoding if cancelled after the response arrived.
    if (state.get() != RESPONSE_RECEIVED) {
      return;
    }

    try {
      T value = decoder.apply(arcusResult.get());
      finishResponse(value);
    } catch (Exception e) {
      failResponse(e);
    }
  }

  /**
   * Cancels this future. If the response has not arrived, the operation is cancelled too;
   * otherwise only this future is cancelled and the pending result is discarded.
   *
   * @param mayInterruptIfRunning ignored
   * @return {@code true} if this call cancelled the future; {@code false} if the outcome was
   * already decided, even when the future is not done yet
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    ArcusFutureState previous = state.getAndSet(DECIDED);

    if (previous == DECIDED) {
      return false;
    }

    cancelTimeoutTask();

    boolean cancelled;
    try {
      if (previous == WAITING_RESPONSE) {
        op.cancel("by application");
      }
    } finally {
      cancelled = super.cancel(false);
    }

    return cancelled;
  }

  /**
   * Cancels this future when the operation is cancelled outside this future, for example on
   * connection loss. Does nothing if the outcome is already decided.
   */
  void internalCancel() {
    if (!state.compareAndSet(WAITING_RESPONSE, DECIDED)) {
      return;
    }

    cancelTimeoutTask();
    super.cancel(false);
  }

  /**
   * Sets the operation backing this future. Must be called before the operation is submitted.
   */
  void setOp(Operation op) {
    this.op = op;
  }

  /**
   * Starts the operation timeout. Called once, right after the operation is submitted.
   * Does nothing if the outcome is already decided.
   *
   * @param timeoutMillis operation timeout in milliseconds
   * @throws IllegalStateException if the timeout is already scheduled
   */
  void scheduleTimeout(long timeoutMillis) {
    if (state.get() != WAITING_RESPONSE) {
      return;
    }

    ScheduledFuture<?> task = ArcusExecutors.TIMEOUT_SCHEDULER.schedule(
        () -> timeout(timeoutMillis),
        timeoutMillis,
        TimeUnit.MILLISECONDS);

    if (!timeoutFuture.compareAndSet(null, task)) {
      task.cancel(false);
      throw new IllegalStateException("Operation timeout is already scheduled.");
    }

    // A response or cancellation may have come first while scheduling.
    if (state.get() != WAITING_RESPONSE) {
      cancelTimeoutTask();
    }
  }

  /**
   * Handles operation timeout expiration.
   *
   * <p>If still awaiting a response, updates the node timeout count, cancels the operation,
   * and completes this future exceptionally on the completion executor so that dependent
   * stages do not run on the timeout scheduler thread.</p>
   *
   * @param timeoutMillis operation timeout in milliseconds
   */
  private void timeout(long timeoutMillis) {
    if (!state.compareAndSet(WAITING_RESPONSE, DECIDED)) {
      return;
    }

    OperationTimeoutException exception = new OperationTimeoutException(
        op.getAPIType() + " operation timed out after " + timeoutMillis
            + " milliseconds while waiting for a response"
            + " @ " + getHandlingNodeName() + ".");

    MemcachedConnection.opTimedOut(op);
    op.cancel("by operation timeout");

    ArcusExecutors.COMPLETION_EXECUTOR.execute(() -> super.completeExceptionally(exception));
  }

  private String getHandlingNodeName() {
    MemcachedNode node = op.getHandlingNode();
    return node == null ? "<unknown>" : node.getNodeName();
  }

  private void cancelTimeoutTask() {
    ScheduledFuture<?> task = timeoutFuture.getAndSet(null);
    if (task != null) {
      task.cancel(false);
    }
  }
}
