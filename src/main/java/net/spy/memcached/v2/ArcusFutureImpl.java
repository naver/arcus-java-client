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
import java.util.function.Function;

import net.spy.memcached.internal.CompositeException;
import net.spy.memcached.ops.Operation;

public class ArcusFutureImpl<T> extends CompletableFuture<T> implements ArcusFuture<T> {

  private Operation op;

  private final ArcusResult<?> arcusResult;
  private final Function<Object, T> decoder;

  /**
   * Use only when the result needs to be decoded.
   */
  public ArcusFutureImpl(ArcusResult<?> arcusResult, Function<Object, T> decoder) {
    this.arcusResult = arcusResult;
    this.decoder = decoder;
  }

  /**
   * Use only when the result doesn't need to be decoded.
   */
  public ArcusFutureImpl(ArcusResult<?> arcusResult) {
    this.arcusResult = arcusResult;
    this.decoder = null;
  }

  /**
   * Called by the IO(Internal) Thread when all Operations are completed.
   * Submits the decoding task to a dedicated thread pool and completes the future.
   * If there is an error in the response, completes the CompletableFuture with an exception.
   */
  @Override
  public void complete() {
    if (this.isDone()) {
      return;
    }

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
   * Cancel this future and the related operations.
   * This method is thread-safe and prevents multiple concurrent cancellation attempts.
   *
   * @param mayInterruptIfRunning this value has no effect in this
   *                              implementation because interrupts are not used to control
   *                              processing.
   * @return {@code true} if this future and all the operations were cancelled,
   * {@code false} if this future was already cancelled or completed.
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (this.isDone()) {
      return false;
    }

    return op.cancel("by application.");
  }

  void internalCancel() {
    super.cancel(true);
  }

  /**
   * For internal use only.
   */
  void setOp(Operation op) {
    this.op = op;
  }
}
