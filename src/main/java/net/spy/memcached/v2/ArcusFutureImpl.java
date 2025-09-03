package net.spy.memcached.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.internal.CompositeException;
import net.spy.memcached.ops.Operation;

public class ArcusFutureImpl<T> extends CompletableFuture<T> implements ArcusFuture<T> {

  private final List<Operation> ops;

  private final ArcusResult<T> arcusResult;

  private final AtomicBoolean isCancelling = new AtomicBoolean(false);

  public ArcusFutureImpl(ArcusResult<T> arcusResult) {
    this.arcusResult = arcusResult;
    this.ops = new ArrayList<>();
  }

  /**
   * Called by the IO(Internal) Thread when all Operations are completed.
   * If there is an error in the response, completes the CompletableFuture with an exception.
   */
  @Override
  public void complete() {
    if (this.isDone()) {
      return;
    }

    Exception exception = hasError();
    if (exception != null) {
      this.completeExceptionally(exception);
    } else {
      super.complete(this.arcusResult.get());
    }
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
  private Exception hasError() {
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
    for (Operation op : ops) {
      if (op.hasErrored()) {
        exceptions.add(op.getException());
      }
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
    if (this.isDone() || !isCancelling.compareAndSet(false, true)) {
      return false;
    }

    try {
      boolean cancelled = false;
      for (Operation op : ops) {
        if (!op.isCancelled()) {
          cancelled |= op.cancel("by application.");
        }
      }

      return cancelled && super.cancel(mayInterruptIfRunning);
    } finally {
      isCancelling.set(false);
    }
  }

  /**
   * If operation is cancelled by the internal process(e.g., node failure handling),
   * this method is called to cancel the future.
   * This method prevents recursive cancellation loops
   * by checking if external cancellation is in progress.
   */
  void internalCancel() {
    if (this.isDone() || isCancelling.get()) {
      return;
    }
    super.cancel(false);
  }

  /**
   * For internal use only.
   */
  void addOp(Operation op) {
    this.ops.add(op);
  }
}
