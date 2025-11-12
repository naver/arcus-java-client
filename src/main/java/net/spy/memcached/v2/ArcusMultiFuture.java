package net.spy.memcached.v2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import net.spy.memcached.internal.CompositeException;

/**
 * A Future representing multiple underlying futures mapped by keys.
 * If any of the underlying futures complete exceptionally, this future
 * also completes exceptionally. If multiple futures completed exceptionally,
 * This future will throw CompositeException containing all exceptions.
 * And the combined result can be obtained.
 *
 * @param <T>
 */
public final class ArcusMultiFuture<T> extends CompletableFuture<T> implements ArcusFuture<T> {

  private final Collection<CompletableFuture<?>> futures;
  private final AtomicReference<T> combinedResult = new AtomicReference<>();

  public ArcusMultiFuture(Collection<CompletableFuture<?>> futures,
                          Supplier<T> combiner) {
    this.futures = futures;
    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .whenCompleteAsync((v, t) -> {
          try {
            T result = combiner.get();
            combinedResult.set(result);

            if (t == null) {
              this.complete(result);
            } else {
              completeExceptionally();
            }
          } catch (Exception e) {
            this.completeExceptionally(e);
          }
        }, ArcusExecutors.COMPLETION_EXECUTOR);
  }

  private void completeExceptionally() {
    List<Exception> exceptions = new ArrayList<>();
    boolean hasNonCancellationException = false;

    for (CompletableFuture<?> future : futures) {
      if (future.isCompletedExceptionally()) {
        try {
          future.join();
        } catch (Exception e) {
          exceptions.add(e);
          if (!future.isCancelled()) {
            hasNonCancellationException = true;
          }
        }
      }
    }

    // If there are only CancellationExceptions, cancel this future
    if (!hasNonCancellationException) {
      super.cancel(true);
      return;
    }

    // If there are other exceptions, include all exceptions (including CancellationException)
    if (exceptions.size() > 1) {
      this.completeExceptionally(new CompositeException(exceptions));
    } else if (exceptions.size() == 1) {
      this.completeExceptionally(exceptions.get(0));
    }
  }

  @Override
  public void complete() {
    // Not used in this implementation
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    boolean cancelled = false;
    for (CompletableFuture<?> future : futures) {
      cancelled |= future.cancel(mayInterruptIfRunning);
    }
    return cancelled;
  }

  /**
   * Retrieves the combined results when some futures completed exceptionally.
   * The result of keys from exceptionally completed futures will be null.
   * If the combined result is not yet ready, this method will wait for all
   * underlying futures to complete and then compute it.
   *
   * @return the combined result
   */
  public T getResultsWithFailures() {
    return combinedResult.get();
  }
}
