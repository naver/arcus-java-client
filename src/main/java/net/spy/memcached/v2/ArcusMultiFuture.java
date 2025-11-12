package net.spy.memcached.v2;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ArcusMultiFuture<T> extends CompletableFuture<T> implements ArcusFuture<T> {

  private final Function<ArcusMultiFuture<T>, T> combiner;

  public ArcusMultiFuture(Collection<CompletableFuture<?>> futures,
                          Function<ArcusMultiFuture<T>, T> combiner) {
    this.combiner = combiner;
    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .thenRun(() -> complete(combiner.apply(this)))
        .exceptionally(throwable -> {
          completeExceptionally(throwable);
          return null;
        });
  }

  @Override
  public void complete() {
    // Not used in this implementation
  }

  /**
   * Retrieves the combined results when some futures completed exceptionally.
   * The result of keys from exceptionally completed futures will be null.
   *
   * @return the combined result
   */
  public T getResultsWithFailures() {
    return combiner.apply(this);
  }
}
