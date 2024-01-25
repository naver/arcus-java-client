package net.spy.memcached.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.result.GetResult;
import net.spy.memcached.ops.CollectionOperationStatus;

public class CollectionGetFuture<T> extends CollectionFuture<T> {
  private GetResult<T> result;

  public CollectionGetFuture(CountDownLatch l, long opTimeout) {
    super(l, opTimeout);
  }

  @Override
  public T get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {
    super.get(duration, units); // for waiting latch.
    return result == null ? null : result.getDecodedValue();
  }

  public void setResult(GetResult<T> result, CollectionOperationStatus status) {
    super.set(null, status);
    this.result = result;
  }
}
