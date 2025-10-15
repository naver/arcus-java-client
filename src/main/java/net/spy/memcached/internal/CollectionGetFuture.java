package net.spy.memcached.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.result.GetResult;
import net.spy.memcached.ops.CollectionOperationStatus;

public class CollectionGetFuture<T> extends CollectionFuture<T> {
  private GetResult<T> result;

  public CollectionGetFuture(CountDownLatch l, long opTimeout, ExecutorService service) {
    super(l, opTimeout, service);
  }

  @Override
  public T get(long duration, TimeUnit unit)
          throws InterruptedException, TimeoutException, ExecutionException {
    super.get(duration, unit); // for waiting latch.
    return result == null ? null : result.getDecodedValue();
  }

  public void setResult(GetResult<T> result, CollectionOperationStatus status) {
    super.set(null, status);
    this.result = result;
  }
}
