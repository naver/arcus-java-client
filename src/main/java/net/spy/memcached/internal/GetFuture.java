package net.spy.memcached.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.result.GetResult;
import net.spy.memcached.ops.OperationStatus;

/**
 * Future returned for GET operations.
 *
 * Not intended for general use.
 *
 * @param <T> Type of object returned from the get
 */
public class GetFuture<T> extends OperationFuture<T> {
  private GetResult<T> result;

  public GetFuture(CountDownLatch l, long opTimeout) {
    super(l, opTimeout);
  }

  @Override
  public T get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {
    super.get(duration, units); // for waiting latch.
    return result == null ? null : result.getDecodedValue();
  }

  public void set(GetResult<T> result, OperationStatus status) {
    super.set(null, status);
    this.result = result;
  }
}
