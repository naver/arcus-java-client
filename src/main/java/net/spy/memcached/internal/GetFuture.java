package net.spy.memcached.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.result.GetResult;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;

/**
 * Future returned for GET operations.
 *
 * Not intended for general use.
 *
 * @param <T> Type of object returned from the get
 */
public class GetFuture<T> implements Future<T> {

  private final OperationFuture<GetResult<T>> rv;

  public GetFuture(CountDownLatch l, long opTimeout) {
    this.rv = new OperationFuture<GetResult<T>>(l, opTimeout);
  }

  public GetFuture(GetFuture<T> parent) {
    this.rv = parent.rv;
  }

  public boolean cancel(boolean ign) {
    return rv.cancel(ign);
  }

  public T get() throws InterruptedException, ExecutionException {
    GetResult<T> result = rv.get();
    return result == null ? null : result.getDecodedValue();
  }

  public T get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {
    GetResult<T> result = rv.get(duration, units);
    return result == null ? null : result.getDecodedValue();
  }

  public OperationStatus getStatus() {
    return rv.getStatus();
  }

  public void set(GetResult<T> result, OperationStatus status) {
    rv.set(result, status);
  }

  public void setOperation(Operation to) {
    rv.setOperation(to);
  }

  public boolean isCancelled() {
    return rv.isCancelled();
  }

  public boolean isDone() {
    return rv.isDone();
  }

}
