package net.spy.memcached.internal;

import net.spy.memcached.ops.CollectionGetOpCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CollectionGetFuture<T> extends CollectionFuture<T> {

  public CollectionGetFuture(CountDownLatch l, long opTimeout) {
    super(l, opTimeout);
  }

  @Override
  public T get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {

    T result = super.get(duration, units);
    if (result != null) {
      CollectionGetOpCallback callback = (CollectionGetOpCallback) op.getCallback();
      callback.addResult();
    }
    return result;
  }
}
