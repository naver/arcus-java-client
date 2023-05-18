package net.spy.memcached.internal;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CollectionBulkFuture<T> implements Future<T> {

  private final Collection<Operation> ops;
  private final long timeout;
  private final CountDownLatch latch;
  private final T result;

  public CollectionBulkFuture(Collection<Operation> ops, long timeout,
                              CountDownLatch latch, T result) {
    this.ops = ops;
    this.timeout = timeout;
    this.latch = latch;
    this.result = result;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      return get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException(e);
    }
  }

  @Override
  public T get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {
    if (!latch.await(duration, units)) {
      ArcusClient.bulkOpTimeOutHandler(duration, units, ops);
    } else {
        // continuous timeout counter will be reset
      MemcachedConnection.opsSucceeded(ops);
    }
    for (Operation op : ops) {
      if (op != null && op.hasErrored()) {
        throw new ExecutionException(op.getException());
      }
      if (op != null && op.isCancelled()) {
        throw new ExecutionException(new RuntimeException(op.getCancelCause()));
      }
    }
    return result;
  }

  @Override
  public boolean cancel(boolean ign) {
    boolean rv = false;
    for (Operation op : ops) {
      op.cancel("by application.");
      rv |= op.getState() == OperationState.WRITE_QUEUED;
    }
    return rv;
  }

  @Override
  public boolean isDone() {
    for (Operation op : ops) {
      if (!(op.getState() == OperationState.COMPLETE || op.isCancelled())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isCancelled() {
    for (Operation op : ops) {
      if (op.isCancelled()) {
        return true;
      }
    }
    return false;
  }
}
