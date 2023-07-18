package net.spy.memcached.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

public class BulkOperationFuture<T> implements Future<Map<String, T>> {
  protected final Map<String, T> failedResult = new HashMap<String, T>();
  protected final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();
  protected final long timeout;
  protected final CountDownLatch latch;

  public BulkOperationFuture(CountDownLatch l, long timeout) {
    this.latch = l;
    this.timeout = timeout;
  }

  @Override
  public boolean cancel(boolean ign) {
    boolean rv = false;
    for (Operation op : ops) {
      if (op.getState() != OperationState.COMPLETE) {
        rv = true;
        op.cancel("by application.");
      }
    }
    return rv;
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
  public Map<String, T> get() throws InterruptedException, ExecutionException {
    try {
      return get(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException(e);
    }
  }

  @Override
  public Map<String, T> get(long duration,
                            TimeUnit units) throws InterruptedException,
          TimeoutException, ExecutionException {
    if (!latch.await(duration, units)) {
      Collection<Operation> timedoutOps = new HashSet<Operation>();
      for (Operation op : ops) {
        if (op.getState() != OperationState.COMPLETE) {
          timedoutOps.add(op);
        } else {
          MemcachedConnection.opSucceeded(op);
        }
      }
      if (timedoutOps.size() > 0) {
        MemcachedConnection.opsTimedOut(timedoutOps);
        throw new CheckedOperationTimeoutException(duration, units, timedoutOps);
      }
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

    return failedResult;
  }

  public void setOperations(Collection<Operation> ops) {
    this.ops.addAll(ops);
  }

  public void addFailedResult(String key, T value) {
    failedResult.put(key, value);
  }
}
