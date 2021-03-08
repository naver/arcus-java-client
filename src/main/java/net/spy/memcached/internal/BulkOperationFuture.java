package net.spy.memcached.internal;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

public abstract class BulkOperationFuture<T>
        extends OperationFuture<Map<String, T>> {
  protected final Map<String, T> failedResult =
          new HashMap<String, T>();
  protected final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();

  public BulkOperationFuture(Collection<String> keys, CountDownLatch l, long timeout) {
    super(l, timeout);

    for (String key : keys) {
      ops.add(createOp(key));
    }
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
  public boolean isCancelled() {
    for (Operation op : ops) {
      if (op.isCancelled()) {
        return true;
      }
    }
    return false;
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

  public abstract Operation createOp(String key);
}
