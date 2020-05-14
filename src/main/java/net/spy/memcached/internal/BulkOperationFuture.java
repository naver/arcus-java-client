package net.spy.memcached.internal;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

public abstract class BulkOperationFuture
        extends OperationFuture<Map<String, CollectionOperationStatus>> {
  protected final Map<String, CollectionOperationStatus> failedResult =
          new HashMap<String, CollectionOperationStatus>();
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
      if (op.isCancelled())
        return true;
    }
    return false;
  }

  @Override
  public Map<String, CollectionOperationStatus> get(long duration,
                                                    TimeUnit units) throws InterruptedException,
          TimeoutException, ExecutionException {
    if (!latch.await(duration, units)) {
      for (Operation op : ops) {
        if (op.getState() != OperationState.COMPLETE) {
          MemcachedConnection.opTimedOut(op);
        } else {
          MemcachedConnection.opSucceeded(op);
        }
      }
      throw new CheckedOperationTimeoutException(
              "Timed out waiting for bulk operation >" + duration + " " + units, ops);
    } else {
      // continuous timeout counter will be reset
      for (Operation op : ops) {
        MemcachedConnection.opSucceeded(op);
      }
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
