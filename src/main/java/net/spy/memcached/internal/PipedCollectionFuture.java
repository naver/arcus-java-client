package net.spy.memcached.internal;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;

public class PipedCollectionFuture<K, V>
        extends CollectionFuture<Map<K, V>> {
  private final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();
  private final List<CollectionOperationStatus> mergedOperationStatus;

  private final Map<K, V> mergedResult =
          new ConcurrentHashMap<K, V>();

  public PipedCollectionFuture(CountDownLatch l, long opTimeout, int opSize) {
    super(l, opTimeout);
    mergedOperationStatus = Collections
            .synchronizedList(new ArrayList<CollectionOperationStatus>(opSize));
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
  public Map<K, V> get(long duration, TimeUnit units)
          throws InterruptedException, TimeoutException, ExecutionException {

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
        MemcachedConnection.opTimedOut(timedoutOps.iterator().next());
        throw new CheckedOperationTimeoutException(duration, units, timedoutOps);
      }
    } else {
      // continuous timeout counter will be reset only once in pipe
      MemcachedConnection.opSucceeded(ops.iterator().next());
    }

    for (Operation op : ops) {
      if (op != null && op.hasErrored()) {
        throw new ExecutionException(op.getException());
      }

      if (op != null && op.isCancelled()) {
        throw new ExecutionException(new RuntimeException(op.getCancelCause()));
      }
    }

    return mergedResult;
  }

  @Override
  public CollectionOperationStatus getOperationStatus() {
    for (CollectionOperationStatus status : mergedOperationStatus) {
      if (!status.isSuccess()) {
        return status;
      }
    }
    return mergedOperationStatus.get(0);
  }

  public void addOperationStatus(CollectionOperationStatus status) {
    mergedOperationStatus.add(status);
  }

  public void addEachResult(K index, V status) {
    mergedResult.put(index, status);
  }

  public void addOperation(Operation op) {
    ops.add(op);
  }
}
