package net.spy.memcached.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

public class PipedCollectionFuture<K, V>
        extends CollectionFuture<Map<K, V>> {
  private final List<Operation> ops = new ArrayList<>();
  private final AtomicReference<CollectionOperationStatus> operationStatus
          = new AtomicReference<>(null);
  private int currentOpIdx = -1;

  private final Map<K, V> failedResult =
          new ConcurrentHashMap<>();

  public PipedCollectionFuture(CountDownLatch l, long opTimeout) {
    super(l, opTimeout);
  }

  @Override
  public boolean cancel(boolean ign) {
    for (int i = currentOpIdx; i < ops.size(); i++) {
      if (ops.get(i).cancel("by application.")) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if any operation is cancelled.
   */
  @Override
  public boolean isCancelled() {
    for (int i = currentOpIdx; i < ops.size(); i++) {
      if (ops.get(i).isCancelled()) {
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
  public Map<K, V> get(long duration, TimeUnit unit)
          throws InterruptedException, TimeoutException, ExecutionException {

    long beforeAwait = System.currentTimeMillis();
    if (!latch.await(duration, unit)) {
      Collection<Operation> timedOutOps = new ArrayList<>();
      for (Operation op : ops) {
        if (op.getState() != OperationState.COMPLETE) {
          timedOutOps.add(op);
        } else {
          MemcachedConnection.opSucceeded(op);
        }
      }
      if (!timedOutOps.isEmpty()) {
        // set timeout only once for piped ops requested to single node.
        MemcachedConnection.opTimedOut(timedOutOps.iterator().next());

        long elapsed = System.currentTimeMillis() - beforeAwait;
        throw new CheckedOperationTimeoutException(duration, unit, elapsed, timedOutOps);
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

    return failedResult;
  }

  @Override
  public CollectionOperationStatus getOperationStatus() {
    return operationStatus.get();
  }

  public void setOperationStatus(CollectionOperationStatus status) {
    if (operationStatus.get() == null) {
      operationStatus.set(status);
      return;
    }

    if (!status.isSuccess() && operationStatus.get().isSuccess()) {
      operationStatus.set(status);
    }
  }

  public void addEachResult(K index, V status) {
    failedResult.put(index, status);
  }

  public void addOperation(Operation op) {
    ops.add(op);
  }

  public Operation getOperation(int idx) {
    return ops.get(idx);
  }

  public void incrCurrentOpIdx() {
    this.currentOpIdx++;
  }

  public Operation getNextOp() {
    try {
      return ops.get(currentOpIdx + 1);
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }
}
