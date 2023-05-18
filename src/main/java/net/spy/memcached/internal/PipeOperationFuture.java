package net.spy.memcached.internal;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import static net.spy.memcached.ArcusClient.pipeOpTimeOutHandler;

public class PipeOperationFuture<T> extends CollectionFuture<T> {

  private final ConcurrentLinkedQueue<Operation> ops;

  private final List<OperationStatus> mergedOperationStatus;

  private final T result;

  public PipeOperationFuture(CountDownLatch l, long opTimeout,
                             ConcurrentLinkedQueue<Operation> ops,
                             List<OperationStatus> mergedOperationStatus,
                             T mergedResult) {
    super(l, opTimeout);
    this.ops = ops;
    this.mergedOperationStatus = mergedOperationStatus;
    this.result = mergedResult;
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
  public T get(long duration, TimeUnit units) throws
          InterruptedException,
          TimeoutException,
          ExecutionException {
    if (!latch.await(duration, units)) {
      pipeOpTimeOutHandler(duration, units, ops);
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
    return result;
  }

  @Override
  public void setOperation(Operation to) {
    super.setOperation(to);
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
  public CollectionOperationStatus getOperationStatus() {
    for (OperationStatus status : mergedOperationStatus) {
      if (!status.isSuccess()) {
        return new CollectionOperationStatus(status);
      }
    }
    return new CollectionOperationStatus(true, "END", CollectionResponse.END);
  }
}
