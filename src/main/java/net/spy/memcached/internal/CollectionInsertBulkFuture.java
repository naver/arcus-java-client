package net.spy.memcached.internal;

import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;


public class CollectionInsertBulkFuture<T> extends CollectionBulkFuture<T> {

  private final Collection<Operation> ops;

  public CollectionInsertBulkFuture(Collection<Operation> ops, long timeout,
                                    CountDownLatch latch, T result) {
    super(ops, timeout, latch, result);
    this.ops = ops;
  }

  public CollectionOperationStatus getOperationStatus() {
    return null;
  }

}
