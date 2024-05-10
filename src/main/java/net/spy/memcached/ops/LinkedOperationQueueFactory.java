package net.spy.memcached.ops;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * OperationQueueFactory that creates LinkedBlockingQueue (unbounded) operation
 * queues.
 */
public class LinkedOperationQueueFactory implements OperationQueueFactory {

  public BlockingQueue<Operation> create() {
    return new LinkedBlockingQueue<>();
  }

}
