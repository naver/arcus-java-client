package net.spy.memcached.protocol.binary;

import net.spy.memcached.ops.NoopOperation;
import net.spy.memcached.ops.OperationCallback;

/**
 * Implementation of a noop operation.
 */
class NoopOperationImpl extends OperationImpl implements NoopOperation {

  static final int CMD = 10;

  public NoopOperationImpl(OperationCallback cb) {
    super(CMD, generateOpaque(), cb);
  }

  @Override
  public void initialize() {
    prepareBuffer("", 0, EMPTY_BYTES);
  }

  @Override
  public boolean isBulkOperation() {
    return false;
  }

  @Override
  public boolean isPipeOperation() {
    return false;
  }
}
