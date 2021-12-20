// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.NoopOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.VersionOperation;
import net.spy.memcached.ops.StatusCode;

/**
 * Operation to request the version of a memcached server.
 */
final class VersionOperationImpl extends OperationImpl
        implements VersionOperation, NoopOperation {

  private static final byte[] REQUEST = KeyUtil.getKeyBytes("version\r\n");

  public VersionOperationImpl(OperationCallback c) {
    super(c);
    setAPIType(APIType.VERSION);
    setOperationType(OperationType.ETC);
  }

  @Override
  public void handleLine(String line) {
    OperationStatus cause;
    if (line.startsWith("VERSION ")) {
      cause = new OperationStatus(
              true, line.substring("VERSION ".length()), StatusCode.SUCCESS);
    } else {
      cause = new OperationStatus(false, line, StatusCode.fromAsciiLine(line));
    }
    getCallback().receivedStatus(cause);
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    setBuffer(ByteBuffer.wrap(REQUEST));
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
