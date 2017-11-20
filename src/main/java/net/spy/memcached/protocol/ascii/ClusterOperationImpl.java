package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.ClusterOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.OperationState;

import java.nio.ByteBuffer;

/**
 * Memcached flush_all operation.
 */
final class ClusterOperationImpl extends OperationImpl
        implements ClusterOperation {

  private static final OperationStatus OK =
          new OperationStatus(true, "OK");

  private final long version;

  public ClusterOperationImpl(long v, OperationCallback cb) {
    super(cb);
    version = v;
    setAPIType(APIType.CLUSTER);
    setOperationType(OperationType.ETC);
  }

  @Override
  public void handleLine(String line) {
    getLogger().debug("Cluster completed successfully");

    getCallback().receivedStatus(matchStatus(line, OK));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    ByteBuffer b = ByteBuffer.allocate(32);
    b.put(("cluster version " + version + "\r\n").getBytes());
    b.flip();
    setBuffer(b);
  }
}