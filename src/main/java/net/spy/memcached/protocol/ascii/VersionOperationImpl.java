// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.NoopOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.VersionOperation;

/**
 * Operation to request the version of a memcached server.
 */
final class VersionOperationImpl extends OperationImpl
	implements VersionOperation, NoopOperation {

	private static final byte[] REQUEST="version\r\n".getBytes();

	public VersionOperationImpl(OperationCallback c) {
		super(c);
		setAPIType(APIType.VERSION);
		setOperationType(OperationType.ETC);
	}

	@Override
	public void handleLine(String line) {
		assert line.startsWith("VERSION ");
		getCallback().receivedStatus(
				new OperationStatus(true, line.substring("VERSION ".length())));
		transitionState(OperationState.COMPLETE);
		/* ENABLE_REPLICATION if */
		/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
		// check switchovered operation for debug
		checkMoved(line);
		/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
		/* ENABLE_REPLICATION end */
	}

	@Override
	public void initialize() {
		setBuffer(ByteBuffer.wrap(REQUEST));
	}

}
