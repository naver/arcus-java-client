package net.spy.memcached.protocol.ascii;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;

import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.protocol.ProxyCallback;
import net.spy.memcached.protocol.TCPMemcachedNodeImpl;

/**
 * Memcached node for the ASCII protocol.
 */
public final class AsciiMemcachedNodeImpl extends TCPMemcachedNodeImpl {

	public AsciiMemcachedNodeImpl(SocketAddress sa, SocketChannel c,
			int bufSize, BlockingQueue<Operation> rq,
			BlockingQueue<Operation> wq, BlockingQueue<Operation> iq, Long opQueueMaxBlockTimeNs) {
		super(sa, c, bufSize, rq, wq, iq, opQueueMaxBlockTimeNs, false); /* ascii never does auth */
	}

	@Override
	protected void optimize() {
		// make sure there are at least two get operations in a row before
		// attempting to optimize them.
		Operation nxtOp = writeQ.peek();
		if (nxtOp instanceof GetOperation && nxtOp.getAPIType() != APIType.MGET) {
			optimizedOp=writeQ.remove();
			nxtOp = writeQ.peek();
			if (nxtOp instanceof GetOperation && nxtOp.getAPIType() != APIType.MGET) {
				OptimizedGetImpl og=new OptimizedGetImpl(
						(GetOperation)optimizedOp);
				optimizedOp=og;

				do {
					GetOperationImpl o=(GetOperationImpl) writeQ.remove();
					if(!o.isCancelled()) {
						og.addOperation(o);
					}
					nxtOp = writeQ.peek();
				} while (nxtOp instanceof GetOperation &&
						nxtOp.getAPIType() != APIType.MGET);

				// Initialize the new mega get
				optimizedOp.initialize();
				assert optimizedOp.getState() == OperationState.WRITING;
				ProxyCallback pcb=(ProxyCallback) og.getCallback();
				getLogger().debug("Set up %s with %s keys and %s callbacks",
						this, pcb.numKeys(), pcb.numCallbacks());
			}
		}
	}

}
