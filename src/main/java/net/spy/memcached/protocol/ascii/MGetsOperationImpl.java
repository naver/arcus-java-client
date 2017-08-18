package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetsOperation;

import java.util.Collections;

/**
 * Implementation of the gets operation.
 */
public class MGetsOperationImpl extends BaseMGetOpImpl implements GetsOperation {

	private static final String CMD="mgets";

	public MGetsOperationImpl(String key, Callback cb) {
		super(CMD, cb, Collections.singleton(key));
		setAPIType(APIType.GETS);
	}

}
