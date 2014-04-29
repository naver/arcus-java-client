/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ops.Operation;

/**
 * Timeout exception that tracks the original operation.
 */
public class CheckedOperationTimeoutException extends TimeoutException {

	private final Collection<Operation> operations;

	/**
	 * Construct a CheckedOperationTimeoutException with the given message
	 * and operation.
	 *
	 * @param message the message
	 * @param op the operation that timed out
	 */
	public CheckedOperationTimeoutException(String message, Operation op) {
		this(message, Collections.singleton(op));
	}

	public CheckedOperationTimeoutException(String message,
			Collection<Operation> ops) {
		super(createMessage(message, ops));
		operations = ops;
	}

	private static String createMessage(String message,
			Collection<Operation> ops) {
		StringBuilder rv = new StringBuilder(message);
		rv.append(" - failing node");
		rv.append(ops.size() == 1 ? ": " : "s: ");
		boolean first = true;
		for(Operation op : ops) {
			if(first) {
				first = false;
			} else {
				rv.append(", ");
			}
			MemcachedNode node = op == null ? null : op.getHandlingNode();
			rv.append(node == null ? "<unknown>" : node.getSocketAddress());
			if (op != null) {
				rv.append(" [").append(op.getState()).append("]");
			}
			if (node != null) {
				rv.append(" [").append(node.getStatus()).append("]");
			}
//			if (op != null && op.getBuffer() != null) {
//				rv.append(" [")
//						.append(new String(op.getBuffer().array()).replace(
//								"\r\n", "\\n")).append("]");
//			}
		}
		return rv.toString();
	}

	/**
	 * Get the operation that timed out.
	 */
	public Collection<Operation> getOperations() {
		return operations;
	}
}
