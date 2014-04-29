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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import junit.framework.TestCase;
import net.spy.memcached.MockMemcachedNode;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.BaseOperationImpl;

public class CheckedOperationTimeoutExceptionTest extends TestCase {

	public void testSingleOperation() {
		Operation op = buildOp(11211);
		assertEquals(CheckedOperationTimeoutException.class.getName()
				+ ": test - failing node: localhost:11211 [WRITING] [MOCK_STATE]",
				new CheckedOperationTimeoutException("test", op).toString());
	}

	public void testNullNode() {
		Operation op = new TestOperation();
		assertEquals(CheckedOperationTimeoutException.class.getName()
				+ ": test - failing node: <unknown> [WRITING]",
				new CheckedOperationTimeoutException("test", op).toString());
	}

	public void testNullOperation() {
		assertEquals(CheckedOperationTimeoutException.class.getName()
				+ ": test - failing node: <unknown>",
				new CheckedOperationTimeoutException("test",
						(Operation)null).toString());
	}


	public void testMultipleOperation() {
		Collection<Operation> ops = new ArrayList<Operation>();
		ops.add(buildOp(11211));
		ops.add(buildOp(64212));
		assertEquals(CheckedOperationTimeoutException.class.getName()
				+ ": test - failing nodes: localhost:11211 [WRITING] [MOCK_STATE], localhost:64212 [WRITING] [MOCK_STATE]",
				new CheckedOperationTimeoutException("test", ops).toString());
	}

	private TestOperation buildOp(int portNum) {
		TestOperation op = new TestOperation();
		MockMemcachedNode node = new MockMemcachedNode(
				InetSocketAddress.createUnresolved("localhost", portNum));
		op.setHandlingNode(node);
		return op;
	}

	static class TestOperation extends BaseOperationImpl implements Operation {

		@Override
		public void initialize() {
			throw new RuntimeException("Not implemented.");
		}

		@Override
		public void readFromBuffer(ByteBuffer data) throws IOException {
			throw new RuntimeException("Not implemented");
		}

	}
}
