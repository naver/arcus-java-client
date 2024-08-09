/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
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
import java.util.concurrent.TimeUnit;

import net.spy.memcached.MockMemcachedNode;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.BaseOperationImpl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CheckedOperationTimeoutExceptionTest {

  private final long duration = 1;
  private final TimeUnit unit = TimeUnit.MILLISECONDS;
  private final long elapsed = 5;

  @Test
  public void testSingleOperation() {
    Operation op = buildOp(11211);
    Exception e = new CheckedOperationTimeoutException(duration, unit, elapsed, op);

    String expected = CheckedOperationTimeoutException.class.getName() +
            ": UNDEFINED operation timed out (5 >= 1 MILLISECONDS)" +
            " - failing node: localhost:11211 [WRITE_QUEUED] [MOCK_STATE]";
    assertEquals(expected, e.toString());
  }

  @Test
  public void testNullNode() {
    Operation op = new TestOperation();
    Exception e = new CheckedOperationTimeoutException(duration, unit, elapsed, op);

    String expected = CheckedOperationTimeoutException.class.getName() +
            ": UNDEFINED operation timed out (5 >= 1 MILLISECONDS)" +
            " - failing node: <unknown> [WRITE_QUEUED]";
    assertEquals(expected, e.toString());
  }

  @Test
  public void testNullOperation() {
    try {
      Exception e = new CheckedOperationTimeoutException(duration, unit, elapsed, (Operation) null);
      fail("NullPointerException is NOT thrown... " + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(e instanceof NullPointerException);
    }
  }

  @Test
  public void testMultipleOperation() {
    Collection<Operation> ops = new ArrayList<>();
    ops.add(buildOp(11211));
    ops.add(buildOp(64212));
    Exception e = new CheckedOperationTimeoutException(duration, unit, elapsed, ops);

    String expected = CheckedOperationTimeoutException.class.getName() +
            ": UNDEFINED operation timed out (5 >= 1 MILLISECONDS)" +
            " - failing nodes: localhost:11211 [WRITE_QUEUED] [MOCK_STATE]," +
            " localhost:64212 [WRITE_QUEUED] [MOCK_STATE]";
    assertEquals(expected, e.toString());
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

    @Override
    public boolean isBulkOperation() {
      return false;
    }

    @Override
    public boolean isPipeOperation() {
      return false;
    }

    @Override
    public boolean isIdempotentOperation() {
      return false;
    }
  }
}
