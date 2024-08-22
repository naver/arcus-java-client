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
package net.spy.memcached.protocol;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the TCPMemcachedNodeImpl
 */
public class TCPMemcachedNodeImplTest {

  @SuppressWarnings("unchecked")
  private Queue<Operation> getQueue(String queueFieldName, TCPMemcachedNodeImpl node)
      throws Exception {
    Field queueField = TCPMemcachedNodeImpl.class.getDeclaredField(queueFieldName);
    queueField.setAccessible(true);

    return (Queue<Operation>) queueField.get(node);
  }

  private long getAddOpCount(TCPMemcachedNodeImpl node)
      throws Exception {
    Field field = TCPMemcachedNodeImpl.class.getDeclaredField("addOpCount");
    field.setAccessible(true);
    return ((AtomicLong) field.get(node)).get();
  }

  @Test
  public void testMoveOperations() throws Exception {
    // given
    final int fromReadOpCount = 5,
              fromWriteOpCount = 5,
              fromInputOpCount = 10,
              fromAllOpCount = fromReadOpCount + fromWriteOpCount + fromInputOpCount;

    final DefaultConnectionFactory factory = new DefaultConnectionFactory();

    TCPMemcachedNodeImpl fromNode = (TCPMemcachedNodeImpl) factory.createMemcachedNode(
        "tcp node impl test node",
        InetSocketAddress.createUnresolved("127.0.0.1", 11211),
        4096
    );

    TCPMemcachedNodeImpl toNode = (TCPMemcachedNodeImpl) factory.createMemcachedNode(
        "tcp node impl test node",
        InetSocketAddress.createUnresolved("127.0.0.2", 11211),
        4096
    );

    List<Operation> fromOperations = new LinkedList<>();
    for (int i = 0; i < fromAllOpCount; i++) {
      Operation op = factory.getOperationFactory().get(
          "cacheKey=" + i,
          new GetOperation.Callback() {
            @Override
            public void receivedStatus(OperationStatus status) {
            }

            @Override
            public void gotData(String key, int flags, byte[] data) {
            }

            @Override
            public void complete() {
            }
          });
      op.initialize();
      fromOperations.add(op);
    }

    for (int i = 0; i < fromAllOpCount; i++) {
      if (fromNode.getReadQueueSize() < fromReadOpCount) {
        assertTrue(getQueue("readQ", fromNode).offer(fromOperations.get(i)));
      } else if (fromNode.getWriteQueueSize() < fromWriteOpCount) {
        assertTrue(getQueue("writeQ", fromNode).offer(fromOperations.get(i)));
      } else {
        assertTrue(getQueue("inputQueue", fromNode).offer(fromOperations.get(i)));
      }
    }

    // when
    assertEquals(fromAllOpCount, fromNode.moveOperations(toNode, false));

    // then
    assertEquals(0, fromNode.getInputQueueSize());
    assertEquals(0, fromNode.getWriteQueueSize());
    assertEquals(0, fromNode.getReadQueueSize());
    assertEquals(fromAllOpCount, toNode.getWriteQueueSize());
    assertEquals(fromAllOpCount, getAddOpCount(toNode));

    for (int i = 0; i < fromAllOpCount; i++) {
      Operation op = fromOperations.get(i);
      assertSame(op.getHandlingNode(), toNode);
      assertFalse(op.isCancelled());
    }
  }
  
}
