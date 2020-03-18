/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2020 JaM2in Co., Ltd.
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

import junit.framework.TestCase;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Test the TCPMemcachedNodeImpl
 */
public class TCPMemcachedNodeImplTest extends TestCase {

  @SuppressWarnings("unchecked")
  private Queue<Operation> getQueue(String queueFieldName, TCPMemcachedNodeImpl node) throws Exception {
    Field queueField = TCPMemcachedNodeImpl.class.getDeclaredField(queueFieldName);
    queueField.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(queueField, queueField.getModifiers() & ~Modifier.FINAL);

    return (Queue) queueField.get(node);
  }

  public void testMoveOperations() throws Exception {
    // given
    final int fromReadOpCount = 5,
              fromWriteOpCount= 5,
              fromInputOpCount = 10,
              fromAllOpCount = fromReadOpCount + fromWriteOpCount + fromInputOpCount,
              inputQueueSize = 15;

    final DefaultConnectionFactory factory = new DefaultConnectionFactory(inputQueueSize, 4096);

    TCPMemcachedNodeImpl fromNode = (TCPMemcachedNodeImpl) factory.createMemcachedNode(
      InetSocketAddress.createUnresolved("127.0.0.1", 11211),
      SocketChannel.open(),
     4096
    );

    TCPMemcachedNodeImpl toNode = (TCPMemcachedNodeImpl) factory.createMemcachedNode(
      InetSocketAddress.createUnresolved("127.0.0.2", 11211),
      SocketChannel.open(),
      4096
    );

    List<Operation> fromOperations = new LinkedList<Operation>() {{
      for (int i = 0; i < fromAllOpCount; i++) {
        Operation op = factory.getOperationFactory().get("cacheKey=" + i, new GetOperation.Callback() {
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
        add(op);
      }
    }};

    for (int i = 0; i < fromAllOpCount; i++) {
      if (fromNode.getReadQueueSize() < fromReadOpCount) {
        getQueue("readQ", fromNode).offer(fromOperations.get(i));
      } else if (fromNode.getWriteQueueSize() < fromWriteOpCount) {
        getQueue("writeQ", fromNode).offer(fromOperations.get(i));
      } else {
        getQueue("inputQueue", fromNode).offer(fromOperations.get(i));
      }
    }

    // when
    fromNode.moveOperations(toNode);

    // then
    assertEquals(0, fromNode.getInputQueueSize());
    assertEquals(0, fromNode.getWriteQueueSize());
    assertEquals(0, fromNode.getReadQueueSize());
    assertEquals(inputQueueSize, toNode.getInputQueueSize());

    for (int i = 0; i < fromAllOpCount; i++) {
      Operation op = fromOperations.get(i);
      if (i < inputQueueSize) {
        assertSame(op.getHandlingNode(), toNode);
        assertFalse(op.isCancelled());
      } else {
        assertTrue(op.isCancelled());
      }
    }
  }
  
}
