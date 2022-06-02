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
package net.spy.memcached;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import junit.framework.TestCase;
import net.spy.memcached.internal.ReconnDelay;
import org.junit.Assert;

/**
 * Test stuff that can be tested within a MemcachedConnection separately.
 */
public class MemcachedConnectionTest extends TestCase {

  private MemcachedConnection conn;
  private ArcusKetamaNodeLocator locator;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder().setReadBufferSize(1024);
    ConnectionFactory cf = cfb.build();
    List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();

    conn = new MemcachedConnection("connection test", cf, addrs,
        cf.getInitialObservers(), cf.getFailureMode(), cf.getOperationFactory());
    locator = (ArcusKetamaNodeLocator) conn.getLocator();
  }

  @Override
  protected void tearDown() throws Exception {
    conn.shutdown();
    super.tearDown();
  }

  public void testDebugBuffer() {
    String input = "this is a test _";
    ByteBuffer bb = ByteBuffer.wrap(input.getBytes());
    String s = MemcachedConnection.dbgBuffer(bb, input.length());
    assertEquals("this is a test \\x5f", s);
  }

  public void testNodesChangeQueue() throws Exception {
    // when
    conn.putNodesChangeQueue("0.0.0.0:11211");
    conn.putNodesChangeQueue("0.0.0.0:11211,0.0.0.0:11212,0.0.0.0:11213");
    conn.putNodesChangeQueue("0.0.0.0:11212");

    // 1st test (nodes=1)
    conn.handleNodesChangeQueue();

    // then
    assertTrue(1 == locator.getAll().size());

    // 2nd test (nodes=3)
    conn.handleNodesChangeQueue();

    // then
    assertTrue(3 == locator.getAll().size());

    // 3rd test (nodes=1)
    conn.handleNodesChangeQueue();

    // then
    assertTrue(1 == locator.getAll().size());
  }

  public void testNodesChangeQueue_empty() throws Exception {
    // when
    // on servers in the queue

    // test
    conn.handleNodesChangeQueue();

    // then
    assertTrue(0 == locator.getAll().size());
  }

  public void testNodesChangeQueue_invalid_addr() {
    try {
      // when : putting an invalid address
      conn.putNodesChangeQueue("");

      // test
      conn.handleNodesChangeQueue();

      // should not be here!
      //fail();
    } catch (Exception e) {
      e.printStackTrace();
      assertEquals("No hosts in list:  ``''", e.getMessage());
    }
  }

  public void testNodesChangeQueue_redundent() throws Exception {
    // when
    conn.putNodesChangeQueue("0.0.0.0:11211,0.0.0.0:11211");

    // test
    conn.handleNodesChangeQueue();

    // then
    assertTrue(2 == locator.getAll().size());
  }

  public void testNodesChangeQueue_twice() throws Exception {
    // when
    conn.putNodesChangeQueue("0.0.0.0:11211");
    conn.putNodesChangeQueue("0.0.0.0:11211");

    // test
    conn.handleNodesChangeQueue();

    // then
    assertTrue(1 == locator.getAll().size());
  }

  public void testAddOperations() throws Exception {
  }

  @SuppressWarnings("unchecked")
  public void testReconnectQueue_delayReconnect() throws Exception {
    MemcachedConnection.ReconnectQueue reconnectQueue = new MemcachedConnection.ReconnectQueue(1);

    Field reconMapField =
        MemcachedConnection.ReconnectQueue.class.getDeclaredField("reconMap");
    reconMapField.setAccessible(true);
    Map<MemcachedNode, Long> reconMap =
        (Map<MemcachedNode, Long>) reconMapField.get(reconnectQueue);

    Field reconSortedMapField =
        MemcachedConnection.ReconnectQueue.class.getDeclaredField("reconSortedMap");
    reconSortedMapField.setAccessible(true);
    SortedMap<Long, MemcachedNode> reconSortedMap =
        (SortedMap<Long, MemcachedNode>) reconSortedMapField.get(reconnectQueue);

    // add test
    MemcachedNode node = new MockMemcachedNode(
        InetSocketAddress.createUnresolved("1.1.1.1", 11211));
    long firstReconnectTime;
    reconnectQueue.add(node, ReconnDelay.DEFAULT);
    firstReconnectTime = reconMap.get(node);
    Assert.assertTrue(firstReconnectTime > System.nanoTime());
    Assert.assertEquals(reconMap.size(), 1);
    Assert.assertEquals(reconSortedMap.get(firstReconnectTime), node);
    Assert.assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    Assert.assertEquals(reconSortedMap.size(), 1);
    Assert.assertFalse(reconnectQueue.isEmpty());
    Assert.assertTrue(reconnectQueue.contains(node));

    // replace test with same node
    long secondReconnectTime;
    reconnectQueue.replace(node, ReconnDelay.DEFAULT);
    secondReconnectTime = reconMap.get(node);
    Assert.assertEquals(secondReconnectTime, firstReconnectTime);
    Assert.assertEquals(reconMap.size(), 1);
    Assert.assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    Assert.assertEquals(reconSortedMap.size(), 1);
    Assert.assertFalse(reconnectQueue.isEmpty());
    Assert.assertTrue(reconnectQueue.contains(node));

    // add test with another node
    MemcachedNode anotherNode = new MockMemcachedNode(
        InetSocketAddress.createUnresolved("2.2.2.2", 11211));
    long anotherReconnectTime;
    reconnectQueue.add(anotherNode, ReconnDelay.DEFAULT);
    anotherReconnectTime = reconMap.get(anotherNode);
    Assert.assertTrue(anotherReconnectTime > System.nanoTime());
    Assert.assertEquals(reconMap.size(), 2);
    Assert.assertEquals(reconSortedMap.get(anotherReconnectTime), anotherNode);
    Assert.assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    Assert.assertEquals(reconSortedMap.size(), 2);
    Assert.assertFalse(reconnectQueue.isEmpty());
    Assert.assertTrue(reconnectQueue.contains(anotherNode));

    // pop test
    Assert.assertNull(reconnectQueue.popReady(System.nanoTime()));
    Thread.sleep(reconnectQueue.getMinDelayMillis() + 10);
    Assert.assertEquals(reconnectQueue.popReady(System.nanoTime()), node);
    Thread.sleep(reconnectQueue.getMinDelayMillis() + 10);
    Assert.assertEquals(reconnectQueue.popReady(System.nanoTime()), anotherNode);
    Assert.assertNull(reconnectQueue.popReady(System.nanoTime()));

    // remove test
    reconnectQueue.add(node, ReconnDelay.DEFAULT);
    reconnectQueue.remove(node);
    Assert.assertFalse(reconMap.containsKey(node));
    Assert.assertEquals(reconMap.size(), 0);
    Assert.assertFalse(reconSortedMap.containsKey(firstReconnectTime));
    Assert.assertEquals(reconSortedMap.size(), 0);
    Assert.assertTrue(reconnectQueue.isEmpty());
    Assert.assertFalse(reconnectQueue.contains(node));
  }

  @SuppressWarnings("unchecked")
  public void testReconnectQueue_immediateReconnect() throws Exception {
    MemcachedConnection.ReconnectQueue reconnectQueue = new MemcachedConnection.ReconnectQueue(1);

    Field reconMapField =
        MemcachedConnection.ReconnectQueue.class.getDeclaredField("reconMap");
    reconMapField.setAccessible(true);
    Map<MemcachedNode, Long> reconMap =
        (Map<MemcachedNode, Long>) reconMapField.get(reconnectQueue);

    Field reconSortedMapField =
        MemcachedConnection.ReconnectQueue.class.getDeclaredField("reconSortedMap");
    reconSortedMapField.setAccessible(true);
    SortedMap<Long, MemcachedNode> reconSortedMap =
        (SortedMap<Long, MemcachedNode>) reconSortedMapField.get(reconnectQueue);

    // put test with delay reconnect
    MemcachedNode node = new MockMemcachedNode(
        InetSocketAddress.createUnresolved("1.1.1.1", 11211));
    long firstReconnectTime;
    reconnectQueue.add(node, ReconnDelay.DEFAULT);
    firstReconnectTime = reconMap.get(node);
    Assert.assertTrue(firstReconnectTime > System.nanoTime());
    Assert.assertEquals(reconMap.size(), 1);
    Assert.assertEquals(reconSortedMap.get(firstReconnectTime), node);
    Assert.assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    Assert.assertEquals(reconSortedMap.size(), 1);
    Assert.assertFalse(reconnectQueue.isEmpty());
    Assert.assertTrue(reconnectQueue.contains(node));
    Assert.assertNull(reconnectQueue.popReady(System.nanoTime()));

    // second put test with immediate reconnect and same node
    long secondReconnectTime;
    reconnectQueue.replace(node, ReconnDelay.IMMEDIATE);
    secondReconnectTime = reconMap.get(node);
    Assert.assertTrue(secondReconnectTime < firstReconnectTime);
    Assert.assertEquals(reconMap.size(), 1);
    Assert.assertFalse(reconSortedMap.containsKey(firstReconnectTime));
    Assert.assertEquals(reconSortedMap.firstKey(), (Long) secondReconnectTime);
    Assert.assertEquals(reconSortedMap.size(), 1);
    Assert.assertFalse(reconnectQueue.isEmpty());
    Assert.assertTrue(reconnectQueue.contains(node));
    Thread.sleep(10);
    Assert.assertEquals(reconnectQueue.popReady(System.nanoTime()), node);
  }
}
