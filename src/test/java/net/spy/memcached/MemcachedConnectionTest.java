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

import net.spy.memcached.internal.ReconnDelay;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test stuff that can be tested within a MemcachedConnection separately.
 */
public class MemcachedConnectionTest {

  private MemcachedConnection conn;
  private ArcusKetamaNodeLocator locator;

  @BeforeEach
  protected void setUp() throws Exception {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder().setReadBufferSize(1024);
    ConnectionFactory cf = cfb.build();
    List<InetSocketAddress> addrs = new ArrayList<>();

    conn = new MemcachedConnection("connection test", cf, addrs,
        cf.getInitialObservers(), cf.getFailureMode(), cf.getOperationFactory());
    locator = (ArcusKetamaNodeLocator) conn.getLocator();
  }

  @AfterEach
  protected void tearDown() throws Exception {
    conn.shutdown();
  }

  @Test
  public void testDebugBuffer() {
    String input = "this is a test _";
    ByteBuffer bb = ByteBuffer.wrap(input.getBytes());
    String s = MemcachedConnection.dbgBuffer(bb, input.length());
    assertEquals("this is a test \\x5f", s);
  }

  @Test
  public void testNodesChangeQueue() throws Exception {
    // when
    conn.setCacheNodesChange("0.0.0.0:11211");

    // 1st test (nodes=1)
    conn.handleCacheNodesChange();

    // then
    assertTrue(1 == locator.getAll().size());

    // when
    conn.setCacheNodesChange("0.0.0.0:11211,0.0.0.0:11212,0.0.0.0:11213");

    // 2nd test (nodes=3)
    conn.handleCacheNodesChange();

    // then
    assertTrue(3 == locator.getAll().size());

    // when
    conn.setCacheNodesChange("0.0.0.0:11212");

    // 3rd test (nodes=1)
    conn.handleCacheNodesChange();

    // then
    assertTrue(1 == locator.getAll().size());
  }

  @Test
  public void testNodesChangeQueue_empty() throws Exception {
    // when
    // on servers in the queue

    // test
    conn.handleCacheNodesChange();

    // then
    assertTrue(0 == locator.getAll().size());
  }

  @Test
  public void testNodesChangeQueue_invalid_addr() {
    try {
      // when : putting an invalid address
      conn.setCacheNodesChange("");

      // test
      conn.handleCacheNodesChange();

      // should not be here!
      //fail();
    } catch (Exception e) {
      e.printStackTrace();
      assertEquals("No hosts in list:  ``''", e.getMessage());
    }
  }

  @Test
  public void testNodesChangeQueue_redundant() throws Exception {
    // when
    conn.setCacheNodesChange("0.0.0.0:11211,0.0.0.0:11211");

    // test
    conn.handleCacheNodesChange();

    // then
    assertTrue(2 == locator.getAll().size());
  }

  @Test
  public void testNodesChangeQueue_twice() throws Exception {
    // when
    conn.setCacheNodesChange("0.0.0.0:11211");
    conn.setCacheNodesChange("0.0.0.0:11211");

    // test
    conn.handleCacheNodesChange();

    // then
    assertTrue(1 == locator.getAll().size());
  }

  @Test
  public void testAddOperations() throws Exception {
  }

  @SuppressWarnings("unchecked")
  @Test
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
    assertTrue(firstReconnectTime > System.nanoTime());
    assertEquals(reconMap.size(), 1);
    assertEquals(reconSortedMap.get(firstReconnectTime), node);
    assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    assertEquals(reconSortedMap.size(), 1);
    assertFalse(reconnectQueue.isEmpty());
    assertTrue(reconnectQueue.contains(node));

    // replace test with same node
    long secondReconnectTime;
    reconnectQueue.replace(node, ReconnDelay.DEFAULT);
    secondReconnectTime = reconMap.get(node);
    assertEquals(secondReconnectTime, firstReconnectTime);
    assertEquals(reconMap.size(), 1);
    assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    assertEquals(reconSortedMap.size(), 1);
    assertFalse(reconnectQueue.isEmpty());
    assertTrue(reconnectQueue.contains(node));

    // add test with another node
    MemcachedNode anotherNode = new MockMemcachedNode(
        InetSocketAddress.createUnresolved("2.2.2.2", 11211));
    long anotherReconnectTime;
    reconnectQueue.add(anotherNode, ReconnDelay.DEFAULT);
    anotherReconnectTime = reconMap.get(anotherNode);
    assertTrue(anotherReconnectTime > System.nanoTime());
    assertEquals(reconMap.size(), 2);
    assertEquals(reconSortedMap.get(anotherReconnectTime), anotherNode);
    assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    assertEquals(reconSortedMap.size(), 2);
    assertFalse(reconnectQueue.isEmpty());
    assertTrue(reconnectQueue.contains(anotherNode));

    // pop test
    assertNull(reconnectQueue.popReady(System.nanoTime()));
    Thread.sleep(reconnectQueue.getMinDelayMillis() + 10);
    assertEquals(reconnectQueue.popReady(System.nanoTime()), node);
    Thread.sleep(reconnectQueue.getMinDelayMillis() + 10);
    assertEquals(reconnectQueue.popReady(System.nanoTime()), anotherNode);
    assertNull(reconnectQueue.popReady(System.nanoTime()));

    // remove test
    reconnectQueue.add(node, ReconnDelay.DEFAULT);
    reconnectQueue.remove(node);
    assertFalse(reconMap.containsKey(node));
    assertEquals(reconMap.size(), 0);
    assertFalse(reconSortedMap.containsKey(firstReconnectTime));
    assertEquals(reconSortedMap.size(), 0);
    assertTrue(reconnectQueue.isEmpty());
    assertFalse(reconnectQueue.contains(node));
  }

  @SuppressWarnings("unchecked")
  @Test
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
    assertTrue(firstReconnectTime > System.nanoTime());
    assertEquals(reconMap.size(), 1);
    assertEquals(reconSortedMap.get(firstReconnectTime), node);
    assertEquals(reconSortedMap.firstKey(), (Long) firstReconnectTime);
    assertEquals(reconSortedMap.size(), 1);
    assertFalse(reconnectQueue.isEmpty());
    assertTrue(reconnectQueue.contains(node));
    assertNull(reconnectQueue.popReady(System.nanoTime()));

    // second put test with immediate reconnect and same node
    long secondReconnectTime;
    reconnectQueue.replace(node, ReconnDelay.IMMEDIATE);
    secondReconnectTime = reconMap.get(node);
    assertTrue(secondReconnectTime < firstReconnectTime);
    assertEquals(reconMap.size(), 1);
    assertFalse(reconSortedMap.containsKey(firstReconnectTime));
    assertEquals(reconSortedMap.firstKey(), (Long) secondReconnectTime);
    assertEquals(reconSortedMap.size(), 1);
    assertFalse(reconnectQueue.isEmpty());
    assertTrue(reconnectQueue.contains(node));
    Thread.sleep(10);
    assertEquals(reconnectQueue.popReady(System.nanoTime()), node);
  }
}
