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
package net.spy.memcached;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import net.spy.memcached.internal.ZnodeType;

/**
 * Test stuff that can be tested within a MemcachedConnection separately.
 */
public class MemcachedConnectionTest extends TestCase {

  private MemcachedConnection conn;
  private ArcusKetamaNodeLocator locator;

  @Override
  protected void setUp() throws Exception {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    ConnectionFactory cf = cfb.build();
    List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();

    conn = new MemcachedConnection(1024, cf, addrs, cf.getInitialObservers(), cf.getFailureMode(), cf.getOperationFactory());
    locator = (ArcusKetamaNodeLocator) conn.getLocator();
  }

  @Override
  protected void tearDown() throws Exception {
    conn.shutdown();
  }

  public void testDebugBuffer() throws Exception {
    String input = "this is a test _";
    ByteBuffer bb = ByteBuffer.wrap(input.getBytes());
    String s = MemcachedConnection.dbgBuffer(bb, input.length());
    assertEquals("this is a test \\x5f", s);
  }

  public void testNodeManageQueue() throws Exception {
    /* ENABLE_MIGRATION if */
    // when
    conn.putZnodeQueue(ZnodeType.CacheList, "0.0.0.0:11211");
    conn.putZnodeQueue(ZnodeType.CacheList, "0.0.0.0:11211,0.0.0.0:11212,0.0.0.0:11213");
    conn.putZnodeQueue(ZnodeType.CacheList, "0.0.0.0:11212");

    // 1st test (nodes=1)
    conn.handleZnodeManageQueue();

    // then
    assertTrue(1 == locator.allNodes.size());

    // 2nd test (nodes=3)
    conn.handleZnodeManageQueue();

    // then
    assertTrue(3 == locator.allNodes.size());

    // 3rd test (nodes=1)
    conn.handleZnodeManageQueue();

    // then
    assertTrue(1 == locator.allNodes.size());
    /* else */
    /*
    // when
    conn.putMemcachedQueue("0.0.0.0:11211");
    conn.putMemcachedQueue("0.0.0.0:11211,0.0.0.0:11212,0.0.0.0:11213");
    conn.putMemcachedQueue("0.0.0.0:11212");

    // 1st test (nodes=1)
    conn.handleNodeManageQueue();

    // then
    assertTrue(1 == locator.allNodes.size());

    // 2nd test (nodes=3)
    conn.handleNodeManageQueue();

    // then
    assertTrue(3 == locator.allNodes.size());

    // 3rd test (nodes=1)
    conn.handleNodeManageQueue();

    // then
    assertTrue(1 == locator.allNodes.size());
    */
    /* ENABLE_MIGRATION end */
  }

  public void testNodeManageQueue_empty() throws Exception {
    // when
    // on servers in the queue

    // test
    /* ENABLE_MIGRATION if */
    conn.handleZnodeManageQueue();

    // then
    assertTrue(0 == locator.allNodes.size());
    /* else */
    /*
    conn.handleNodeManageQueue();

    // then
    assertTrue(0 == locator.allNodes.size());
    */
    /* ENABLE_MIGRATION end */
  }

  public void testNodeManageQueue_invalid_addr() throws Exception {
    try {
      // when : putting an invalid address
      /* ENABLE_MIGRATION if */
      conn.putZnodeQueue(ZnodeType.CacheList, "");

      // test
      conn.handleZnodeManageQueue();
      /* else */
      /*
      conn.putMemcachedQueue("");

      // test
      conn.handleNodeManageQueue();
      */
      /* ENABLE_MIGRATION end */

      // should not be here!
      //fail();
    } catch (Exception e) {
      e.printStackTrace();
      assertEquals("No hosts in list:  ``''", e.getMessage());
    }
  }

  public void testNodeManageQueue_redundent() throws Exception {
    /* ENABLE_MIGRATION if */
    // when
    conn.putZnodeQueue(ZnodeType.CacheList, "0.0.0.0:11211,0.0.0.0:11211");

    // test
    conn.handleZnodeManageQueue();

    // then
    assertTrue(2 == locator.allNodes.size());
    /* else */
    /*
    // when
    conn.putMemcachedQueue("0.0.0.0:11211,0.0.0.0:11211");

    // test
    conn.handleNodeManageQueue();

    // then
    assertTrue(2 == locator.allNodes.size());
    */
    /* ENABLE_MIGRATION end */
  }

  public void testNodeManageQueue_twice() throws Exception {
    /* ENABLE_MIGRATION if */
    // when
    conn.putZnodeQueue(ZnodeType.CacheList, "0.0.0.0:11211");
    conn.putZnodeQueue(ZnodeType.CacheList, "0.0.0.0:11211");

    // test
    conn.handleZnodeManageQueue();

    // then
    assertTrue(1 == locator.allNodes.size());
    /* else */
    /*
    // when
    conn.putMemcachedQueue("0.0.0.0:11211");
    conn.putMemcachedQueue("0.0.0.0:11211");

    // test
    conn.handleNodeManageQueue();

    // then
    assertTrue(1 == locator.allNodes.size());
    */
    /* ENABLE_MIGRATION end */
  }

  public void testAddOperations() throws Exception {

  }
}
