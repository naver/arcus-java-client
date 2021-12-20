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
    ByteBuffer bb = ByteBuffer.wrap(KeyUtil.getKeyBytes(input));
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
}
