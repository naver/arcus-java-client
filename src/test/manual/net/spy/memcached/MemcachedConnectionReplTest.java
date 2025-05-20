/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2024 JaM2in Co., Ltd.
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MemcachedConnectionReplTest {

  private MemcachedConnection conn;
  private ArcusReplKetamaNodeLocator locator;

  @BeforeEach
  protected void setUp() throws Exception {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    cfb.setArcusReplEnabled(true);
    ConnectionFactory cf = cfb.build();
    List<InetSocketAddress> addrs = new ArrayList<>();

    conn = new MemcachedConnection("connection test", cf, addrs,
            cf.getInitialObservers(), cf.getFailureMode(), cf.getOperationFactory());
    conn.setArcusReplEnabled(true);
    locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
  }

  @AfterEach
  protected void tearDown() throws Exception {
    conn.shutdown();
  }

  @Test
  void testHandleCacheNodesChange() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    MemcachedReplicaGroup group = locator.getAllGroups().get("g0");
    assertNotNull(group);
    assertEquals(1, group.getSlaveNodes().size());

    MemcachedNode master = group.getMasterNode();
    MemcachedNode slave = group.getSlaveNodes().get(0);
    assertNotNull(master);
    assertNotNull(slave);

    SocketAddress masterAddr = master.getSocketAddress();
    SocketAddress slaveAddr = slave.getSocketAddress();
    assertEquals("{g0 M 127.0.0.1:11211}", masterAddr.toString());
    assertEquals("{g0 S 127.0.0.2:11211}", slaveAddr.toString());
  }

  @Test
  void testHandleCacheNodesChange_switchover() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^S^127.0.0.1:11211", "g0^S^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    MemcachedReplicaGroup group = locator.getAllGroups().get("g0");
    assertNotNull(group);
    assertEquals(1, group.getSlaveNodes().size());

    MemcachedNode master = group.getMasterNode();
    MemcachedNode slave = group.getSlaveNodes().get(0);
    assertNotNull(master);
    assertNotNull(slave);

    SocketAddress masterAddr = master.getSocketAddress();
    SocketAddress slaveAddr = slave.getSocketAddress();
    assertEquals("{g0 M 127.0.0.1:11211}", masterAddr.toString());
    assertEquals("{g0 S 127.0.0.2:11211}", slaveAddr.toString());

    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^S^127.0.0.1:11211", "g0^M^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    group = locator.getAllGroups().get("g0");
    assertNotNull(group);
    assertEquals(1, group.getSlaveNodes().size());

    master = group.getMasterNode();
    slave = group.getSlaveNodes().get(0);
    assertNotNull(master);
    assertNotNull(slave);

    masterAddr = master.getSocketAddress();
    slaveAddr = slave.getSocketAddress();
    assertEquals("{g0 M 127.0.0.2:11211}", masterAddr.toString());
    assertEquals("{g0 S 127.0.0.1:11211}", slaveAddr.toString());
  }

  @Test
  void testHandleCacheNodesChange_failover_master() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Collections.singletonList(
            "g0^S^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    MemcachedReplicaGroup group = locator.getAllGroups().get("g0");
    assertNotNull(group);
    assertEquals(1, group.getSlaveNodes().size());

    MemcachedNode master = group.getMasterNode();
    MemcachedNode slave = group.getSlaveNodes().get(0);
    assertNotNull(master);
    assertNotNull(slave);

    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Collections.singletonList(
            "g0^M^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(1, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    group = locator.getAllGroups().get("g0");
    assertNotNull(group);
    assertEquals(0, group.getSlaveNodes().size());

    master = group.getMasterNode();
    assertNotNull(master);

    SocketAddress masterAddr = master.getSocketAddress();
    assertEquals("{g0 M 127.0.0.2:11211}", masterAddr.toString());
  }

  @Test
  void testHandleCacheNodesChange_failover_slave() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Collections.singletonList(
            "g0^M^127.0.0.1:11211")));
    conn.handleCacheNodesChange();

    assertEquals(1, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    MemcachedReplicaGroup group = locator.getAllGroups().get("g0");
    assertNotNull(group);
    assertEquals(0, group.getSlaveNodes().size());

    MemcachedNode master = group.getMasterNode();
    assertNotNull(master);

    SocketAddress masterAddr = master.getSocketAddress();
    assertEquals("{g0 M 127.0.0.1:11211}", masterAddr.toString());
  }

  @Test
  void testHandleCacheNodesChange_failover_all() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211")));
    conn.handleCacheNodesChange();
    assertEquals(2, locator.getAll().size());
    assertEquals(1, locator.getAllGroups().size());

    conn.setCacheNodesChange(new ArrayList<>());
    conn.handleCacheNodesChange();

    assertEquals(0, locator.getAll().size());
    assertEquals(0, locator.getAllGroups().size());
  }

  @Test
  void testHandleCacheNodesChange_multiple_groups() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211",
            "g1^M^127.0.0.3:11211", "g1^S^127.0.0.4:11211")));
    conn.handleCacheNodesChange();
    assertEquals(4, locator.getAll().size());
    assertEquals(2, locator.getAllGroups().size());

    MemcachedReplicaGroup group1 = locator.getAllGroups().get("g0");
    assertNotNull(group1);
    assertEquals(1, group1.getSlaveNodes().size());

    MemcachedNode master1 = group1.getMasterNode();
    MemcachedNode slave1 = group1.getSlaveNodes().get(0);
    assertNotNull(master1);
    assertNotNull(slave1);

    SocketAddress master1Addr = master1.getSocketAddress();
    SocketAddress slave1Addr = slave1.getSocketAddress();
    assertEquals("{g0 M 127.0.0.1:11211}", master1Addr.toString());
    assertEquals("{g0 S 127.0.0.2:11211}", slave1Addr.toString());

    MemcachedReplicaGroup group2 = locator.getAllGroups().get("g1");
    assertNotNull(group2);
    assertEquals(1, group2.getSlaveNodes().size());

    MemcachedNode master2 = group2.getMasterNode();
    MemcachedNode slave2 = group2.getSlaveNodes().get(0);
    assertNotNull(master2);
    assertNotNull(slave2);

    SocketAddress master2Addr = master2.getSocketAddress();
    SocketAddress slave2Addr = slave2.getSocketAddress();
    assertEquals("{g1 M 127.0.0.3:11211}", master2Addr.toString());
    assertEquals("{g1 S 127.0.0.4:11211}", slave2Addr.toString());
  }

  @Test
  void testHandleCacheNodesChange_edge_case_1() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211",
            "g1^M^127.0.0.3:11211", "g1^S^127.0.0.4:11211")));
    conn.handleCacheNodesChange();
    assertEquals(4, locator.getAll().size());
    assertEquals(2, locator.getAllGroups().size());

    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g1^M^127.0.0.1:11211", "g1^S^127.0.0.2:11211",
            "g0^M^127.0.0.3:11211", "g0^S^127.0.0.4:11211")));
    conn.handleCacheNodesChange();
    assertEquals(4, locator.getAll().size());
    assertEquals(2, locator.getAllGroups().size());

    MemcachedReplicaGroup group1 = locator.getAllGroups().get("g0");
    assertNotNull(group1);
    assertEquals(1, group1.getSlaveNodes().size());

    MemcachedNode master1 = group1.getMasterNode();
    MemcachedNode slave1 = group1.getSlaveNodes().get(0);
    assertNotNull(master1);
    assertNotNull(slave1);

    SocketAddress master1Addr = master1.getSocketAddress();
    SocketAddress slave1Addr = slave1.getSocketAddress();
    assertEquals("{g0 M 127.0.0.3:11211}", master1Addr.toString());
    assertEquals("{g0 S 127.0.0.4:11211}", slave1Addr.toString());

    MemcachedReplicaGroup group2 = locator.getAllGroups().get("g1");
    assertNotNull(group2);
    assertEquals(1, group2.getSlaveNodes().size());

    MemcachedNode master2 = group2.getMasterNode();
    MemcachedNode slave2 = group2.getSlaveNodes().get(0);
    assertNotNull(master2);
    assertNotNull(slave2);

    SocketAddress master2Addr = master2.getSocketAddress();
    SocketAddress slave2Addr = slave2.getSocketAddress();
    assertEquals("{g1 M 127.0.0.1:11211}", master2Addr.toString());
    assertEquals("{g1 S 127.0.0.2:11211}", slave2Addr.toString());
  }

  @Test
  void testHandleCacheNodesChange_edge_case_2() throws IOException {
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g0^M^127.0.0.1:11211", "g0^S^127.0.0.2:11211",
            "g1^M^127.0.0.3:11211", "g1^S^127.0.0.4:11211")));
    conn.handleCacheNodesChange();
    assertEquals(4, locator.getAll().size());
    assertEquals(2, locator.getAllGroups().size());

    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(Arrays.asList(
            "g1^S^127.0.0.1:11211", "g1^M^127.0.0.2:11211",
            "g0^S^127.0.0.3:11211", "g0^M^127.0.0.4:11211")));
    conn.handleCacheNodesChange();
    assertEquals(4, locator.getAll().size());
    assertEquals(2, locator.getAllGroups().size());

    MemcachedReplicaGroup group1 = locator.getAllGroups().get("g0");
    assertNotNull(group1);
    assertEquals(1, group1.getSlaveNodes().size());

    MemcachedNode master1 = group1.getMasterNode();
    MemcachedNode slave1 = group1.getSlaveNodes().get(0);
    assertNotNull(master1);
    assertNotNull(slave1);

    SocketAddress master1Addr = master1.getSocketAddress();
    SocketAddress slave1Addr = slave1.getSocketAddress();
    assertEquals("{g0 M 127.0.0.4:11211}", master1Addr.toString());
    assertEquals("{g0 S 127.0.0.3:11211}", slave1Addr.toString());

    MemcachedReplicaGroup group2 = locator.getAllGroups().get("g1");
    assertNotNull(group2);
    assertEquals(1, group2.getSlaveNodes().size());

    MemcachedNode master2 = group2.getMasterNode();
    MemcachedNode slave2 = group2.getSlaveNodes().get(0);
    assertNotNull(master2);
    assertNotNull(slave2);

    SocketAddress master2Addr = master2.getSocketAddress();
    SocketAddress slave2Addr = slave2.getSocketAddress();
    assertEquals("{g1 M 127.0.0.2:11211}", master2Addr.toString());
    assertEquals("{g1 S 127.0.0.1:11211}", slave2Addr.toString());
  }
}
