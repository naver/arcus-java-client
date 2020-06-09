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

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.CacheMonitor.CacheMonitorListener;

import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class CacheMonitorTest extends MockObjectTestCase {

  private Mock watcher;
  private Mock listener;
  private ZooKeeper zooKeeper;
  private CacheMonitor cacheMonitor;
  private List<String> children;
  private static final String ARCUS_BASE_CACHE_LIST_ZPATH = "/arcus/cache_list/";

  private static final String serviceCode = "dev";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    listener = mock(CacheMonitorListener.class);
    watcher = mock(Watcher.class);
    zooKeeper = new ZooKeeper("", 15000, (Watcher) watcher.proxy()); // can't mock
    children = new ArrayList<String>();

    cacheMonitor = new CacheMonitor(zooKeeper, ARCUS_BASE_CACHE_LIST_ZPATH,
            serviceCode, (CacheMonitorListener) listener.proxy());
  }

  @Override
  public void tearDown() throws Exception {
    zooKeeper.close();
    super.tearDown();
  }

  public void testProcessResult() {
    // when
    children.add("0.0.0.0:11211");
    listener.expects(once()).method("commandNodeChange").with(eq(children));

    // test
    cacheMonitor.processResult(
        Code.OK.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);

    // then
    assertEquals(children, ((CacheMonitorListener) listener).getPrevCacheList());
  }

  public void testProcessResult_emptyChildren() {
    List<String> fakeChildren = new ArrayList<String>();
    fakeChildren.add("0.0.0.0:23456");

    // when : empty children
    listener.expects(once()).method("commandNodeChange").with(eq(fakeChildren));

    // test
    cacheMonitor.processResult(
        Code.OK.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);

    // then
    assertEquals(fakeChildren, ((CacheMonitorListener) listener).getPrevCacheList());
  }

  public void testProcessResult_otherEvents() {
    children.add("127.0.0.1:11211");
    listener.expects(never()).method("commandNodeChange");

    Code code;

    code = Code.NONODE;
    cacheMonitor.processResult(
        code.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);
    // do nothing

    code = Code.SESSIONEXPIRED;
    listener.expects(once()).method("closing");
    cacheMonitor.processResult(
        code.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);
    assertTrue(cacheMonitor.isDead());

    code = Code.NOAUTH;
    listener.expects(once()).method("closing");
    cacheMonitor.processResult(
        code.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);
    assertTrue(cacheMonitor.isDead());

    code = Code.CONNECTIONLOSS;
    cacheMonitor.processResult(
        code.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);

    code = Code.SESSIONMOVED;
    cacheMonitor.processResult(
        code.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);
  }

  public void testProcess_syncConnected() throws Exception {
    // when
    WatchedEvent event = new WatchedEvent(
        EventType.None, KeeperState.SyncConnected, ARCUS_BASE_CACHE_LIST_ZPATH + "/dev");

    // test
    cacheMonitor.process(event);

    // then
    // do nothing
  }

  public void testProcess_disconnected() throws Exception {
    // when
    WatchedEvent event = new WatchedEvent(
        EventType.None, KeeperState.Disconnected, ARCUS_BASE_CACHE_LIST_ZPATH + "/dev");

    // test
    cacheMonitor.process(event);

    // then
    // do nothing
  }

  public void testProcess_expired() throws Exception {
    // when
    WatchedEvent event = new WatchedEvent(
        EventType.None, KeeperState.Expired, ARCUS_BASE_CACHE_LIST_ZPATH + "/dev");
    listener.expects(once()).method("closing");

    // test
    cacheMonitor.process(event);

    // then
    assertTrue(cacheMonitor.isDead());
  }

  public void testProcess_nodeChildrenChanged() throws Exception {
    // when
    WatchedEvent event = new WatchedEvent(
        EventType.NodeChildrenChanged,
        KeeperState.SyncConnected, ARCUS_BASE_CACHE_LIST_ZPATH + "/dev");

    // test
    cacheMonitor.process(event);

    // then
    // do nothing
  }
}
