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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jmock.Expectations;
import org.jmock.Mockery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CacheMonitorTest {

  private Watcher watcher;
  private CacheMonitorListener listener;
  private ZooKeeper zooKeeper;
  private CacheMonitor cacheMonitor;
  private List<String> children;
  private Mockery context;
  private static final String ARCUS_BASE_CACHE_LIST_ZPATH = "/arcus/cache_list/";
  private static final String serviceCode = "dev";

  @BeforeEach
  protected void setUp() throws Exception {
    listener = context.mock(CacheMonitorListener.class);
    watcher = context.mock(Watcher.class);
    zooKeeper = new ZooKeeper("", 15000, watcher); // can't mock
    children = new ArrayList<>();

    cacheMonitor = new CacheMonitor(zooKeeper, ARCUS_BASE_CACHE_LIST_ZPATH,
            serviceCode, true, listener);
  }

  @AfterEach
  public void tearDown() throws Exception {
    zooKeeper.close();
    context.assertIsSatisfied();
  }

  @Test
  public void testProcessResult() {
    // when
    children.add("0.0.0.0:11211");
    context.checking(
        new Expectations() {{
          oneOf(listener).commandNodeChange(children);
        }}
    );

    // test
    cacheMonitor.processResult(
        Code.OK.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);

    // then
    assertEquals(children, listener.getPrevCacheList());
  }

  @Test
  public void testProcessResult_emptyChildren() {
    List<String> fakeChildren = new ArrayList<>();
    fakeChildren.add("0.0.0.0:23456");

    // when : empty children
    context.checking(
        new Expectations() {{
          oneOf(listener).commandNodeChange(fakeChildren);
        }}
    );

    // test
    cacheMonitor.processResult(
        Code.OK.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);

    // then
    assertEquals(fakeChildren, listener.getPrevCacheList());
  }

  @Test
  public void testProcessResult_otherEvents() {
    children.add("127.0.0.1:11211");
    context.checking(
        new Expectations() {{
          never(listener).commandNodeChange(children);
        }}
    );

    Code code;

    code = Code.NONODE;
    cacheMonitor.processResult(
        code.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);
    // do nothing

    code = Code.SESSIONEXPIRED;
    context.checking(
        new Expectations() {{
          oneOf(listener).closing();
        }}
    );
    cacheMonitor.processResult(
        code.intValue(), ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, null, children);
    assertTrue(cacheMonitor.isDead());

    code = Code.NOAUTH;
    context.checking(
        new Expectations() {{
          oneOf(listener).closing();
        }}
    );
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

  @Test
  public void testProcess_syncConnected() throws Exception {
    // when
    WatchedEvent event = new WatchedEvent(
        EventType.None, KeeperState.SyncConnected, ARCUS_BASE_CACHE_LIST_ZPATH + "/dev");

    // test
    cacheMonitor.process(event);

    // then
    // do nothing
  }

  @Test
  public void testProcess_disconnected() throws Exception {
    // when
    WatchedEvent event = new WatchedEvent(
        EventType.None, KeeperState.Disconnected, ARCUS_BASE_CACHE_LIST_ZPATH + "/dev");

    // test
    cacheMonitor.process(event);

    // then
    // do nothing
  }

  @Test
  public void testProcess_expired() throws Exception {
    // when
    WatchedEvent event = new WatchedEvent(
        EventType.None, KeeperState.Expired, ARCUS_BASE_CACHE_LIST_ZPATH + "/dev");
    context.checking(
            new Expectations() {{
              oneOf(listener).closing();
            }}
    );

    // test
    cacheMonitor.process(event);

    // then
    assertTrue(cacheMonitor.isDead());
  }

  @Test
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
