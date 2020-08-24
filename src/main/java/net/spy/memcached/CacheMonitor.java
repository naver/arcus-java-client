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

import java.util.List;

import net.spy.memcached.compat.SpyObject;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * CacheMonitor monitors the changes of the cache server list
 * in the ZooKeeper node{@code (/arcus/cache_list/<service_code>)}.
 */
public class CacheMonitor extends SpyObject implements Watcher,
        ChildrenCallback {

  private final ZooKeeper zk;

  private final String cacheListZPath;

  private final String serviceCode;

  private volatile boolean dead;

  private final CacheMonitorListener listener;

  /**
   * Constructor
   *
   * @param zk              ZooKeeper connection
   * @param cacheListZPath  path of cache list znode
   * @param serviceCode     service code (or cloud name) to identify each cloud
   * @param listener        Callback listener
   */
  public CacheMonitor(ZooKeeper zk, String cacheListZPath, String serviceCode,
                      CacheMonitorListener listener) {
    this.zk = zk;
    this.cacheListZPath = cacheListZPath;
    this.serviceCode = serviceCode;
    this.listener = listener;

    getLogger().info("Initializing the CacheMonitor.");

    // Get the cache list from the Arcus admin asynchronously.
    // Returning list would be processed in processResult().
    asyncGetCacheList();
  }

  /**
   * Other classes use the CacheMonitor by implementing this method
   */
  public interface CacheMonitorListener {
    /**
     * The existing children of the node has changed.
     *
     * @param children new children node list
     */
    void commandCacheListChange(List<String> children);

    List<String> getPrevCacheList();

    /**
     * The ZooKeeper session is no longer valid.
     */
    void closing();
  }

  /**
   * Processes every events from the ZooKeeper.
   */
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.NodeChildrenChanged) {
      asyncGetCacheList();
    }
  }

  /**
   * A callback function to process the result of getChildren(watch=true).
   */
  public void processResult(int rc, String path, Object ctx,
                            List<String> children) {
    switch (Code.get(rc)) {
      case OK:
        listener.commandCacheListChange(children);
        return;
      case NONODE:
        getLogger().fatal(
            "Cannot find your service code. Please contact Arcus support to solve this problem. "
            + getInfo());
        return;
      case SESSIONEXPIRED:
        getLogger().warn("Session expired. Trying to reconnect to the Arcus admin. " + getInfo());
        shutdown();
        return;
      case NOAUTH:
        getLogger().fatal("Authorization failed " + getInfo());
        shutdown();
        return;
      case CONNECTIONLOSS:
        getLogger().warn("Connection lost. Trying to reconnect to the Arcus admin." + getInfo());
        asyncGetCacheList();
        return;
      default:
        getLogger().warn(
            "Ignoring an unexpected event from the Arcus admin. code="
            + Code.get(rc) + ", " + getInfo());
        asyncGetCacheList();
        return;
    }
  }

  /**
   * Get the cache list asynchronously from the Arcus admin.
   */
  void asyncGetCacheList() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Set a new watch on " + (cacheListZPath + serviceCode));
    }

    zk.getChildren(cacheListZPath + serviceCode, this, this, null);
  }

  /**
   * Shutdown the CacheMonitor.
   */
  public void shutdown() {
    if (!dead) {
      getLogger().info("Shutting down the CacheMonitor. " + getInfo());
      dead = true;
      listener.closing();
    }
  }

  /**
   * Set cache monitor is dead.
   */
  public void setDead() {
    if (!dead) {
      getLogger().info("Set CacheMonitor is dead. " + getInfo());
      dead = true;
    }
  }

  /**
   * Check if the cache monitor is dead.
   */
  public boolean isDead() {
    return dead;
  }

  private String getInfo() {
    String zkSessionId = null;
    if (zk != null) {
      zkSessionId = "0x" + Long.toHexString(zk.getSessionId());
    }
    return "[serviceCode=" + serviceCode + ", adminSessionId=" + zkSessionId + "]";
  }
}
