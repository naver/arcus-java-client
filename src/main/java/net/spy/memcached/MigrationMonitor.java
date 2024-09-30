/*
 * arcus-java-client : Arcus Java client
 * Copyright 2014-2023 JaM2in Co., Ltd.
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
/* ENABLE_MIGRATION if */
package net.spy.memcached;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import net.spy.memcached.compat.SpyObject;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * MigrationMonitor monitors the changes of the cloud_stat
 * in the ZooKeeper node{@code (/arcus/cloud_stat/<service_code>)}.
 */
public final class MigrationMonitor extends SpyObject implements Watcher {

  private final ZooKeeper zk;

  private final String alterListZPath;

  private final String cloudStatZPath;

  private final String serviceCode;

  private volatile boolean dead = false;

  private final MigrationMonitorListener listener;

  private final AsyncCallback.Children2Callback cloudStatCallback;

  private final AsyncCallback.Children2Callback alterListCallback;

  private final CountDownLatch migrationInitLatch;

  /**
   * Constructor
   *
   * @param zk             ZooKeeper Connection
   * @param cloudStatZPath path of cloud_stat znode
   * @param serviceCode    service code (or cloud name) to identity each cloud
   * @param listener       Callback listener
   */
  public MigrationMonitor(ZooKeeper zk, String cloudStatZPath,
                          String serviceCode, boolean startup,
                          final MigrationMonitorListener listener) throws InterruptedException {
    this.zk = zk;
    this.cloudStatZPath = cloudStatZPath + serviceCode;
    this.alterListZPath = cloudStatZPath + serviceCode + "/alter_list";
    this.serviceCode = serviceCode;
    this.listener = listener;

    this.cloudStatCallback = (rc, s, o, list, stat) -> processCloudStatResult(rc, list);
    this.alterListCallback = (rc, s, o, list, stat) -> processAlterListResult(rc, list);

    getLogger().info("Initializing the MigrationMonitor.");

    if (startup) {
      this.migrationInitLatch = new CountDownLatch(1);
    } else {
      this.migrationInitLatch = new CountDownLatch(0);
    }

    // Get the cloud_stat from the Arcus admin asynchronously.
    // Returning result would be processed in processResult().
    asyncGetCloudStat();

    if (startup) {
      this.migrationInitLatch.await();
    }
  }

  /**
   * Other classes use the MigrationMonitor by implementing this method
   */
  public interface MigrationMonitorListener {

    boolean commandCloudStatChange(List<String> children);
    /**
     * The children of the alter_list has changed.
     *
     * @param children new children node list
     */
    boolean commandAlterListChange(List<String> children);

    /**
     * The ZooKeeper session is no longer valid.
     */
    void closing();
  }

  /**
   * Processes every event from the ZooKeeper.
   */
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.NodeChildrenChanged) {
      String path = event.getPath();
      if (cloudStatZPath.equals(path)) {
        asyncGetCloudStat();
      } else if (alterListZPath.equals(path)) {
        asyncGetAlterList();
      }
    }
  }

  /**
   * A callback function to process the result of cloud_stat getChildren(watch=true).
   */
  public void processCloudStatResult(int rc, List<String> list) {
    boolean doCountDown = true;

    switch (Code.get(rc)) {
      case OK:
        if (listener.commandCloudStatChange(list)) {
          asyncGetAlterList();
          doCountDown = false;
        }
        break;
      case NONODE:
        getLogger().fatal("Cannot find the cloud_stat znode. Stop watching. " + getInfo());
        shutdown();
        break;
      case SESSIONEXPIRED:
        getLogger().warn("Session expired. Reconnect to the Arcus admin. " + getInfo());
        shutdown();
        break;
      case NOAUTH:
        getLogger().fatal("Authorization failed " + getInfo());
        shutdown();
        break;
      case CONNECTIONLOSS:
        getLogger().warn("Connection lost. Trying to reconnect to the Arcus admin." + getInfo());
        asyncGetCloudStat();
        doCountDown = false;
        break;
      default:
        getLogger().warn("Ignoring an unexpected event on cloud_stat. code="
            + Code.get(rc) + ", " + getInfo());
        asyncGetCloudStat();
        doCountDown = false;
        break;
    }

    countDownMigrationInitLatch(doCountDown);
  }

  /**
   * A callback function to process the result of alter_list getChildren(watch=true).
   */
  public void processAlterListResult(int rc, List<String> list) {
    boolean doCountDown = true;
    // process alter_list iif STATE == PREPARED.

    switch (Code.get(rc)) {
      case OK:
        if (!listener.commandAlterListChange(list)) {
          getLogger().fatal("Cannot initialize alter_list znode.");
          shutdown();
        }
        break;
      case NONODE:
        getLogger().info("Cannot find the alter_list znode. Stop watching. " + getInfo());
        break;
      case SESSIONEXPIRED:
        getLogger().warn("Session expired. Reconnect to the Arcus admin. " + getInfo());
        shutdown();
        break;
      case NOAUTH:
        getLogger().fatal("Authorization failed " + getInfo());
        shutdown();
        break;
      case CONNECTIONLOSS:
        getLogger().warn(
            "Connection lost. Trying to reconnect to the Arcus admin." + getInfo());
        asyncGetAlterList();
        doCountDown = false;
        break;
      default:
        getLogger().warn("Ignoring an unexpected event on alter_list. code="
            + Code.get(rc) + ", " + getInfo());
        asyncGetAlterList();
        doCountDown = false;
        break;
    }

    countDownMigrationInitLatch(doCountDown);
  }

  /**
   * Get the cloud_stat asynchronously from the Arcus admin.
   */
  private void asyncGetCloudStat() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Set a new watch on " + cloudStatZPath);
    }
    zk.getChildren(cloudStatZPath, this, cloudStatCallback, null);
  }

  /**
   * Get the alter_list asynchronously from the Arcus admin.
   */
  private void asyncGetAlterList() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Set a new watch on " + alterListZPath);
    }
    zk.getChildren(alterListZPath, this, alterListCallback, null);
  }

  /**
   * Shutdown the MigrationMonitor.
   */
  public void shutdown() {
    if (!dead) {
      getLogger().info("Shutting down the MigrationMonitor. " + getInfo());
      dead = true;
      listener.closing();
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
    return "[monitor=MigrationMonitor" +
        ", serviceCode=" + serviceCode +
        ", adminSessionId=" + zkSessionId + "]";
  }

  private void countDownMigrationInitLatch(boolean doCountDown) {
    if (doCountDown && migrationInitLatch.getCount() > 0) {
      migrationInitLatch.countDown();
    }
  }

}
/* ENABLE_MIGRATION end */
