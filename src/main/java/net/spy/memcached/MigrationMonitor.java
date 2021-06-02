/*
 * arcus-java-client : Arcus Java client
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

import net.spy.memcached.compat.SpyObject;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.AbstractMap;
import java.util.List;

/**
 * MigrationMonitor monitors the changes of the cloud_stat
 * in the ZooKeeper node{@code (/arcus/cloud_stat/<service_code>)}.
 */
public class MigrationMonitor extends SpyObject implements Watcher {

  private final ZooKeeper zk;

  private final String alterListZPath;

  private final String cloudStatZPath;

  private final String serviceCode;

  private final AsyncCallback.Children2Callback cloudStatCallback;

  private final AsyncCallback.Children2Callback alterListCallback;

  private final MigrationMonitorListener listener;

  private MigrationType type = MigrationType.UNKNOWN;

  private MigrationState state = MigrationState.UNKNOWN;

  private volatile boolean dead;

  /**
   * Constructor
   *
   * @param zk             ZooKeeper Connection
   * @param cloudStatZPath path of cloud_stat znode
   * @param serviceCode    service code (or cloud name) to identity each cloud
   * @param listener       Callback listener
   */
  public MigrationMonitor(ZooKeeper zk, String cloudStatZPath,
                          String serviceCode, final MigrationMonitorListener listener) {
    this.zk = zk;
    this.cloudStatZPath = cloudStatZPath + serviceCode;
    this.alterListZPath = cloudStatZPath + serviceCode + "/alter_list";
    this.serviceCode = serviceCode;
    this.listener = listener;

    getLogger().info("Initializing the MigrationMonitor.");

    this.cloudStatCallback = new AsyncCallback.Children2Callback() {
      /**
       * A callback function to process the result of cloud_stat getChildren(watch=true).
       */
      @Override
      public void processResult(int rc, String s, Object o, List<String> list, Stat stat) {
        switch (Code.get(rc)) {
          case OK:
            commandCloudStatChange(list);
            return;
          case SESSIONEXPIRED:
            getLogger().warn("Session expired. Reconnect to the Arcus admin. "
                + getInfo());
            shutdown();
            return;
          default:
            getLogger().warn("Ignoring an unexpected event on cloud_stat. code="
                + Code.get(rc) + ", " + getInfo());
            asyncGetCloudStat();
            return;
        }
      }
    };
    this.alterListCallback = new AsyncCallback.Children2Callback() {
      /**
       * A callback function to process the result of alter_list getChildren(watch=true).
       */
      @Override
      public void processResult(int rc, String s, Object o, List<String> list, Stat stat) {
        switch (Code.get(rc)) {
          case OK:
            listener.commandAlterListChange(list);
            return;
          case SESSIONEXPIRED:
            getLogger().warn("Session expired. Reconnect to the Arcus admin. "
                + getInfo());
            shutdown();
            return;
          case NONODE:
            getLogger().fatal("Cannot find the alter_list znode. Stop watching. " + getInfo());
            return; /* stop watching */
          default:
            getLogger().warn("Ignoring an unexpected event on alter_list. code="
                    + Code.get(rc) + ", " + getInfo());
            asyncGetAlterList();
            return;
        }
      }
    };

    // Get the cloud_stat from the Arcus admin asynchronously.
    // Returning result would be processed in processResult().
    asyncGetCloudStat();
  }

  /**
   * Processes every events from the ZooKeeper.
   */
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.NodeChildrenChanged) {
      String path = event.getPath();
      if (path != null && path.equals(cloudStatZPath)) {
        asyncGetCloudStat();
      } else if (path != null && path.equals(alterListZPath)) {
        asyncGetAlterList();
      }
    }
  }

  private AbstractMap.SimpleEntry<MigrationType, MigrationState> validateCloudStatZNodes(
      List<String> children) {
    /* There are following znodes in cloud_stat :
     * INTERNAL, STATE, alter_list
     */
    int child_count = 3;
    int valid_count = 0;
    if (children.size() != child_count) {
      return null;
    }

    MigrationType type = MigrationType.UNKNOWN;
    MigrationState state = MigrationState.UNKNOWN;

    for (String znode : children) {
      if (znode.equals("INTERNAL") || znode.equals("alter_list")) {
        valid_count++;
        continue;
      }

      /* STATE znode format:
       * <STATE>^<JOIN|LEAVE>^<BEGIN|PREPARED|DONE>
       */
      if (znode.startsWith("STATE^")) {
        String[] tokens = znode.split("\\^");
        if (tokens.length < 3) {
          break;
        }
        try {
          type = MigrationType.valueOf(tokens[1]);
          state = MigrationState.valueOf(tokens[2]);
          valid_count++;
        } catch (IllegalArgumentException e) {
          return null;
        }
      }
    }
    if (valid_count != child_count) {
      getLogger().warn("Invalid cloud_stat znodes. zpath=" + cloudStatZPath);
      return null;
    }
    return new AbstractMap.SimpleEntry<MigrationType, MigrationState>(type, state);
  }

  private void commandCloudStatChange(List<String> children) {
    if (children.size() == 0) {
      resetMigrationTypeAndState();
      return;
    }

    AbstractMap.SimpleEntry<MigrationType, MigrationState> typeAndState =
        validateCloudStatZNodes(children);
    if (typeAndState == null) {
      return;
    }
    MigrationType newType = typeAndState.getKey();
    MigrationState newState = typeAndState.getValue();
    getLogger().info("MigrationMonitor reads cloud_stat. type=" + newType + ", state=" + newState);

    if (newState == MigrationState.DONE) {
      getLogger().warn("Migration state is DONE. reset type and state.");
      resetMigrationTypeAndState();
      return;
    }

    if (type != MigrationType.UNKNOWN && type != newType) {
      getLogger().error("The cloud_stat type mismatch. curType=" + type +
          ", newType=" + newType + ", zpath=" + cloudStatZPath);
      resetMigrationTypeAndState();
      return;
    }

    if (newState == MigrationState.BEGIN) {
      /* All alter_list is not yet registered. */
      setMigrationTypeAndState(newType, newState);
    } else if (newState == MigrationState.PREPARED) {
      /* All alter_list has been registered.
       * Read the alter_list and pass it to the IO Thread to prepare for the migration.
       */
      if (state == MigrationState.UNKNOWN || state == MigrationState.BEGIN) {
        setMigrationTypeAndState(newType, newState);
      }
      asyncGetAlterList();
    }
  }

  private void setMigrationTypeAndState(MigrationType type, MigrationState state) {
    this.type = type;
    this.state = state;
    listener.setMigrationTypeAndState(type, state);
  }

  private void resetMigrationTypeAndState() {
    this.type = MigrationType.UNKNOWN;
    this.state = MigrationState.UNKNOWN;
    listener.setMigrationTypeAndState(type, state);
  }

  /**
   * Get the cloud_stat asynchronously from the Arcus admin.
   */
  private void asyncGetCloudStat() {
    String zpath = cloudStatZPath;
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Set a new watch on " + zpath);
    }
    zk.getChildren(zpath, this, cloudStatCallback, null);
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
   * Other classes use the MigrationMonitor by implementing this method
   */
  public interface MigrationMonitorListener {
    /**
     * The children of the alter_list has changed.
     *
     * @param children new children node list
     */
    void commandAlterListChange(List<String> children);
    /**
     *
     * @param type   migration type
     * @param state  migration state
     */
    void setMigrationTypeAndState(MigrationType type, MigrationState state);
    /**
     * The ZooKeeper session is no longer valid.
     */
    void closing();
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
   * Check if the MigrationMonitor is dead.
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
