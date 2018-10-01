/*
 * arcus-java-client : Arcus Java client
 * Copyright 2017 JaM2in Co., Ltd.
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
import net.spy.memcached.internal.MigrationMode;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class MigrationMonitor extends SpyObject {
  private final static String joiningListPath = "joining_list";

  private final static String leavingListPath = "leaving_list";

  private final static String migrationsPath = "INTERNAL/migrations";

  private ZooKeeper zk;

  private String cloudStatZpath;

  private String serviceCode;

  volatile boolean dead;

  private MigrationMonitorListener listener;

  private MigrationWatcher cloudStatWatcher;

  private MigrationWatcher alterListWatcher;

  private MigrationWatcher migrationsWatcher;

  private MigrationMode mode;

  /**
   * Constructor
   *
   * @param zk             ZooKeeper connection
   * @param cloudStatZpath ZooKeeper cloud_stat directory path
   * @param serviceCode    service code (or cloud name) to identify each cloud
   * @param listener       Callback listener
   */
  public MigrationMonitor(ZooKeeper zk, String cloudStatZpath, String serviceCode,
                          final MigrationMonitorListener listener) {
    this.zk = zk;
    this.cloudStatZpath = cloudStatZpath;
    this.serviceCode = serviceCode;
    this.listener = listener;
    this.cloudStatWatcher = new MigrationWatcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
          asyncGetMigrationStatus();
        }
      }

      @Override
      public void processResult(int rc, String s, Object o, List<String> list) {
        Code code = Code.get(rc);
        if (code == Code.OK) {
          //set a new Watcher for joining list or leaving list
          setMigrationMode(list);
        } else if (code == Code.NONODE) {
          getLogger().warn("Cloud_stat zpath is deleted." + getInfo());
          shutdown();
        } else {
          handleCallbackError(code);
        }
      }
    };
    this.alterListWatcher = new MigrationWatcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
          asyncGetAlterList();
        }
      }

      @Override
      public void processResult(int rc, String s, Object o, List<String> list) {
        Code code = Code.get(rc);
        if (code == Code.OK) {
          listener.commandAlterNodeChange(list, mode);
        } else if (code == Code.NONODE) {
          if (mode != MigrationMode.Init) {
            mode = MigrationMode.Init;
            listener.initialMigrationNodeChange(mode);
          }
        } else {
          handleCallbackError(code);
        }
      }
    };
    this.migrationsWatcher = new MigrationWatcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
          asyncGetMigrationsList();
        }
      }

      @Override
      public void processResult(int rc, String s, Object o, List<String> list) {
        Code code = Code.get(rc);
        if (code == Code.OK) {
          listener.commandMigrationsZNodeChange(list);
        } else if (code == Code.NONODE) {
          if (mode != MigrationMode.Init) {
            mode = MigrationMode.Init;
            listener.initialMigrationNodeChange(mode);
          }
        } else {
          handleCallbackError(code);
        }
      }
    };
    getLogger().info("Initializing the MigrationMonitor");
    mode = MigrationMode.Init;
    asyncGetMigrationStatus();
  }

  public interface MigrationMonitorListener {

    void commandAlterNodeChange(List<String> children, MigrationMode mode);

    void commandMigrationsZNodeChange(List<String> migrations);

    void initialMigrationNodeChange(MigrationMode mode);

    void commandMigrationVersionChange(long version);

    void closing();
  }

  private interface MigrationWatcher extends Watcher, AsyncCallback.ChildrenCallback {
  }

  private void handleCallbackError(Code code) {
    switch (code) {
      case SESSIONEXPIRED:
        getLogger().warn("Session expired. Trying to reconnect to the Arcus admin. " + getInfo());
        shutdown();
        break;
      case NOAUTH:
        getLogger().fatal("Authorization failed " + getInfo());
        shutdown();
        break;
      case CONNECTIONLOSS:
        getLogger().warn("Connection lost. Trying to reconnect to the Arcus admin." + getInfo());
        break;
      default:
        getLogger().warn("Ignoring an unexpected event from the Arcus admin. code=" + code + ", " + getInfo());
        break;
    }
  }

  private void asyncGetMigrationStatus() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Set a new watch on " + (cloudStatZpath + serviceCode) + " for Migration");
    }
    zk.getChildren(cloudStatZpath + serviceCode, cloudStatWatcher, cloudStatWatcher, null);
  }

  private void setMigrationMode(List<String> children) {
    if (children.size() == 3) {
      boolean prepared = false;
      boolean done = false;
      long clusterVersion = -1;

      for (String str : children) {
        String[] split = str.split("\\^");
        if (split.length >= 2) {
          if (split[1].equals("PREPARED")) {
            clusterVersion = Long.valueOf(split[3]);
            prepared = true;
            break;
          } else if (split[1].equals("DONE")) {
            clusterVersion = Long.valueOf(split[2]);
            done = true;
            break;
          }
        }
      }

      if (prepared) {
        if (children.contains(joiningListPath)) {
          mode = MigrationMode.Join;
          asyncGetAlterList();
          asyncGetMigrationsList();
        } else if (children.contains(leavingListPath)) {
          mode = MigrationMode.Leave;
          asyncGetAlterList();
          asyncGetMigrationsList();
        } else {
          getLogger().fatal("Migration alterList ZK directory error.");
          shutdown();
        }
        getLogger().info("Migration is prepared.");
        assert clusterVersion != -1;
        listener.commandMigrationVersionChange(clusterVersion);
      }

      if (done) {
        if (mode != MigrationMode.Init) {
          assert clusterVersion != -1;

          mode = MigrationMode.Init;
          listener.initialMigrationNodeChange(mode);
          listener.commandMigrationVersionChange(clusterVersion);
        }
      }
    } else {
      if (mode != MigrationMode.Init) {
        mode = MigrationMode.Init;
        listener.initialMigrationNodeChange(mode);
      }
    }
  }

  private void asyncGetAlterList() {
    switch (mode) {
      case Join:
        zk.getChildren(cloudStatZpath + serviceCode + "/" + joiningListPath, alterListWatcher, alterListWatcher, null);
        break;
      case Leave:
        zk.getChildren(cloudStatZpath + serviceCode + "/" + leavingListPath, alterListWatcher, alterListWatcher, null);
        break;
      case Init:
        break;
      default:
        getLogger().error("Unexpected Migration Mode : " + mode);
        shutdown();
    }
  }

  private void asyncGetMigrationsList() {
    zk.getChildren(cloudStatZpath + serviceCode + "/" + migrationsPath, migrationsWatcher, migrationsWatcher, null);
  }

  public void shutdown() {
    getLogger().info("Shutting down the MigrationMonitor. " + getInfo());
    dead = true;
    if (listener != null) {
      listener.closing();
    }
  }

  private String getInfo() {
    String zkSessionId = null;
    if (zk != null) {
      zkSessionId = "0x" + Long.toHexString(zk.getSessionId());
    }

    return "[serviceCode=" + serviceCode + ", adminSessionId=" + zkSessionId + "]";
  }
}
