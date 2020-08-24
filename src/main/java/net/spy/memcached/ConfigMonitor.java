/*
 * arcus-java-client : Arcus Java client
 * Copyright 2020 JaM2in Co., Ltd.
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

import net.spy.memcached.compat.SpyObject;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.util.ConfigUtils;

/**
 * ConfigMonitor monitors the changes of the zk server ensemble list
 * in the ZooKeeper node(/zookeeper/config).
 */
public class ConfigMonitor extends SpyObject implements Watcher,
        DataCallback {

  private final ZooKeeper zk;

  private Long configVersion;

  private String prevConfigString;

  private volatile boolean dead;

  private final ConfigMonitorListener listener;

  /**
   * Constructor
   *
   * @param zk          ZooKeeper connection
   * @param listener    Callback listener
   */
  public ConfigMonitor(ZooKeeper zk, ConfigMonitorListener listener) {
    this.zk = zk;
    this.listener = listener;

    getLogger().info("Initializing the ConfigMonitor.");

    // Get the config from the Arcus admin synchronously.
    // Returning list would be processed in processResult().
    syncGetConfig();
  }

  /**
   * Other classes use the ConfigMonitor by implementing this method
   */
  public interface ConfigMonitorListener {
    /**
     * The connect string of zk admin has changed.
     */
    void setZkConnectString(String zkConnectString);

    /**
     * The ZooKeeper session is no longer valid.
     */
    void closing();
  }

  /**
   * Processes every events from the ZooKeeper.
   */
  public void process(WatchedEvent event) {
    synchronized (this) {
      if (event.getType() != Event.EventType.None &&
          event.getPath() != null && event.getPath().equals(ZooDefs.CONFIG_NODE)) {
        syncGetConfig();
      }
    }
  }

  /**
   * A callback function to process the result of getConfig(watch=true).
   */
  public void processResult(int rc, String path, Object ctx,
                            byte[] data, Stat stat) {
    if (path != null && path.equals(ZooDefs.CONFIG_NODE)) {
      // similar to config -c
      String configData = new String(data);
      String[] config = ConfigUtils.getClientConfigStr(configData).split(" ");
      long version;

      try {
        version = Long.parseLong(config[0], 16);
      } catch (NumberFormatException e) {
        getLogger().warn("NuberFormatException trying parsing version.");
        return;
      }

      if (this.configVersion == null) {
        this.configVersion = version;
        prevConfigString = configData;
      } else if (version > this.configVersion) {
        getLogger().warn("Admin config has been changed : "
                + "From=" + prevConfigString + ", "
                + "To=" + configData + ", "
                + "[addminSessionId=0x"
                + Long.toHexString(zk.getSessionId()) + "]");

        String hostList = config[1];
        listener.setZkConnectString(hostList);
        try {
          zk.updateServerList(hostList);
        } catch (IOException e) {
          getLogger().warn("IOException trying to updateServerList. retry later");
        }
        this.configVersion = version;
        prevConfigString = configData;
      } else {
        getLogger().warn("Unexpected version. configVersion = "
                + this.configVersion + ", version = " + version);
      }
    }
  }

  private void syncGetConfig() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Set a new watch on " + ZooDefs.CONFIG_NODE);
    }

    zk.sync(ZooDefs.CONFIG_NODE, null, null);
    zk.getConfig(this, this, null);
  }

  /**
   * Shutdown the ConfigMonitor.
   */
  public void shutdown() {
    if (!dead) {
      getLogger().info("Shutting down the ConfigMonitor. " + getInfo());
      dead = true;
      listener.closing();
    }
  }

  /**
   * Set config monitor is dead.
   */
  public void setDead() {
    if (!dead) {
      getLogger().info("Set ConfigMonitor is dead. " + getInfo());
      dead = true;
    }
  }

  /**
   * Check if the config monitor is dead.
   */
  public boolean isDead() {
    return dead;
  }

  private String getInfo() {
    String zkSessionId = null;
    if (zk != null) {
      zkSessionId = "0x" + Long.toHexString(zk.getSessionId());
    }
    return "[adminSessionId=" + zkSessionId + "]";
  }
}
