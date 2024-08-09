/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2021 JaM2in Co., Ltd.
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.ArcusClientException.InitializeClientException;
import net.spy.memcached.compat.SpyThread;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * A program to use CacheMonitor to start and
 * stop memcached node based on a znode. The program watches the
 * specified znode and saves the znode that corresponds to the
 * memcached server in the remote machine. It also changes the
 * previous ketama node
 */
public class CacheManager extends SpyThread implements Watcher,
        CacheMonitor.CacheMonitorListener, MigrationMonitor.MigrationMonitorListener {
  private static final String ARCUS_BASE_CACHE_LIST_ZPATH = "/arcus/cache_list/";

  private static final String ARCUS_BASE_CLIENT_LIST_ZPATH = "/arcus/client_list/";

  /* ENABLE_MIGRATION if */
  private static final String ARCUS_BASE_CLOUD_STAT_ZPATH = "/arcus/cloud_stat/";
  /* ENABLE_MIGRATION end */

  /* ENABLE_REPLICATION if */
  private static final String ARCUS_REPL_CACHE_LIST_ZPATH = "/arcus_repl/cache_list/";

  private static final String ARCUS_REPL_CLIENT_LIST_ZPATH = "/arcus_repl/client_list/";

  /* ENABLE_MIGRATION if */
  private static final String ARCUS_REPL_CLOUD_STAT_ZPATH = "/arcus_repl/cloud_stat/";
  /* ENABLE_MIGRATION end */
  /* ENABLE_REPLICATION end */

  private static final int ZK_SESSION_TIMEOUT = 15000;

  private static final long ZK_CONNECT_TIMEOUT = ZK_SESSION_TIMEOUT;
  private static final AtomicInteger POOL_ID = new AtomicInteger(1);

  private final String zkConnectString;

  private final String serviceCode;

  private CacheMonitor cacheMonitor;

  private ZooKeeper zk;

  private CountDownLatch zkInitLatch;

  private ArcusClient[] client;

  private final ConnectionFactoryBuilder cfb;

  private final int waitTimeForConnect;

  private final int poolSize;

  private volatile boolean shutdownRequested = false;

  private List<String> prevCacheList;

  /* ENABLE_REPLICATION if */
  private boolean arcusReplEnabled = false;
  /* ENABLE_REPLICATION end */

  /* ENABLE_MIGRATION if */
  private boolean arcusMigrEnabled = false;

  private MigrationMonitor migrationMonitor;

  private MigrationType type = MigrationType.UNKNOWN;

  private MigrationState state = MigrationState.UNKNOWN;

  private boolean startup = true;

  private boolean readingCacheList = false;
  /* ENABLE_MIGRATION end */

  public CacheManager(String hostPort, String serviceCode,
                      ConnectionFactoryBuilder cfb, int poolSize, int waitTimeForConnect) {
    if (cfb.getFailureMode() == FailureMode.Redistribute) {
      throw new InitializeClientException(
          "Redistribute failure mode is not compatible with ArcusClient. " +
          "Use other failure mode.");
    }

    this.zkConnectString = hostPort;
    this.serviceCode = serviceCode;
    this.cfb = cfb;
    this.poolSize = poolSize;
    this.waitTimeForConnect = waitTimeForConnect;

    initZooKeeperClient();

    if (this.client == null) {  // initArcusClient() failure
      shutdownZooKeeperClient();
      throw new InitializeClientException("Can't initialize Arcus client.");
    }

    /* ENABLE_MIGRATION if */
    if (this.arcusMigrEnabled && isMigrationMonitorDead()) { // initMigrationMonitor() failure
      shutdownZooKeeperClient();
      throw new InitializeClientException("Can't initialize Migration Monitor.");
    }
    /* ENABLE_MIGRATION end */

    this.startup = false;

    setName("Cache Manager IO for " + serviceCode + "@" + hostPort);
    setDaemon(true);
    start();

    getLogger().info("CacheManager started. (" + serviceCode + "@" + hostPort + ")");

  }

  private String getCacheListZPath() {
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      return ARCUS_REPL_CACHE_LIST_ZPATH;
    }
    /* ENABLE_REPLICATION end */
    return ARCUS_BASE_CACHE_LIST_ZPATH;
  }

  /* ENABLE_MIGRATION if */
  private String getCloudStatZPath() {
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      return ARCUS_REPL_CLOUD_STAT_ZPATH;
    }
    /* ENABLE_REPLICATION end */
    return ARCUS_BASE_CLOUD_STAT_ZPATH;
  }
  /* ENABLE_MIGRATION end */

  private String getClientListZPath() {
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      return ARCUS_REPL_CLIENT_LIST_ZPATH;
    }
    /* ENABLE_REPLICATION end */
    return ARCUS_BASE_CLIENT_LIST_ZPATH;
  }

  private void connectZooKeeper() {
    zkInitLatch = new CountDownLatch(1);
    try {
      zk = new ZooKeeper(zkConnectString, ZK_SESSION_TIMEOUT, this);

      /* In the above ZooKeeper() internals, reverse DNS lookup occurs
       * when the getHostName() of InetSocketAddress class is called.
       * In Windows, the reverse DNS lookup includes NetBIOS lookup
       * that bring delay of 5 seconds (as well as dns and host file lookup).
       * So, ZK_CONNECT_TIMEOUT is set as much like ZK session timeout.
       */
      if (!zkInitLatch.await(ZK_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS)) {
        getLogger().fatal("Connecting to Arcus admin(%s) timed out : %d miliseconds",
            zkConnectString, ZK_CONNECT_TIMEOUT);
        throw new AdminConnectTimeoutException(zkConnectString);
      }
    } catch (AdminConnectTimeoutException e) {
      shutdownZooKeeperClient();
      throw e;
    } catch (Exception e) {
      shutdownZooKeeperClient();
      throw new InitializeClientException("Can't connect to zookeeper.", e);
    }
  }

  private void initZooKeeperClient() {
    getLogger().info("Trying to connect to Arcus admin(%s@%s)", serviceCode, zkConnectString);
    connectZooKeeper();
    try {
      /* ENABLE_REPLICATION if */
      if (zk.exists(ARCUS_REPL_CACHE_LIST_ZPATH + serviceCode, false) != null) {
        arcusReplEnabled = true;
        cfb.internalArcusReplEnabled(true);
        getLogger().info("Connected to Arcus repl cluster (serviceCode=%s)", serviceCode);
      } else {
      /* ENABLE_REPLICATION end */
        if (zk.exists(ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, false) != null) {
          /* ENABLE_REPLICATION if */
          arcusReplEnabled = false;
          cfb.internalArcusReplEnabled(false);
          /* ENABLE_REPLICATION end */
          getLogger().info("Connected to Arcus cluster (seriveCode=%s)", serviceCode);
        } else {
          getLogger().fatal("Service code not found. (%s)", serviceCode);
          throw new NotExistsServiceCodeException(serviceCode);
        }
      }

      /* ENABLE_MIGRATION if */
      if (cfb.getArcusMigrationCheck() &&
          zk.exists(getCloudStatZPath() + serviceCode, false) != null) {
        arcusMigrEnabled = true;
        cfb.internalArcusMigrEnabled(true);
        getLogger().info("Migration feature is enabled.");
      } else {
        arcusMigrEnabled = false;
        cfb.internalArcusMigrEnabled(false);
        getLogger().info("Migration feature is disabled.");
      }
      /* ENABLE_MIGRATION end */

      String path = getClientInfo();
      if (path.isEmpty()) {
        getLogger().fatal("Can't create the znode of client info (" + path + ")");
        throw new InitializeClientException("Can't create client info");
      }

      if (zk.exists(path, false) == null) {
        zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      }

      // create the cache monitor
      cacheMonitor = new CacheMonitor(zk, getCacheListZPath(), serviceCode, startup, this);

      /* ENABLE_MIGRATION if */
      if (client != null && arcusMigrEnabled) {
        initMigrationMonitor();
      }
      /* ENABLE_MIGRATION end */
    } catch (NotExistsServiceCodeException e) {
      shutdownZooKeeperClient();
      throw e;
    } catch (InitializeClientException e) {
      shutdownZooKeeperClient();
      throw e;
    } catch (Exception e) {
      shutdownZooKeeperClient();
      throw new InitializeClientException("Can't initialize Arcus client.", e);
    }
  }

  private void initMigrationMonitor() throws InterruptedException {
    migrationMonitor = new MigrationMonitor(zk, getCloudStatZPath(), serviceCode, startup, this);
  }

  private String getClientInfo() {
    String path = getClientListZPath() + serviceCode + "/";

    // create an ephemeral znode with the following client info.
    // {client hostname}_{ip address}_{pool size}_java_{client version}_
    // {YYYYMMDDHHIISS}_{zk session id}
    // _jre={jre version}

    // get host info
    String hostInfo;
    try {
      hostInfo = InetAddress.getLocalHost().getHostName() + "_"
               + InetAddress.getLocalHost().getHostAddress() + "_";
    } catch (Exception e) {
      getLogger().fatal("Can't get client host info.", e);
      hostInfo = "unknown-host_0.0.0.0_";
    }
    hostInfo = hostInfo
             + this.poolSize + "_java_" + ArcusClient.getVersion() + "_";

    // get create time and zk session id
    String restInfo;
    try {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
      Date currentTime = new Date();
      restInfo = simpleDateFormat.format(currentTime) + "_" + zk.getSessionId();
    } catch (Exception e) {
      getLogger().fatal("Can't get time and zk session id.", e);
      restInfo = "00000000000000_0";
    }

    // get jre info (additional)
    String jreInfo;
    try {
      jreInfo = "_jre=" + System.getProperty("java.version");
    } catch (Exception e) {
      getLogger().fatal("Can't get java version.", e);
      jreInfo = "_jre=null";
    }

    return path + hostInfo + restInfo + jreInfo;
  }

  /***************************************************************************
   * We do process only child node change event ourselves, we just need to
   * forward them on.
   *
   */
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      switch (event.getState()) {
        case SyncConnected:
          zkInitLatch.countDown();
          getLogger().info("Connected to Arcus admin. (%s@%s)", serviceCode, zkConnectString);
          if (cacheMonitor != null) {
            getLogger().warn("Reconnected to the Arcus admin. " + getInfo());
          } else {
            getLogger().debug("cm is null, servicecode : %s, state:%s, type:%s",
                    serviceCode, event.getState(), event.getType());
          }
          break;
        case Disconnected:
          getLogger().warn("Disconnected from the Arcus admin. Trying to reconnect. " + getInfo());
          break;
        case Expired:
          // If the session was expired, just shutdown this client to be re-initiated.
          getLogger().warn("Session expired. Trying to reconnect to the Arcus admin." + getInfo());
          if (cacheMonitor != null) {
            cacheMonitor.shutdown();
          }
          break;
      }
    }
  }

  public void run() {
    synchronized (this) {
      while (!shutdownRequested) {
        long retrySleepTime = 0;
        boolean isDead = false;

        if (cacheMonitor.isDead()) {
          isDead = true;
          try {
            getLogger().warn("Unexpected disconnection from Arcus admin. " +
                "Trying to reconnect to Arcus admin. CacheList =" + prevCacheList);
            shutdownZooKeeperClient();
            initZooKeeperClient();
          } catch (AdminConnectTimeoutException e) {
            retrySleepTime = 1000L; // 1 second
          } catch (NotExistsServiceCodeException e) {
            retrySleepTime = 5000L; // 5 second
          } catch (InitializeClientException e) {
            retrySleepTime = 5000L; // 5 second
          } catch (Exception e) {
            retrySleepTime = 1000L; // 1 second
            getLogger().warn("Unexpected exception is caught while reconnect to Arcus admin: %s",
                e.getMessage());
          }
          /* ENABLE_MIGRATION if */
        } else if (arcusMigrEnabled && isMigrationMonitorDead()) {
          isDead = true;
          try {
            getLogger().warn("Unexpected shutdown of MigrationMonitor. " +
                    "Trying to recreate MigrationMonitor.");
            initMigrationMonitor();
          } catch (InterruptedException e) {
            retrySleepTime = 5000L; // 5 second
          } catch (Exception e) {
            retrySleepTime = 1000L; // 1 second
            getLogger().warn("Unexpected exception is caught while recreate MigrationMonitor: %s",
                e.getMessage());
          }
          /* ENABLE_MIGRATION end */
        }

        long start = System.currentTimeMillis();
        waitBeforeRetryMonitorCheck(retrySleepTime);
        long end = System.currentTimeMillis();

        if (isDead && retrySleepTime == 0) {
          long elapsed = end - start;
          if (elapsed < 1000L) {
            waitBeforeRetryMonitorCheck(1000L - elapsed);
          }
        }
      }
    }
    getLogger().info("Close cache manager.");
    shutdownZooKeeperClient();
  }

  private void waitBeforeRetryMonitorCheck(long retrySleepTime) {
    try {
      wait(retrySleepTime);
    } catch (InterruptedException e) {
      getLogger().warn("Cache manager thread is interrupted while wait: %s",
          e.getMessage());
    }
  }

  public void closing() {
    synchronized (this) {
      notifyAll();
    }
  }

  public void commandNodeChange(List<String> children) {
    /* TODO */
  }

  private String getAddressListString(List<String> children) {
    StringBuilder addrs = new StringBuilder();
    for (int i = 0; i < children.size(); i++) {
      String[] temp = children.get(i).split("-");
      if (i != 0) {
        addrs.append(",").append(temp[0]);
      } else {
        addrs.append(temp[0]);
      }
    }
    return addrs.toString();
  }

  /**
   * Change current MemcachedNodes to new MemcachedNodes but intersection of
   * current and new will be ruled out.
   *
   * @param children new children node list
   */
  public void commandCacheListChange(List<String> children) {
    if (children.isEmpty()) {
      getLogger().error("Cannot find any cache nodes for your service code. " +
              "Please contact Arcus support to solve this problem. " +
              "[serviceCode=" + serviceCode + ", adminSessionId=0x" +
              Long.toHexString(zk.getSessionId()));
    }

    if (!children.equals(prevCacheList)) {
      getLogger().warn("Cache list has been changed : "
          + "From=" + prevCacheList + ", "
          + "To=" + children + ", "
          + "[serviceCode=" + serviceCode + ", adminSessionId=0x"
          + Long.toHexString(zk.getSessionId()));
    }

    // Store the current children.
    prevCacheList = children;

    /* ENABLE_REPLICATION if */
    // children is the current list of znodes in the cache_list directory
    // Arcus base cluster and repl cluster use different znode names.
    //
    // Arcus base cluster
    // Znode names are ip:port-hostname.  Just remove -hostname and concat
    // all names separated by commas.  AddrUtil turns ip:port into InetSocketAddress.
    //
    // Arcus repl cluster
    // Znode names are group^{M,S}^ip:port-hostname.  Concat all names separated
    // by commas.  ArcusRepNodeAddress turns these names into ArcusReplNodeAddress.
    /* ENABLE_REPLICATION end */
    String addrs = getAddressListString(children);

    if (client == null) {
      if (!addrs.isEmpty()) {
        initArcusClient(addrs);
      }
      return;
    }

    for (ArcusClient ac : client) {
      MemcachedConnection conn = ac.getMemcachedConnection();
      conn.setCacheNodesChange(addrs);
    }
  }

  /* ENABLE_MIGRATION if */
  @Override
  public void setReadingCacheList(boolean reading) {
    readingCacheList = reading;
  }

  @Override
  public boolean commandCloudStatChange(List<String> children) {
    // return true if STATE == PREPARED.
    if (children.isEmpty()) {
      /*
       * case 1 : No child znode in clout_stat znode.
       * case 2 : Starting migration.
       */

      clearMigrationTypeAndState();
      return false;
    }

    AbstractMap.SimpleEntry<MigrationType, MigrationState> typeAndState =
        validateCloudStatZNodes(children);
    if (typeAndState == null) {
      return false;
    }

    MigrationType newType = typeAndState.getKey();
    MigrationState newState = typeAndState.getValue();
    getLogger().info("MigrationMonitor reads cloud_stat. type=" + newType + ", state=" + newState);

    if (type != MigrationType.UNKNOWN && type != newType) {
      getLogger().error("The cloud_stat type mismatch. curType=" + type + ", newType=" + newType);
      clearMigrationTypeAndState();
      return false;
    }

    type = newType;

    if (state == newState) {
      // TODO: call clearMigrationTypeAndState or not.
      return false;
    }

    state = newState;

    for (ArcusClient ac : client) {
      MemcachedConnection conn = ac.getMemcachedConnection();
      conn.setMigrationTypeAndState(type, state);
    }

    /* All alter_list has been registered.
     * Read the alter_list and pass it to the IO Thread to prepare for the migration.
     */
    return state == MigrationState.PREPARED;
  }

  @Override
  public boolean commandAlterListChange(List<String> children) {
    // return false if this method is failed.
    String addrs = getAddressListString(children);
    if (startup) {
      try {
        for (ArcusClient ac : client) {
          MemcachedConnection conn = ac.getMemcachedConnection();
          conn.prepareAlterConnections(arcusReplEnabled ?
              ArcusReplNodeAddress.getAddresses(addrs) : AddrUtil.getAddresses(addrs));
        }
      } catch (IOException e) {
        getLogger().fatal("Cannot initialize alter_list znode.", e);
        return false; // startup && caught exception => return false
      }
    } else {
      for (ArcusClient ac : client) {
        MemcachedConnection conn = ac.getMemcachedConnection();
        conn.setAlterNodesChange(addrs, readingCacheList);
      }
    }
    return true;
  }

  private void clearMigrationTypeAndState() {
    if (type != MigrationType.UNKNOWN || state != MigrationState.UNKNOWN) {
      type = MigrationType.UNKNOWN;
      state = MigrationState.UNKNOWN;

      for (ArcusClient ac : client) {
        MemcachedConnection conn = ac.getMemcachedConnection();
        conn.setMigrationTypeAndState(type, state);
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
    if (children.size() < child_count) {
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
       * STATE^<JOIN|LEAVE>^<BEGIN|PREPARED|DONE>
       */
      if (znode.startsWith("STATE^")) {
        String[] tokens = znode.split("\\^");
        if (tokens.length < 3) {
          break;
        }

        type = MigrationType.fromString(tokens[1]);
        if (type == MigrationType.UNKNOWN) {
          break;
        }

        state = MigrationState.fromString(tokens[2]);
        if (state == MigrationState.UNKNOWN) {
          break;
        }

        valid_count++;
      }
    }
    if (valid_count != child_count) {
      getLogger().warn("Invalid cloud_stat znodes.");
      return null;
    }
    return new AbstractMap.SimpleEntry<>(type, state);
  }

  private boolean isMigrationMonitorDead() {
    return migrationMonitor == null || migrationMonitor.isDead();
  }
  /* ENABLE_MIGRATION end */

  public List<String> getPrevCacheList() {
    return this.prevCacheList;
  }

  private String getInfo() {
    String zkSessionId = null;
    if (zk != null) {
      zkSessionId = "0x" + Long.toHexString(zk.getSessionId());
    }
    return "[serviceCode=" + serviceCode + ", adminSessionId=" + zkSessionId + "]";
  }

  /**
   * initialized ArcusClient Pool.
   *
   * @param addrs current available Memcached Addresses
   */
  private void initArcusClient(String addrs) {
    List<InetSocketAddress> socketList = getSocketAddressList(addrs);
    int addrCount = socketList.size();

    final CountDownLatch latch = new CountDownLatch(addrCount * poolSize);
    final ConnectionObserver observer = new ConnectionObserver() {
      @Override
      public void connectionLost(SocketAddress sa) { }
      @Override
      public void connectionEstablished(SocketAddress sa, int reconnectCount) {
        latch.countDown();
      }
    };
    cfb.setInitialObservers(Collections.singleton(observer));

    int poolId = CacheManager.POOL_ID.getAndIncrement();
    client = new ArcusClient[poolSize];
    int i = 0;
    try {
      for (; i < poolSize; i++) {
        String clientName = "ArcusClient" + "-" + poolId +
                "(" + (i + 1) + "-" + poolSize + ") for " + serviceCode;
        client[i] = ArcusClient.getInstance(cfb.build(), clientName, socketList);
        client[i].setName("Memcached IO for " + serviceCode);
        client[i].setCacheManager(this);
      }
    } catch (IOException e) {
      getLogger().fatal("Arcus Connection has critical problems. contact arcus manager.", e);
      /* shutdown created ArcusClient */
      while (--i >= 0) {
        client[i].shutdown();
      }
      client = null;
      return;
    }

    try {
      int awaitTime = waitTimeForConnect == 0 ? 50 * addrCount * poolSize : waitTimeForConnect;
      if (latch.await(awaitTime, TimeUnit.MILLISECONDS)) {
        getLogger().warn("All arcus connections are established.");
      } else {
        getLogger().error("Some arcus connections are not established.");
      }
      // Success signal for initial connections to Zookeeper and Memcached.
    } catch (InterruptedException e) {
      getLogger().fatal("Arcus Connection has critical problems. contact arcus manager.", e);
    }

  }

  private List<InetSocketAddress> getSocketAddressList(String addrs) {
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      List<InetSocketAddress> socketList = ArcusReplNodeAddress.getAddresses(addrs);

      Map<String, List<ArcusReplNodeAddress>> newAllGroups =
              ArcusReplNodeAddress.makeGroupAddrsList(socketList);

      // recreate socket list
      socketList.clear();
      for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllGroups.entrySet()) {
        if (ArcusReplNodeAddress.validateGroup(entry)) {
          socketList.addAll(entry.getValue());
        }
      }
      return socketList;
    }
    /* ENABLE_REPLICATION end */
    return AddrUtil.getAddresses(addrs);
  }

  /**
   * Returns current ArcusClient
   *
   * @return current ArcusClient
   */
  public ArcusClient[] getAC() {
    return client;
  }

  private void shutdownZooKeeperClient() {
    if (zk == null) {
      return;
    }

    try {
      getLogger().info("Close the ZooKeeper client. serviceCode=" + serviceCode +
              ", adminSessionId=0x" + Long.toHexString(zk.getSessionId()));
      zk.close();
      zk = null;
    } catch (InterruptedException e) {
      getLogger().warn("An exception occured while closing ZooKeeper client.", e);
    }
  }

  public void shutdown() {
    if (!shutdownRequested) {
      getLogger().info("Shut down cache manager.");
      shutdownRequested = true;
      closing();
    }
  }
}
