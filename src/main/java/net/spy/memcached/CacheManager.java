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

/**
 * A program to use CacheMonitor to start and
 * stop memcached node based on a znode. The program watches the
 * specified znode and saves the znode that corresponds to the
 * memcached server in the remote machine. It also changes the
 * previous ketama node
 */

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ArcusClientException.InitializeClientException;
import net.spy.memcached.compat.SpyThread;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class CacheManager extends SpyThread implements Watcher,
        CacheMonitor.CacheMonitorListener {
  private static final String ARCUS_BASE_CACHE_LIST_ZPATH = "/arcus/cache_list/";

  private static final String ARCUS_BASE_CLIENT_INFO_ZPATH = "/arcus/client_list/";

  /* ENABLE_REPLICATION if */
  private static final String ARCUS_REPL_CACHE_LIST_ZPATH = "/arcus_repl/cache_list/";

  private static final String ARCUS_REPL_CLIENT_INFO_ZPATH = "/arcus_repl/client_list/";

  /* ENABLE_REPLICATION end */
  private static final int ZK_SESSION_TIMEOUT = 15000;

  private static final long ZK_CONNECT_TIMEOUT = ZK_SESSION_TIMEOUT;

  private final String hostPort;

  private final String serviceCode;

  private CacheMonitor cacheMonitor;

  private ZooKeeper zk;

  private ArcusClient[] client;

  private final CountDownLatch clientInitLatch;

  private final ConnectionFactoryBuilder cfb;

  private final int waitTimeForConnect;

  private final int poolSize;

  private volatile boolean shutdownRequested = false;

  private CountDownLatch zkInitLatch;

  private List<String> prevCacheList;

  /**
   * The locator class of the spymemcached has an assumption
   * that it should have one cache node at least.
   * Thus, we add a fake server node in it
   * if there's no cache servers for the given service code.
   * This is just a work-around, but it works really.
   */
  public static final String FAKE_SERVER_NODE = "0.0.0.0:23456";

  /* ENABLE_REPLICATION if */
  private boolean arcusReplEnabled = false;

  /* ENABLE_REPLICATION end */
  public CacheManager(String hostPort, String serviceCode,
                      ConnectionFactoryBuilder cfb, CountDownLatch clientInitLatch, int poolSize,
                      int waitTimeForConnect) {

    this.hostPort = hostPort;
    this.serviceCode = serviceCode;
    this.cfb = cfb;
    this.clientInitLatch = clientInitLatch;
    this.poolSize = poolSize;
    this.waitTimeForConnect = waitTimeForConnect;

    initZooKeeperClient();

    setName("Cache Manager IO for " + serviceCode + "@" + hostPort);
    setDaemon(true);
    start();

    getLogger().info("CacheManager started. (" + serviceCode + "@" + hostPort + ")");

  }

  private void initZooKeeperClient() {
    try {
      getLogger().info("Trying to connect to Arcus admin(%s@%s)", serviceCode, hostPort);

      zkInitLatch = new CountDownLatch(1);
      zk = new ZooKeeper(hostPort, ZK_SESSION_TIMEOUT, this);

      try {
        /* In the above ZooKeeper() internals, reverse DNS lookup occurs
         * when the getHostName() of InetSocketAddress class is called.
         * In Windows, the reverse DNS lookup includes NetBIOS lookup
         * that bring delay of 5 seconds (as well as dns and host file lookup).
         * So, ZK_CONNECT_TIMEOUT is set as much like ZK session timeout.
         */
        if (zkInitLatch.await(ZK_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS) == false) {
          getLogger().fatal("Connecting to Arcus admin(%s) timed out : %d miliseconds",
                  hostPort, ZK_CONNECT_TIMEOUT);
          throw new AdminConnectTimeoutException(hostPort);
        }

        /* ENABLE_REPLICATION if */
        if (zk.exists(ARCUS_REPL_CACHE_LIST_ZPATH + serviceCode, false) != null) {
          arcusReplEnabled = true;
          cfb.internalArcusReplEnabled(true);
          getLogger().info("Connecting to Arcus repl cluster");
        } else if (zk.exists(ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, false) != null) {
          arcusReplEnabled = false;
          cfb.internalArcusReplEnabled(false);
          getLogger().info("Connecting to Arcus cluster");
        } else {
          getLogger().fatal("ARCUS cluster named \"%s\" not found.", serviceCode);
          throw new NotExistsServiceCodeException(serviceCode);
        }
        /* ENABLE_REPLICATION else */
        /*
        if (zk.exists(ARCUS_BASE_CACHE_LIST_ZPATH + serviceCode, false) == null) {
          getLogger().fatal("Service code not found. (" + serviceCode + ")");
          throw new NotExistsServiceCodeException(serviceCode);
        }
        */

        /* ENABLE_REPLICATION end */

        String path = getClientInfo();
        if (path.isEmpty()) {
          getLogger().fatal("Can't create the znode of client info (" + path + ")");
          throw new InitializeClientException("Can't create client info");
        } else {
          if (zk.exists(path, false) == null) {
            zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
          }
        }
      } catch (AdminConnectTimeoutException e) {
        shutdownZooKeeperClient();
        throw e;
      } catch (NotExistsServiceCodeException e) {
        shutdownZooKeeperClient();
        throw e;
      } catch (InitializeClientException e) {
        shutdownZooKeeperClient();
        throw e;
      } catch (InterruptedException ie) {
        getLogger().fatal("Can't connect to Arcus admin(%s@%s) %s",
                serviceCode, hostPort, ie.getMessage());
        shutdownZooKeeperClient();
        return;
      } catch (Exception e) {
        getLogger().fatal("Unexpected exception. contact to Arcus administrator", e);

        shutdownZooKeeperClient();
        throw new InitializeClientException("Can't initialize Arcus client.", e);
      }

      /* ENABLE_REPLICATION if */
      String cacheListZPath = arcusReplEnabled ? ARCUS_REPL_CACHE_LIST_ZPATH
              : ARCUS_BASE_CACHE_LIST_ZPATH;
      cacheMonitor = new CacheMonitor(zk, cacheListZPath, serviceCode, this);
      /* ENABLE_REPLICATION else */
      /*
      cacheMonitor = new CacheMonitor(zk, ARCUS_BASE_CACHE_LIST_ZPATH, serviceCode, this);
      */
      /* ENABLE_REPLICATION end */
    } catch (IOException e) {
      throw new InitializeClientException("Can't initialize Arcus client.", e);
    }
  }

  private String getClientInfo() {
    String path = "";

    // create the ephemeral znode
    // "/arcus/client_list/{service_code}/{client hostname}_{ip address}_{pool size}_java_{client version}_{YYYYMMDDHHIISS}_{zk session id}"
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      path = ARCUS_REPL_CLIENT_INFO_ZPATH + serviceCode + "/";
    } else {
      path = ARCUS_BASE_CLIENT_INFO_ZPATH + serviceCode + "/";
    }
    /* ENABLE_REPLICATION else */
    /*
    path = ARCUS_BASE_CLIENT_INFO_ZPATH + serviceCode + "/";
    */
    /* ENABLE_REPLICATION end */

    /* get host info */
    String hostInfo;
    try {
      hostInfo = InetAddress.getLocalHost().getHostName() + "_"
               + InetAddress.getLocalHost().getHostAddress() + "_";
    } catch (Exception e) {
      getLogger().fatal("Can't get client host info.", e);
      hostInfo = "unknown-host_0.0.0.0_";
    }
    path = path + hostInfo
         + this.poolSize + "_java_" + ArcusClient.VERSION + "_";

    /* get time and zk session id */
    String restInfo;
    try {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
      Date currentTime = new Date();

      restInfo = simpleDateFormat.format(currentTime) + "_"
               + zk.getSessionId();
    } catch (Exception e) {
      getLogger().fatal("Can't get time and zk session id.", e);
      restInfo = "00000000000000_0";
    }
    path = path + restInfo;

    return path;
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
          getLogger().info("Connected to Arcus admin. (%s@%s)", serviceCode, hostPort);
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
          if (cacheMonitor != null)
            cacheMonitor.shutdown();
          break;
      }
    }
  }

  public void run() {
    synchronized (this) {
      while (!shutdownRequested) {
        if (!cacheMonitor.dead) {
          try {
            wait();
          } catch (InterruptedException e) {
              getLogger().warn("Cache mananger thread is interrupted while wait: %s",
                               e.getMessage());
          }
        } else {
          long retrySleepTime = 0;
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
            getLogger().warn("upexpected exception is caught while reconnet to Arcus admin: %s",
                             e.getMessage());
          }
          if (retrySleepTime > 0) { // retry is needed
            try {
              Thread.sleep(retrySleepTime);
            } catch (InterruptedException e) {
              getLogger().warn("Cache mananger thread is interrupted while sleep: %s",
                               e.getMessage());
            }
          }
        }
      }
    }
    getLogger().info("Close cache manager.");
    shutdownZooKeeperClient();
  }

  public void closing() {
    synchronized (this) {
      notifyAll();
    }
  }

  private String getAddressListString(List<String> children) {
    String addrs = "";
    for (int i = 0; i < children.size(); i++) {
      String[] temp = children.get(i).split("-");
      if (i != 0) {
        addrs = addrs + "," + temp[0];
      } else {
        addrs = temp[0];
      }
    }
    return addrs;
  }

  /**
   * If there's no children in the znode, make a fake server node.
   *
   * Change current MemcachedNodes to new MemcachedNodes but intersection of
   * current and new will be ruled out.
   *
   * @param children
   *            new children node list
   */
  public void commandCacheListChange(List<String> children) {
    // If there's no children, add a fake server node to the list.
    if (children.size() == 0) {
      getLogger().error("Cannot find any cache nodes for your service code. " +
              "Please contact Arcus support to solve this problem. " +
              "[serviceCode=" + serviceCode + ", addminSessionId=0x" +
              Long.toHexString(zk.getSessionId()));
      children.add(CacheManager.FAKE_SERVER_NODE);
    }

    if (!children.equals(prevCacheList)) {
      getLogger().warn("Cache list has been changed : From=" + prevCacheList + ", To=" + children + ", " +
              "[serviceCode=" + serviceCode + ", addminSessionId=0x" +
              Long.toHexString(zk.getSessionId()));
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
      createArcusClient(addrs);
      return;
    }

    for (ArcusClient ac : client) {
      MemcachedConnection conn = ac.getMemcachedConnection();
      conn.putMemcachedQueue(addrs);
      conn.getSelector().wakeup();
    }
  }

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
   * Create a ArcusClient
   *
   * @param addrs
   *            current available Memcached Addresses
   */
  private void createArcusClient(String addrs) {
    /* ENABLE_REPLICATION if */
    List<InetSocketAddress> socketList;
    int addrCount;
    if (arcusReplEnabled) {
      socketList = ArcusReplNodeAddress.getAddresses(addrs);

      Map<String, List<ArcusReplNodeAddress>> newAllGroups =
              ArcusReplNodeAddress.makeGroupAddrsList(socketList);

      /* recreate socket list */
      socketList.clear();
      for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllGroups.entrySet()) {
        if (entry.getValue().size() == 0)
          socketList.add(ArcusReplNodeAddress.createFake(entry.getKey()));
        else
          socketList.addAll(entry.getValue());
      }

      // Exclude fake server addresses in the initial latch count.
      // Otherwise we may block here for a while trying to connect to
      // slave-only groups.
      addrCount = 0;
      for (InetSocketAddress a : socketList) {
        // See TCPMemcachedNodeImpl:TCPMemcachedNodeImpl().
        if (("/" + CacheManager.FAKE_SERVER_NODE).equals(
                a.getAddress() + ":" + a.getPort()) != true)
          addrCount++;
      }
    } else {
      socketList = AddrUtil.getAddresses(addrs);
      // Preserve base cluster behavior.  The initial latch count
      // includes fake server addresses.
      addrCount = socketList.size();
    }
    /* ENABLE_REPLICATION else */
    /*

    List<InetSocketAddress> socketList = AddrUtil.getAddresses(addrs);
    int addrCount = socketList.size();
    */
    /* ENABLE_REPLICATION end */

    final CountDownLatch latch = new CountDownLatch(addrCount * poolSize);
    final ConnectionObserver observer = new ConnectionObserver() {

      @Override
      public void connectionLost(SocketAddress sa) {

      }

      @Override
      public void connectionEstablished(SocketAddress sa, int reconnectCount) {
        latch.countDown();
      }
    };

    cfb.setInitialObservers(Collections.singleton(observer));

    int _awaitTime = 0;
    if (waitTimeForConnect == 0)
      _awaitTime = 50 * addrCount * poolSize;
    else
      _awaitTime = waitTimeForConnect;

    client = new ArcusClient[poolSize];
    for (int i = 0; i < poolSize; i++) {
      try {
        client[i] = ArcusClient.getInstance(cfb.build(), socketList);
        client[i].setName("Memcached IO for " + serviceCode);
        client[i].setCacheManager(this);
      } catch (IOException e) {
        getLogger().fatal("Arcus Connection has critical problems. contact arcus manager.");
      }
    }
    try {
      if (latch.await(_awaitTime, TimeUnit.MILLISECONDS)) {
        getLogger().warn("All arcus connections are established.");
      } else {
        getLogger().error("Some arcus connections are not established.");
      }
      // Success signal for initial connections to Zookeeper and
      // Memcached.
    } catch (InterruptedException e) {
      getLogger().fatal("Arcus Connection has critical problems. contact arcus manager.");
    }
    this.clientInitLatch.countDown();

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
