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
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import net.spy.memcached.collection.CollectionBulkStore;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeGetBulkWithByteTypeBkey;
import net.spy.memcached.collection.BTreeGetBulkWithLongTypeBkey;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.internal.ReconnDelay;
import net.spy.memcached.internal.ZnodeMap;
import net.spy.memcached.internal.ZnodeType;
import net.spy.memcached.internal.MigrationMode;
import net.spy.memcached.internal.MigrationMap;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.CollectionBulkStoreOperation;
import net.spy.memcached.protocol.ascii.CollectionBulkStoreOperationImpl;

/**
 * Connection to a cluster of memcached servers.
 */
public final class MemcachedConnection extends SpyObject {

  // The number of empty selects we'll allow before assuming we may have
  // missed one and should check the current selectors.  This generally
  // indicates a bug, but we'll check it nonetheless.
  private static final int DOUBLE_CHECK_EMPTY = 256;
  // The number of empty selects we'll allow before blowing up.  It's too
  // easy to write a bug that causes it to loop uncontrollably.  This helps
  // find those bugs and often works around them.
  private static final int EXCESSIVE_EMPTY = 0x1000000;

  private volatile boolean shutDown = false;
  // If true, optimization will collapse multiple sequential get ops
  private final boolean shouldOptimize;
  private Selector selector = null;
  private final NodeLocator locator;
  private final FailureMode failureMode;
  // maximum amount of time to wait between reconnect attempts
  private final long maxDelay;
  private int emptySelects = 0;
  // AddedQueue is used to track the QueueAttachments for which operations
  // have recently been queued.
  private final ConcurrentLinkedQueue<MemcachedNode> addedQueue;
  // reconnectQueue contains the attachments that need to be reconnected
  // The key is the time at which they are eligible for reconnect
  private final SortedMap<Long, MemcachedNode> reconnectQueue;
  private final Collection<ConnectionObserver> connObservers =
          new ConcurrentLinkedQueue<ConnectionObserver>();
  private final OperationFactory opFact;
  private final int timeoutExceptionThreshold;
  private final int timeoutRatioThreshold;

  /* ENABLE_MIGRATION if */
  private BlockingQueue<ZnodeMap> _znodeManageQueue = new LinkedBlockingQueue<ZnodeMap>();
  private MigrationMode _migrationMode = MigrationMode.Init;
  /* else */
  /*
  private BlockingQueue<String> _nodeManageQueue = new LinkedBlockingQueue<String>();
  */
  /* ENABLE_MIGRATION end */
  private final ConnectionFactory f;
  private Map<SocketAddress, String> versions = new ConcurrentHashMap<SocketAddress, String>();

  /* ENABLE_REPLICATION if */
  private boolean arcusReplEnabled;

  /* ENABLE_REPLICATION end */

  /**
   * Construct a memcached connection.
   *
   * @param bufSize the size of the buffer used for reading from the server
   * @param f       the factory that will provide an operation queue
   * @param a       the addresses of the servers to connect to
   * @throws IOException if a connection attempt fails early
   */
  public MemcachedConnection(int bufSize, ConnectionFactory f,
                             List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
                             FailureMode fm, OperationFactory opfactory)
          throws IOException {
    this.f = f;
    connObservers.addAll(obs);
    reconnectQueue = new TreeMap<Long, MemcachedNode>();
    addedQueue = new ConcurrentLinkedQueue<MemcachedNode>();
    failureMode = fm;
    shouldOptimize = f.shouldOptimize();
    maxDelay = f.getMaxReconnectDelay();
    opFact = opfactory;
    timeoutExceptionThreshold = f.getTimeoutExceptionThreshold();
    timeoutRatioThreshold = f.getTimeoutRatioThreshold();
    selector = Selector.open();
    List<MemcachedNode> connections = new ArrayList<MemcachedNode>(a.size());
    for (SocketAddress sa : a) {
      connections.add(attachMemcachedNode(sa));
    }
    locator = f.createLocator(connections);
  }

  /* ENABLE_REPLICATION if */
  // handleNodeManageQueue and updateConnections behave slightly differently
  // depending on the Arcus version.  We could have created a subclass and overload
  // those methods.  But, MemcachedConnection is a final class.
  void setArcusReplEnabled(boolean b) {
    arcusReplEnabled = b;
  }

  boolean getArcusReplEnabled() {
    return arcusReplEnabled;
  }

  /* ENABLE_REPLICATION end */
  private boolean selectorsMakeSense() {
    for (MemcachedNode qa : locator.getAll()) {
      if (qa.getSk() != null && qa.getSk().isValid()) {
        if (qa.getChannel().isConnected()) {
          int sops = qa.getSk().interestOps();
          int expected = 0;
          if (qa.hasReadOp()) {
            expected |= SelectionKey.OP_READ;
          }
          if (qa.hasWriteOp()) {
            expected |= SelectionKey.OP_WRITE;
          }
          if (qa.getBytesRemainingToWrite() > 0) {
            expected |= SelectionKey.OP_WRITE;
          }
          assert sops == expected : "Invalid ops:  "
                  + qa + ", expected " + expected + ", got " + sops;
        } else {
          int sops = qa.getSk().interestOps();
          assert sops == SelectionKey.OP_CONNECT
                  : "Not connected, and not watching for connect: "
                  + sops;
        }
      }
    }
    getLogger().debug("Checked the selectors.");
    return true;
  }

  /**
   * MemcachedClient calls this method to handle IO over the connections.
   */
  public void handleIO() throws IOException {
    if (shutDown) {
      throw new IOException("No IO while shut down");
    }

    // Deal with all of the stuff that's been added, but may not be marked
    // writable.
    handleInputQueue();
    getLogger().debug("Done dealing with queue.");

    long delay = 0;
    if (!reconnectQueue.isEmpty()) {
      long now = System.currentTimeMillis();
      long then = reconnectQueue.firstKey();
      delay = Math.max(then - now, 1);
    }
    getLogger().debug("Selecting with delay of %sms", delay);
    assert selectorsMakeSense() : "Selectors don't make sense.";
    int selected = selector.select(delay);
    Set<SelectionKey> selectedKeys = selector.selectedKeys();

    if (selectedKeys.isEmpty() && !shutDown) {
      getLogger().debug("No selectors ready, interrupted: "
              + Thread.interrupted());
      if (++emptySelects > DOUBLE_CHECK_EMPTY) {
        for (SelectionKey sk : selector.keys()) {
          getLogger().info("%s has %s, interested in %s",
                  sk, sk.readyOps(), sk.interestOps());
          if (sk.readyOps() != 0) {
            getLogger().info("%s has a ready op, handling IO", sk);
            handleIO(sk);
          } else {
            lostConnection((MemcachedNode) sk.attachment(), ReconnDelay.DEFAULT, "too many empty selects");
          }
        }
        assert emptySelects < EXCESSIVE_EMPTY
                : "Too many empty selects";
      }
    } else {
      getLogger().debug("Selected %d, selected %d keys",
              selected, selectedKeys.size());
      emptySelects = 0;

      for (SelectionKey sk : selectedKeys) {
        handleIO(sk);
      }

      selectedKeys.clear();
    }

    // see if any connections blew up with large number of timeouts
    for (SelectionKey sk : selector.keys()) {
      MemcachedNode mn = (MemcachedNode) sk.attachment();
      if (mn.getContinuousTimeout() > timeoutExceptionThreshold) {
        getLogger().warn(
                "%s exceeded continuous timeout threshold. >%s (%s)",
                mn.getSocketAddress().toString(), timeoutExceptionThreshold, mn.getStatus());
        lostConnection(mn, ReconnDelay.DEFAULT, "continuous timeout");
      } else if (timeoutRatioThreshold > 0 && mn.getTimeoutRatioNow() > timeoutRatioThreshold) {
        getLogger().warn(
                "%s exceeded timeout ratio threshold. >%s (%s)",
                mn.getSocketAddress().toString(), timeoutRatioThreshold, mn.getStatus());
        lostConnection(mn, ReconnDelay.DEFAULT, "high timeout ratio");
      }
    }

    /* ENABLE_MIGRATION if */
    handleZnodeManageQueue();
    /* else */
    /*
    // Deal with the memcached server group that's been added by CacheManager.
    handleNodeManageQueue();
    */
    /* ENABLE_MIGRATION end */

    if (!shutDown && !reconnectQueue.isEmpty()) {
      attemptReconnects();
    }
  }

  public void updateConnections(List<InetSocketAddress> addrs) throws IOException {
    List<MemcachedNode> attachNodes = new ArrayList<MemcachedNode>();
    List<MemcachedNode> removeNodes = new ArrayList<MemcachedNode>();

    // Classify the incoming node list.
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      List<MemcachedReplicaGroup> changeRoleGroups = new ArrayList<MemcachedReplicaGroup>();
      // will do task after locator update
      List<Task> taskList = new ArrayList<Task>();
      Map<String, List<ArcusReplNodeAddress>> newAllGroups =
              ArcusReplNodeAddress.makeGroupAddrsList(addrs);

      Map<String, MemcachedReplicaGroup> oldAllGroups =
              ((ArcusReplKetamaNodeLocator) locator).getAllGroups();

      for (Map.Entry<String, MemcachedReplicaGroup> entry : oldAllGroups.entrySet()) {
        MemcachedReplicaGroup oldGroup = entry.getValue();

        List<ArcusReplNodeAddress> newGroupAddrs = newAllGroups.get(entry.getKey());

        ArcusClient.arcusLogger.debug("New group nodes : " + newGroupAddrs);
        ArcusClient.arcusLogger.debug("Old group nodes : [" + entry.getValue() + "]");

        if (newGroupAddrs == null) {
          /* Old group nodes have disappered. Remove the old group nodes. */
          removeNodes.add(oldGroup.getMasterNode());
          if (oldGroup.getSlaveNode() != null)
            removeNodes.add(oldGroup.getSlaveNode());
          continue;
        }
        if (newGroupAddrs.size() == 0) {
          /* New group is invalid, do nothing. */
          newAllGroups.remove(entry.getKey());
          continue;
        }

        ArcusReplNodeAddress oldMasterAddr = oldGroup.getMasterNode() != null ?
                (ArcusReplNodeAddress) oldGroup.getMasterNode().getSocketAddress() : null;
        ArcusReplNodeAddress oldSlaveAddr = oldGroup.getSlaveNode() != null ?
                (ArcusReplNodeAddress) oldGroup.getSlaveNode().getSocketAddress() : null;
        assert (oldMasterAddr != null);

        if (newGroupAddrs.size() == 1) { /* New group has only a master node. */
          if (oldSlaveAddr == null) { /* Old group has only a master node. */
            if (newGroupAddrs.get(0).getIPPort().equals(oldMasterAddr.getIPPort())) {
              /* The same master node. Do nothing. */
            } else {
              MemcachedNode newMasterNode;
              /* The master of the group has changed to the new one. */
              removeNodes.add(oldGroup.getMasterNode());
              /* ENABLE_MIGRATION if */
              attachNodes.add(newMasterNode = checkAndAttachNode(newGroupAddrs.get(0)));
              /* else */
              /*
              attachNodes.add(newMasterNode = attachMemcachedNode(newGroupAddrs.get(0)));
               */
              /* ENABLE_MIGRATION end */


              /* move operation old master -> new master */
              taskList.add(new SetupResendTask(oldGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), newMasterNode));
            }
          } else { /* Old group has both a master node and a slave node. */
            if (newGroupAddrs.get(0).getIPPort().equals(oldMasterAddr.getIPPort())) {
              /* The old slave has disappeared. */
              removeNodes.add(oldGroup.getSlaveNode());

              /* move operation slave -> master
               * Slave node have only read operations, then don't need call setupResend
               */
              taskList.add(new MoveOperationTask(oldGroup.getSlaveNode(), oldGroup.getMasterNode()));
            } else if (newGroupAddrs.get(0).getIPPort().equals(oldSlaveAddr.getIPPort())) {
              /* The old slave has failovered to the master with new slave */
              removeNodes.add(oldGroup.getMasterNode());
              changeRoleGroups.add(oldGroup);

              /* move operation master -> slave */
              taskList.add(new SetupResendTask(oldGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), oldGroup.getSlaveNode()));
            } else {
              MemcachedNode newMasterNode;
              /* All nodes of old group have gone away. And, new master has appeared. */
              removeNodes.add(oldGroup.getMasterNode());
              removeNodes.add(oldGroup.getSlaveNode());
              /* ENABLE_MIGRATION if */
              attachNodes.add(newMasterNode = checkAndAttachNode(newGroupAddrs.get(0)));
              /* else */
              /*
              attachNodes.add(newMasterNode = attachMemcachedNode(newGroupAddrs.get(0)));
               */
              /* ENABLE_MIGRATION end */

              /* move operation old master -> new master */
              taskList.add(new SetupResendTask(oldGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), newMasterNode));

              /* move operation old slave -> new master
               * Slave node have only read operations, then don't need call setupResend
               */
              taskList.add(new MoveOperationTask(oldGroup.getSlaveNode(), newMasterNode));
            }
          }
        } else { /* New group has both a master node and a slave node */
          if (oldSlaveAddr == null) { /* Old group has only a master node. */
            if (newGroupAddrs.get(0).getIPPort().equals(oldMasterAddr.getIPPort())) {
              /* New slave node has appeared. */
              /* ENABLE_MIGRATION if */
              attachNodes.add(checkAndAttachNode(newGroupAddrs.get(1)));
              /* else */
              /*
              attachNodes.add(attachMemcachedNode(newGroupAddrs.get(1)));
               */
              /* ENABLE_MIGRATION end */
            } else if (newGroupAddrs.get(1).getIPPort().equals(oldMasterAddr.getIPPort())) {
              MemcachedNode newMasterNode;
              /* Really rare case: old master => slave, A new master */
              changeRoleGroups.add(oldGroup);
              /* ENABLE_MIGRATION if */
              attachNodes.add(newMasterNode = checkAndAttachNode(newGroupAddrs.get(0)));
              /* else */
              /*
              attachNodes.add(newMasterNode = attachMemcachedNode(newGroupAddrs.get(0)));
               */
              /* ENABLE_MIGRATION end */

              /* move operation old master -> new master */
              taskList.add(new QueueReconnectTask(oldGroup.getMasterNode(), ReconnDelay.IMMEDIATE, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), newMasterNode));
            } else {
              MemcachedNode newMasterNode;
              /* Old master has gone away. And, new group has appeared. */
              removeNodes.add(oldGroup.getMasterNode());
              /* ENABLE_MIGRATION if */
              attachNodes.add(newMasterNode = checkAndAttachNode(newGroupAddrs.get(0)));
              attachNodes.add(checkAndAttachNode(newGroupAddrs.get(1)));
              /* else */
              /*
              attachNodes.add(newMasterNode = attachMemcachedNode(newGroupAddrs.get(0)));
              attachNodes.add(attachMemcachedNode(newGroupAddrs.get(1)));
               */
              /* ENABLE_MIGRATION end */

              /* move operation old master -> new master */
              taskList.add(new SetupResendTask(oldGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), newMasterNode));
            }
          } else { /* Old group has both a master node and a slave node. */
            if (newGroupAddrs.get(0).getIPPort().equals(oldMasterAddr.getIPPort())) {
              if (newGroupAddrs.get(1).getIPPort().equals(oldSlaveAddr.getIPPort())) {
                /* The same master and slave nodes. Do nothing. */
              } else {
                MemcachedNode newSlaveNode;
                /* Only old slave has changed to the new one. */
                removeNodes.add(oldGroup.getSlaveNode());
                /* ENABLE_MIGRATION if */
                attachNodes.add(newSlaveNode = checkAndAttachNode(newGroupAddrs.get(1)));
                /* else */
                /*
                attachNodes.add(newSlaveNode = attachMemcachedNode(newGroupAddrs.get(1)));
                 */
                /* ENABLE_MIGRATION end */

                /* move operation old slave -> new slave
                 * Slave node have only read operations, then don't need call setupResend
                 */
                taskList.add(new MoveOperationTask(oldGroup.getSlaveNode(), newSlaveNode));
              }
            } else if (newGroupAddrs.get(0).getIPPort().equals(oldSlaveAddr.getIPPort())) {
              if (newGroupAddrs.get(1).getIPPort().equals(oldMasterAddr.getIPPort())) {
                /* Switchover */
                changeRoleGroups.add(oldGroup);

                /* move operation master -> slave
                 * must keep the following execution order when switchover
                 * - first moveOperations
                 * - second, queueReconnect
                 *
                 * because moves all operations
                 */
                taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), oldGroup.getSlaveNode()));
                taskList.add(new QueueReconnectTask(oldGroup.getMasterNode(), ReconnDelay.IMMEDIATE, "Discarded all pending reading state operation to move operations."));
              } else {
                /* Failover. And, new slave has appeared */
                removeNodes.add(oldGroup.getMasterNode());
                changeRoleGroups.add(oldGroup);
                /* ENABLE_MIGRATION if */
                attachNodes.add(checkAndAttachNode(newGroupAddrs.get(1)));
                /* else */
                /*
                attachNodes.add(attachMemcachedNode(newGroupAddrs.get(1)));
                */
                /* ENABLE_MIGRATION end */

                /* move operation old master -> old slave */
                taskList.add(new SetupResendTask(oldGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), oldGroup.getSlaveNode()));
              }
            } else {
              /* A completely new master has appered. */
              if (newGroupAddrs.get(1).getIPPort().equals(oldMasterAddr.getIPPort())) {
                /* Old slave disappeared. Old master has changed to the slave. */
                MemcachedNode newMasterNode;
                removeNodes.add(oldGroup.getSlaveNode());
                changeRoleGroups.add(oldGroup);
                /* ENABLE_MIGRATION if */
                attachNodes.add(newMasterNode = checkAndAttachNode(newGroupAddrs.get(0)));
                /* else */
                /*
                attachNodes.add(newMasterNode = attachMemcachedNode(newGroupAddrs.get(0)));
                */
                /* ENABLE_MIGRATION end */

                /* move operation old master -> new master */
                taskList.add(new QueueReconnectTask(oldGroup.getMasterNode(), ReconnDelay.IMMEDIATE, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), newMasterNode));

                /* move operation old slave -> old master(slave)
                 * Slave node have only read operations, then don't need call setupResend
                 */
                taskList.add(new MoveOperationTask(oldGroup.getSlaveNode(), oldGroup.getMasterNode()));
              } else if (newGroupAddrs.get(1).getIPPort().equals(oldSlaveAddr.getIPPort())) {
                MemcachedNode newMasterNode;
                /* Only old master has disappeared. */
                removeNodes.add(oldGroup.getMasterNode());
                /* ENABLE_MIGRATION if */
                attachNodes.add(newMasterNode = checkAndAttachNode(newGroupAddrs.get(0)));
                /* else */
                /*
                attachNodes.add(newMasterNode = attachMemcachedNode(newGroupAddrs.get(0)));
                */
                /* ENABLE_MIGRATION end */

                /* move operation old master -> new master */
                taskList.add(new SetupResendTask(oldGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), newMasterNode));
              } else {
                MemcachedNode newMasterNode;
                MemcachedNode newSlaveNode;
                /* Old group has completely changed to the new group. */
                removeNodes.add(oldGroup.getMasterNode());
                removeNodes.add(oldGroup.getSlaveNode());
                /* ENABLE_MIGRATION if */
                attachNodes.add(newMasterNode = checkAndAttachNode(newGroupAddrs.get(0)));
                attachNodes.add(newSlaveNode = checkAndAttachNode(newGroupAddrs.get(1)));
                /* else */
                /*
                attachNodes.add(newMasterNode = attachMemcachedNode(newGroupAddrs.get(0)));
                attachNodes.add(newSlaveNode = attachMemcachedNode(newGroupAddrs.get(1)));
                */
                /* ENABLE_MIGRATION end */

                /* move operation old master -> new master */
                taskList.add(new SetupResendTask(oldGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldGroup.getMasterNode(), newMasterNode));

                /* move operation old slave -> new slave
                 * Slave node have only read operations, then don't need call setupResend
                 */
                taskList.add(new MoveOperationTask(oldGroup.getSlaveNode(), newSlaveNode));
              }
            }
          }
        }
        newAllGroups.remove(entry.getKey());
      }

      for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllGroups.entrySet()) {
        List<ArcusReplNodeAddress> newGroupAddrs = entry.getValue();
        if (newGroupAddrs.size() == 0) { // Incomplete group, now
          /* ENABLE_MIGRATION if */
          if (_migrationMode != MigrationMode.Init) {
            /* During the migration, the slave can be created first. */
            continue;
          }
          /* ENABLE_MIGRATION end */
          attachNodes.add(checkAndAttachNode(ArcusReplNodeAddress.createFake(entry.getKey())));
        } else { // Completely new group
          attachNodes.add(checkAndAttachNode(newGroupAddrs.get(0)));
          if (newGroupAddrs.size() > 1)
            attachNodes.add(checkAndAttachNode(newGroupAddrs.get(1)));
        }
      }

      // Update the hash.
      ((ArcusReplKetamaNodeLocator) locator).update(attachNodes, removeNodes, changeRoleGroups);
      // do task after locator update
      for (Task t : taskList)
        t.doTask();
    } else {
      for (MemcachedNode node : locator.getAll()) {
        if (addrs.contains((InetSocketAddress) node.getSocketAddress())) {
          addrs.remove((InetSocketAddress) node.getSocketAddress());
        } else {
          removeNodes.add(node);
        }
      }

      for (SocketAddress sa : addrs) {
        /* ENABLE_MIGRATION if */
        attachNodes.add(checkAndAttachNode(sa));
        /* else */
        /*
        attachNodes.add(attachMemcachedNode(sa));
        */
        /* ENABLE_MIGRATION end */
      }

      // Update the hash.
      locator.update(attachNodes, removeNodes);
    }
    /* ENABLE_REPLICATION else */
    /*
    for (MemcachedNode node : locator.getAll()) {
      if (addrs.contains((InetSocketAddress) node.getSocketAddress())) {
        addrs.remove((InetSocketAddress) node.getSocketAddress());
      } else {
        removeNodes.add(node);
      }
    }

    // Make connections to the newly added nodes.
    for (SocketAddress sa : addrs) {
      attachNodes.add(attachMemcachedNode(sa));
    }

    // Update the hash.
    locator.update(attachNodes, removeNodes);
    */
    /* ENABLE_REPLICATION end */

    // Remove unavailable nodes in the reconnect queue.
    for (MemcachedNode node : removeNodes) {
      getLogger().info("old memcached node removed %s", node);
      for (Entry<Long, MemcachedNode> each : reconnectQueue.entrySet()) {
        if (node.equals(each.getValue())) {
          reconnectQueue.remove(each.getKey());
          break;
        }
      }
      String cause = "node removed.";
      if (failureMode == FailureMode.Cancel) {
        cancelOperations(node.destroyWriteQueue(false), cause);
        cancelOperations(node.destroyInputQueue(), cause);
      } else if (failureMode == FailureMode.Redistribute || failureMode == FailureMode.Retry) {
        redistributeOperations(node.destroyWriteQueue(true), cause);
        redistributeOperations(node.destroyInputQueue(), cause);
      }
    }
  }

  /* ENABLE_MIGRATION if */
  private MemcachedNode checkAndAttachNode(ArcusReplNodeAddress newAddr) throws IOException {
    boolean exist = false;
    MemcachedNode node = null;
    Map<String, MemcachedReplicaGroup> group = ((ArcusReplKetamaNodeLocator) locator).getAllMigrationGroups();
    for (Map.Entry<String, MemcachedReplicaGroup> g : group.entrySet()) {
      ArcusReplNodeAddress masterAddr = g.getValue().getMasterNode() != null ?
              (ArcusReplNodeAddress) g.getValue().getMasterNode().getSocketAddress() : null;
      ArcusReplNodeAddress slaveAddr = g.getValue().getSlaveNode() != null ?
              (ArcusReplNodeAddress) g.getValue().getSlaveNode().getSocketAddress() : null;
      if (masterAddr != null && newAddr.getIPPort().equals(masterAddr.getIPPort())) {
        node = g.getValue().getMasterNode();
        exist = true;
        break;
      } else if (slaveAddr != null && newAddr.getIPPort().equals(slaveAddr.getIPPort())) {
        node = g.getValue().getSlaveNode();
        exist = true;
        break;
      }
    }
    if (!exist) {
      node = attachMemcachedNode(newAddr);
    }
    return node;
  }

  private MemcachedNode checkAndAttachNode(SocketAddress newAddr) throws IOException {
    return attachMemcachedNode(newAddr);
  }

  private void updateMigrations(List<InetSocketAddress> addrs, MigrationMode mode) throws IOException {
    List<MemcachedNode> attachNodes = new ArrayList<MemcachedNode>();
    List<MemcachedNode> removeNodes = new ArrayList<MemcachedNode>();

    getLogger().info("manage migration cluster " + mode + " : " + addrs);

    if (arcusReplEnabled) {
      List<MemcachedReplicaGroup> changeRoleGroups = new ArrayList<MemcachedReplicaGroup>();
      List<Task> taskList = new ArrayList<Task>();

      Map<String, List<ArcusReplNodeAddress>> newAllMGGroups =
              ArcusReplNodeAddress.makeGroupAddrsList(addrs);

      Map<String, MemcachedReplicaGroup> oldMGAllGroups =
              ((ArcusReplKetamaNodeLocator) locator).getAllMigrationGroups();

      for (Map.Entry<String, MemcachedReplicaGroup> entry : oldMGAllGroups.entrySet()) {
        MemcachedReplicaGroup oldMGGroup = entry.getValue();

        List<ArcusReplNodeAddress> newMGGroupAddrs = newAllMGGroups.get(entry.getKey());

        ArcusClient.arcusLogger.debug("New migration group nodes : " + newMGGroupAddrs);
        ArcusClient.arcusLogger.debug("Old migration group nodes : [" + entry.getValue() + "]");

        if (newMGGroupAddrs == null) {
          /* Old group nodes have disappered. Remove the old group nodes. */
          removeNodes.add(oldMGGroup.getMasterNode());
          if (oldMGGroup.getSlaveNode() != null)
            removeNodes.add(oldMGGroup.getSlaveNode());
          continue;
        }
        if (newMGGroupAddrs.size() == 0) {
          /* New group is invalid, do nothing. */
          newAllMGGroups.remove(entry.getKey());
          continue;
        }

        ArcusReplNodeAddress oldMGMasterAddr = oldMGGroup.getMasterNode() != null ?
                (ArcusReplNodeAddress) oldMGGroup.getMasterNode().getSocketAddress() : null;
        ArcusReplNodeAddress oldMGSlaveAddr = oldMGGroup.getSlaveNode() != null ?
                (ArcusReplNodeAddress) oldMGGroup.getSlaveNode().getSocketAddress() : null;
        assert (oldMGMasterAddr != null);

        if (newMGGroupAddrs.size() == 1) { /* New group has only a master node. */
          if (oldMGSlaveAddr == null) { /* Old group has only a master node. */
            if (newMGGroupAddrs.get(0).getIPPort().equals(oldMGMasterAddr.getIPPort())) {
              /* The same master node. Do nothing. */
            } else {
              MemcachedNode newMasterNode;
              /* The master of the group has changed to the new one. */
              removeNodes.add(oldMGGroup.getMasterNode());
              attachNodes.add(newMasterNode = mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));

              /* move operation old master -> new master */
              taskList.add(new SetupResendTask(oldMGGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), newMasterNode));
            }
          } else { /* Old group has both a master node and a slave node. */
            if (newMGGroupAddrs.get(0).getIPPort().equals(oldMGMasterAddr.getIPPort())) {
              /* The old slave has disappeared. */
              removeNodes.add(oldMGGroup.getSlaveNode());

              /* move operation slave -> master
               * Slave node have only read operations, then don't need call setupResend
               */
              taskList.add(new MoveOperationTask(oldMGGroup.getSlaveNode(), oldMGGroup.getMasterNode()));
            } else if (newMGGroupAddrs.get(0).getIPPort().equals(oldMGSlaveAddr.getIPPort())) {
              /* The old slave has failovered to the master with new slave */
              removeNodes.add(oldMGGroup.getMasterNode());
              changeRoleGroups.add(oldMGGroup);

              /* move operation master -> slave */
              taskList.add(new SetupResendTask(oldMGGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), oldMGGroup.getSlaveNode()));
            } else {
              MemcachedNode newMasterNode;
              /* All nodes of old group have gone away. And, new master has appeared. */
              removeNodes.add(oldMGGroup.getMasterNode());
              removeNodes.add(oldMGGroup.getSlaveNode());
              attachNodes.add(newMasterNode = mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));

              /* move operation old master -> new master */
              taskList.add(new SetupResendTask(oldMGGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), newMasterNode));

              /* move operation old slave -> new master
               * Slave node have only read operations, then don't need call setupResend
               */
              taskList.add(new MoveOperationTask(oldMGGroup.getSlaveNode(), newMasterNode));
            }
          }
        } else { /* New group has both a master node and a slave node */
          if (oldMGSlaveAddr == null) { /* Old group has only a master node. */
            if (newMGGroupAddrs.get(0).getIPPort().equals(oldMGMasterAddr.getIPPort())) {
              /* New slave node has appeared. */
              attachNodes.add(mgCheckAndAttachNode(newMGGroupAddrs.get(1), mode));
            } else if (newMGGroupAddrs.get(1).getIPPort().equals(oldMGMasterAddr.getIPPort())) {
              MemcachedNode newMasterNode;
              /* Really rare case: old master => slave, A new master */
              changeRoleGroups.add(oldMGGroup);
              attachNodes.add(newMasterNode = mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));

              /* move operation old master -> new master */
              taskList.add(new QueueReconnectTask(oldMGGroup.getMasterNode(), ReconnDelay.IMMEDIATE, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), newMasterNode));
            } else {
              MemcachedNode newMasterNode;
              /* Old master has gone away. And, new group has appeared. */
              removeNodes.add(oldMGGroup.getMasterNode());
              attachNodes.add(newMasterNode = mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));
              attachNodes.add(mgCheckAndAttachNode(newMGGroupAddrs.get(1), mode));

              /* move operation old master -> new master */
              taskList.add(new SetupResendTask(oldMGGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
              taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), newMasterNode));
            }
          } else { /* Old group has both a master node and a slave node. */
            if (newMGGroupAddrs.get(0).getIPPort().equals(oldMGMasterAddr.getIPPort())) {
              if (newMGGroupAddrs.get(1).getIPPort().equals(oldMGSlaveAddr.getIPPort())) {
                /* The same master and slave nodes. Do nothing. */
              } else {
                MemcachedNode newSlaveNode;
                /* Only old slave has changed to the new one. */
                removeNodes.add(oldMGGroup.getSlaveNode());
                attachNodes.add(newSlaveNode = mgCheckAndAttachNode(newMGGroupAddrs.get(1), mode));

                /* move operation old slave -> new slave
                 * Slave node have only read operations, then don't need call setupResend
                 */
                taskList.add(new MoveOperationTask(oldMGGroup.getSlaveNode(), newSlaveNode));
              }
            } else if (newMGGroupAddrs.get(0).getIPPort().equals(oldMGSlaveAddr.getIPPort())) {
              if (newMGGroupAddrs.get(1).getIPPort().equals(oldMGMasterAddr.getIPPort())) {
                /* Switchover */
                changeRoleGroups.add(oldMGGroup);

                /* move operation master -> slave
                 * must keep the following execution order when switchover
                 * - first moveOperations
                 * - second, queueReconnect
                 *
                 * because moves all operations
                 */
                taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), oldMGGroup.getSlaveNode()));
                taskList.add(new QueueReconnectTask(oldMGGroup.getMasterNode(), ReconnDelay.IMMEDIATE, "Discarded all pending reading state operation to move operations."));
              } else {
                /* Failover. And, new slave has appeared */
                removeNodes.add(oldMGGroup.getMasterNode());
                changeRoleGroups.add(oldMGGroup);
                attachNodes.add(mgCheckAndAttachNode(newMGGroupAddrs.get(1), mode));

                /* move operation old master -> old slave */
                taskList.add(new SetupResendTask(oldMGGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), oldMGGroup.getSlaveNode()));
              }
            } else {
              /* A completely new master has appered. */
              if (newMGGroupAddrs.get(1).getIPPort().equals(oldMGMasterAddr.getIPPort())) {
                /* Old slave disappeared. Old master has changed to the slave. */
                MemcachedNode newMasterNode;
                removeNodes.add(oldMGGroup.getSlaveNode());
                changeRoleGroups.add(oldMGGroup);
                attachNodes.add(newMasterNode = mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));

                /* move operation old master -> new master */
                taskList.add(new QueueReconnectTask(oldMGGroup.getMasterNode(), ReconnDelay.IMMEDIATE, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), newMasterNode));

                /* move operation old slave -> old master(slave)
                 * Slave node have only read operations, then don't need call setupResend
                 */
                taskList.add(new MoveOperationTask(oldMGGroup.getSlaveNode(), oldMGGroup.getMasterNode()));
              } else if (newMGGroupAddrs.get(1).getIPPort().equals(oldMGSlaveAddr.getIPPort())) {
                MemcachedNode newMasterNode;
                /* Only old master has disappeared. */
                removeNodes.add(oldMGGroup.getMasterNode());
                attachNodes.add(newMasterNode = mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));

                /* move operation old master -> new master */
                taskList.add(new SetupResendTask(oldMGGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), newMasterNode));
              } else {
                MemcachedNode newMasterNode;
                MemcachedNode newSlaveNode;
                /* Old group has completely changed to the new group. */
                removeNodes.add(oldMGGroup.getMasterNode());
                removeNodes.add(oldMGGroup.getSlaveNode());
                attachNodes.add(newMasterNode = mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));
                attachNodes.add(newSlaveNode = mgCheckAndAttachNode(newMGGroupAddrs.get(1), mode));

                /* move operation old master -> new master */
                taskList.add(new SetupResendTask(oldMGGroup.getMasterNode(), false, "Discarded all pending reading state operation to move operations."));
                taskList.add(new MoveOperationTask(oldMGGroup.getMasterNode(), newMasterNode));

                /* move operation old slave -> new slave
                 * Slave node have only read operations, then don't need call setupResend
                 */
                taskList.add(new MoveOperationTask(oldMGGroup.getSlaveNode(), newSlaveNode));
              }
            }
          }
        }
        newAllMGGroups.remove(entry.getKey());
      }

      for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllMGGroups.entrySet()) {
        List<ArcusReplNodeAddress> newMGGroupAddrs = entry.getValue();
        if (newMGGroupAddrs.size() == 0) { /* Incomplete group, now */
          /* do nothing */
        } else { /* Completely new group */
          attachNodes.add(mgCheckAndAttachNode(newMGGroupAddrs.get(0), mode));
          if (newMGGroupAddrs.size() > 1)
            attachNodes.add(mgCheckAndAttachNode(newMGGroupAddrs.get(1), mode));
        }
      }

      // Update the hash and migration hash.
      ((ArcusReplKetamaNodeLocator) locator).updateMigration(attachNodes, removeNodes, mode, changeRoleGroups);
      // do task after locator update
      for (Task t : taskList)
        t.doTask();
    }
  }

  private MemcachedNode mgCheckAndAttachNode(ArcusReplNodeAddress newAddr, MigrationMode mode) throws IOException {
    boolean exist = false;
    MemcachedNode node = null;
    Map<String, MemcachedReplicaGroup> group = (mode == MigrationMode.Join) ?
            ((ArcusReplKetamaNodeLocator) locator).getAllMigrationGroups() :
            ((ArcusReplKetamaNodeLocator) locator).getAllGroups();
    for (Map.Entry<String, MemcachedReplicaGroup> g : group.entrySet()) {
      ArcusReplNodeAddress masterAddr = g.getValue().getMasterNode() != null ?
              (ArcusReplNodeAddress) g.getValue().getMasterNode().getSocketAddress() : null;
      ArcusReplNodeAddress slaveAddr = g.getValue().getSlaveNode() != null ?
              (ArcusReplNodeAddress) g.getValue().getSlaveNode().getSocketAddress() : null;
      if (masterAddr != null && newAddr.getIPPort().equals(masterAddr.getIPPort())) {
        node = g.getValue().getMasterNode();
        exist = true;
        break;
      } else if (slaveAddr != null && newAddr.getIPPort().equals(slaveAddr.getIPPort())) {
        node = g.getValue().getSlaveNode();
        exist = true;
        break;
      }
    }
    if (!exist && mode == MigrationMode.Join) {
      node = attachMemcachedNode(newAddr);
    }
    return node;
  }

  private MemcachedNode mgCheckAndAttachNode(SocketAddress newAddr, MigrationMode mode) throws IOException {
    MemcachedNode node = null;
    for (MemcachedNode tmp : ((ArcusKetamaNodeLocator) locator).getAll()) {
      if (newAddr.equals(tmp.getSocketAddress())) {
        node = tmp;
        break;
      }
    }
    if (node == null && mode == MigrationMode.Join) {
      node = attachMemcachedNode(newAddr);
    }
    return node;
  }

  private <T> BTreeGetBulk<T> makeChildBtreeGetBulk(Map<String, Object> arguments, List<String> keyList) {
    String range = (String) arguments.get("range");
    ElementFlagFilter eFlagFilter = (ElementFlagFilter) arguments.get("eFlagFilter");
    int offset = (Integer) arguments.get("offset");
    int count = (Integer) arguments.get("count");
    boolean reverse = (Boolean) arguments.get("reverse");

    if ((Boolean) arguments.get("byteBkey")) {
      return new BTreeGetBulkWithByteTypeBkey<T>(keyList, range, eFlagFilter, offset, count, reverse);
    } else {
      return new BTreeGetBulkWithLongTypeBkey<T>(keyList, range, eFlagFilter, offset, count, reverse);
    }
  }

  private MemcachedReplicaGroup getOwnerGroup(MemcachedNode node, String ownerName) {
    assert arcusReplEnabled;

    MigrationMode mode = ((ArcusReplKetamaNodeLocator) locator).getMigrationMode();
    Map<String, MemcachedReplicaGroup> groups;
    Map<String, MemcachedReplicaGroup> subGroups;
    MemcachedReplicaGroup group;

    if (mode == MigrationMode.Join) {
      groups = ((ArcusReplKetamaNodeLocator) locator).getAllMigrationGroups();
      subGroups = ((ArcusReplKetamaNodeLocator) locator).getAllGroups();
      if (groups.containsKey(ownerName)) {
        group = groups.get(ownerName);
      } else {
        do {
          if (subGroups.containsKey(ownerName)) {
            group = subGroups.get(ownerName);
            break;
          }
          group = node.getReplicaGroup();
        } while (false);
      }
    } else {
      /* mode == MigrationMode.Init || mode == MigrationMode.Leave */
      groups = ((ArcusReplKetamaNodeLocator) locator).getAllGroups();
      if (groups.containsKey(ownerName)) {
        group = groups.get(ownerName);
      } else {
        group = node.getReplicaGroup();
      }
    }
    return group;
  }

  private void multipleKeyMigrationgOp(MemcachedNode node, Operation operation) {
    /* multiple key operation */
    Map<MemcachedNode, List<Collection<String>>> chunks = new HashMap<MemcachedNode, List<Collection<String>>>();
    Map<MemcachedNode, Integer> chunkCount = new HashMap<MemcachedNode, Integer>();

    for (int i = 0; i < operation.getMgResponseSize(); i++) {
      /* parsing migration response string */
      String[] splitedResponse = operation.getMgResponse(i).split(" ");
      assert splitedResponse.length == 3;

      String key = splitedResponse[1];
      String ownerName = splitedResponse[2];

      /* TODO::refactoring with findNodeByKey(key) */
      if (arcusReplEnabled) {
        MemcachedReplicaGroup group = getOwnerGroup(node, ownerName);
        if (group.getMasterNode() != null) {
          MemcachedNode nd = group.getMasterNode();
          List<Collection<String>> lks = chunks.get(nd);
          if (lks == null) {
            lks = new ArrayList<Collection<String>>();
            Collection<String> ts = new ArrayList<String>();
            lks.add(0, ts);
            chunks.put(nd, lks);
            chunkCount.put(nd, 0);
          }
          if (lks.get(chunkCount.get(nd)).size() >= 200 /* GET_BULK_CHUNK_SIZE */) {
            int count = chunkCount.get(nd) + 1;
            Collection<String> ts = new ArrayList<String>();
            lks.add(count, ts);
            chunkCount.put(nd, count);
          }
          Collection<String> ks = lks.get(chunkCount.get(nd));
          ks.add(key);
        } else {
          getLogger().warn("Delay NOT_MY_KEY because invalid group state : " + group);
        }
      }
    }

    int moveOperationCount = 0;
    for (Map.Entry<MemcachedNode, List<Collection<String>>> me
            : chunks.entrySet()) {
      MemcachedNode nd = me.getKey();
      for (int i = 0; i <= chunkCount.get(nd); i++) {
        Operation op = null;
        if (operation.getAPIType() == APIType.GET) {
          /* GetBulk Operation */
          op = opFact.get(me.getValue().get(i), (GetOperation.Callback) operation.getCallback(), operation);
        } else if (operation.getAPIType() == APIType.MGET) {
          /* MGet Operation */
          op = opFact.mget(me.getValue().get(i), (GetOperation.Callback) operation.getCallback(), operation);
        } else if (operation.getAPIType() == APIType.BOP_GET) {
          /* BTreeGetBulk Operation */
          Map<String, Object> arguments = operation.getArguments();
          BTreeGetBulk getBulk = makeChildBtreeGetBulk(arguments, (List<String>) me.getValue().get(i));
          op = opFact.bopGetBulk(getBulk, (BTreeGetBulkOperation.Callback<?>) operation.getCallback(), operation);
        }
        assert op != null;
        node.moveOperation(op, nd);
        addedQueue.offer(nd);
        moveOperationCount++;
        Selector s = selector.wakeup();
        assert s == selector : "Wakeup returned the wrong selector.";
      }
    }
    operation.setMoved(true);
    operation.setMigratingCount(moveOperationCount);
  }

  private void singleKeyMigratingOp(MemcachedNode node, Operation operation) {
    /* single key operation */
    String[] splitedResponse = operation.getMgResponse(0).split(" ");
    String ownerName = splitedResponse[1];

    if (operation instanceof CollectionBulkStoreOperation) {
      CollectionBulkStoreOperationImpl o = (CollectionBulkStoreOperationImpl) operation;
      List<String> keys = o.getStore().getKeyList();
      if (o.getStore().getItemCount() > 1) {
        int index = o.getStore().getNextOpIndex();
        int moveOperationCount = 0;
        for (int i = index; i < keys.size(); i++) {
          String key = keys.get(i);
          List<String> singleKeyList = new ArrayList<String>(Collections.singleton(key));
          CollectionBulkStore<?> st = o.getStore().makeSingleStore(singleKeyList);

          Operation op = opFact.collectionBulkStore(singleKeyList, st, operation.getCallback(), operation);
          assert op != null;

          /* check for replication in findNodeByKey() */
          MemcachedNode nd = findNodeByKey(key);
          node.moveOperation(op, nd);
          addedQueue.offer(nd);
          moveOperationCount++;
          Selector s = selector.wakeup();
          assert s == selector : "Wakeup returned the wrong selector.";
        }
        operation.setMoved(true);
        operation.setMigratingCount(moveOperationCount);
        return;
      }
    }

    if (arcusReplEnabled) {
      MemcachedReplicaGroup group = getOwnerGroup(node, ownerName);
      if (group.getMasterNode() != null) {
        /*  must keep the following execution order when NOT_MY_KEY
         * - first moveOperations
         * - second, selector wakeup
         */
        MemcachedNode nd = group.getMasterNode();
        node.moveOperation(operation, nd);
        addedQueue.offer(nd);
        Selector s = selector.wakeup();
        assert s == selector : "Wakeup returned the wrong selector.";
      } else {
        getLogger().warn("Delay NOT_MY_KEY because invalid group state : " + group);
      }
    }
  }

  private void moveMigratingOp(MemcachedNode node, Operation operation) {
    /* Move and Retry the Operation */
    /* response
     * SINGLE KEY   : NOT_MY_KEY <owner name>
     * MULTIPLE KEY : NOT_MY_KEY <key> <owner name>
     */
    String[] splitForCheckMultipleKey = operation.getMgResponse(0).split(" ");
    if (splitForCheckMultipleKey.length > 2) {
      multipleKeyMigrationgOp(node, operation);
    } else {
      singleKeyMigratingOp(node, operation);
    }
  }

  public void putZnodeQueue(ZnodeType ztype, Object arg) {
    _znodeManageQueue.offer(new ZnodeMap(ztype, arg));
  }

  public void handleZnodeManageQueue() throws IOException {
    if (_znodeManageQueue.isEmpty()) {
      return;
    }

    ZnodeMap znodeMap = _znodeManageQueue.poll();
    ZnodeType ztype = znodeMap.getZtype();
    Object arg = znodeMap.getArgument();

    if (ztype == ZnodeType.CacheList) {
      /* handle node list */

      // Get addresses from the queue
      String addrs = (String) arg;

      // Update the memcached server group.
      if (arcusReplEnabled) {
        updateConnections(ArcusReplNodeAddress.getAddresses(addrs));
      } else {
        updateConnections(AddrUtil.getAddresses(addrs));
      }
    } else if (ztype == ZnodeType.MigrationList) {
      /* handle migration alter node list */
      MigrationMap mgMap = (MigrationMap) arg;
      String addrs = mgMap.getAddr();
      MigrationMode mode = mgMap.getMode();

      // Do update migration hash ring
      if (mode == MigrationMode.Init) {
        /* cleanup migration information.
         * migration was done or failed.
          */
        if (_migrationMode != MigrationMode.Init) {
          if (arcusReplEnabled) {
            ((ArcusReplKetamaNodeLocator) locator).cleanupMigration();
          }
        }
      } else {
        if (arcusReplEnabled) {
          updateMigrations(ArcusReplNodeAddress.getAddresses(addrs), mode);
        } else {
          updateMigrations(AddrUtil.getAddresses(addrs), mode);
        }
      }
      _migrationMode = mode;
    } else if (ztype == ZnodeType.MigrationState) {
      /* handle migrations znode */
      List<String> migrations = new ArrayList<String>();
      for (int i = 0; i < ((List) arg).size(); i++) {
        migrations.add(String.valueOf(((List) arg).get(i)));
      }

      if (arcusReplEnabled) {
        ((ArcusReplKetamaNodeLocator) locator).reflectMigratedHash(migrations);
      }
    } else {
      /* set migration version of all MemcachedNode */
      assert ztype == ZnodeType.MigrationVersion;
      long version = Long.valueOf((Long) arg);
      setMigrationVersion(version);
    }
  }
  /* ENABLE_MIGRATION end */

  /* ENABLE_REPLICATION if */
  private void switchoverMemcachedReplGroup(MemcachedNode node) {
    MemcachedReplicaGroup group = node.getReplicaGroup();

    /*  must keep the following execution order when switchover
     * - first moveOperations
     * - second, queueReconnect
     *
     * because moves all operations
     */
    if (group.getMasterNode() != null && group.getSlaveNode() != null) {
      if (((ArcusReplNodeAddress) node.getSocketAddress()).master) {
        node.moveOperations(group.getSlaveNode());
        addedQueue.offer(group.getSlaveNode());
        ((ArcusReplKetamaNodeLocator) locator).switchoverReplGroup(group);
      } else {
        node.moveOperations(group.getMasterNode());
        addedQueue.offer(group.getMasterNode());
      }
      queueReconnect(node, ReconnDelay.IMMEDIATE, "Discarded all pending reading state operation to move operations.");
    } else {
      getLogger().warn("Delay switchover because invalid group state : " + group);
    }
  }

  /* ENABLE_REPLICATION end */
  MemcachedNode attachMemcachedNode(SocketAddress sa) throws IOException {
    SocketChannel ch = SocketChannel.open();
    ch.configureBlocking(false);
    // bufSize : 16384 (default value)
    MemcachedNode qa = f.createMemcachedNode(sa, ch, f.getReadBufSize());
    if (timeoutRatioThreshold > 0) {
      qa.enableTimeoutRatio();
    }
    int ops = 0;
    ch.socket().setTcpNoDelay(!f.useNagleAlgorithm());
    ch.socket().setReuseAddress(true);
    /* ENABLE_REPLICATION if */
    // Do not attempt to connect if this node is fake.
    // Otherwise, we keep connecting to a non-existent listen address
    // and keep failing/reconnecting.
    if (qa.isFake()) {
      // Locator assumes non-null selectionkey.  So add a dummy one...
      ops = SelectionKey.OP_CONNECT;
      qa.setSk(ch.register(selector, ops, qa));
      getLogger().info("new fake memcached node added %s to connect queue", qa);
      return qa;
    }
    /* ENABLE_REPLICATION end */
    // Initially I had attempted to skirt this by queueing every
    // connect, but it considerably slowed down start time.
    try {
      if (ch.connect(sa)) {
        getLogger().info("new memcached node connected to %s immediately", qa);
        // FIXME.  Do we ever execute this path?
        // This method does not call observer.connectionEstablished.
        qa.connected();
      } else {
        getLogger().info("new memcached node added %s to connect queue", qa);
        ops = SelectionKey.OP_CONNECT;
      }
      qa.setSk(ch.register(selector, ops, qa));
      assert ch.isConnected()
              || qa.getSk().interestOps() == SelectionKey.OP_CONNECT
              : "Not connected, and not wanting to connect";
    } catch (SocketException e) {
      getLogger().warn("new memcached socket error on initial connect");
      queueReconnect(qa, ReconnDelay.DEFAULT, "initial connection error");
    }
    prepareVersionInfo(qa, sa);
    return qa;
  }

  private void prepareVersionInfo(final MemcachedNode node, final SocketAddress sa) {
    Operation op = opFact.version(new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        versions.put(sa, status.getMessage());
      }

      @Override
      public void complete() {
        setVersionInfo(node);
      }
    });
    addOperation(node, op);
  }

  private void setVersionInfo(MemcachedNode node) {
    if (node.getVersion() == null) {
      if (versions.containsKey(node.getSocketAddress())) {
        node.setVersion(versions.remove(node.getSocketAddress()));
      } else {
        prepareVersionInfo(node, node.getSocketAddress());
      }
    }
  }

  /* ENABLE_MIGRATION if */
  private void sendMigrationVersion(final MemcachedNode node, final long version) {
    Operation op = opFact.cluster(version, new OperationCallback() {
      public void receivedStatus(OperationStatus s) {
        /* do nothing */
      }

      public void complete() {
        /* do nothing */
      }
    });
    addOperation(node, op);
  }

  private void setMigrationVersion(long version) {
    getLogger().debug("Send cluster version to all Node, version = %d", version);

    if (arcusReplEnabled) {
      Map<String, MemcachedReplicaGroup> allGroups =
              ((ArcusReplKetamaNodeLocator) locator).getAllGroups();
      for (Map.Entry<String, MemcachedReplicaGroup> entry : allGroups.entrySet()) {
        MemcachedReplicaGroup group = entry.getValue();
        if (group.getMasterNode() != null) {
          sendMigrationVersion(group.getMasterNode(), version);
        }
        if (group.getSlaveNode() != null) {
          sendMigrationVersion(group.getSlaveNode(), version);
        }
      }
    } else {
      for (MemcachedNode node : locator.getAll()) {
        sendMigrationVersion(node, version);
      }
    }
  }

  /* ENABLE_MIGRATION end */

  /* ENABLE_MIGRATION if */
  /* else */
  /* TODO::check and remove later.
  public void putMemcachedQueue(String addrs) {
    _nodeManageQueue.offer(addrs);
  }

  // Handle the memcached server group that's been added by CacheManager.
  void handleNodeManageQueue() throws IOException {
    if (_nodeManageQueue.isEmpty()) {
      return;
    }

    // Get addresses from the queue
    String addrs = _nodeManageQueue.poll();

    // Update the memcached server group.
    /* ENABLE_REPLICATION if */
  //if (arcusReplEnabled)
  //	updateConnections(ArcusReplNodeAddress.getAddresses(addrs));
  //else
  //	updateConnections(AddrUtil.getAddresses(addrs));
    /* ENABLE_REPLICATION else */
    /*
    updateConnections(AddrUtil.getAddresses(addrs));
    */
    /* ENABLE_REPLICATION end */
  //}
  /* ENABLE_MIGRATION end */


  // Handle any requests that have been made against the client.
  private void handleInputQueue() {
    if (!addedQueue.isEmpty()) {
      getLogger().debug("Handling queue");
      // If there's stuff in the added queue.  Try to process it.
      Collection<MemcachedNode> toAdd = new HashSet<MemcachedNode>();
      // Transfer the queue into a hashset.  There are very likely more
      // additions than there are nodes.
      Collection<MemcachedNode> todo = new HashSet<MemcachedNode>();

      MemcachedNode node;
      while ((node = addedQueue.poll()) != null) {
        todo.add(node);
      }

      // Now process the queue.
      for (MemcachedNode qa : todo) {
        boolean readyForIO = false;
        if (qa.isActive()) {
          if (qa.getCurrentWriteOp() != null) {
            readyForIO = true;
            getLogger().debug("Handling queued write %s", qa);
          }
        } else {
          toAdd.add(qa);
        }
        qa.copyInputQueue();
        if (readyForIO) {
          try {
            if (qa.getWbuf().hasRemaining()) {
              handleWrites(qa.getSk(), qa);
            }
          } catch (IOException e) {
            getLogger().warn("Exception handling write", e);
            lostConnection(qa, ReconnDelay.DEFAULT, "exception handling write");
          }
        }
        qa.fixupOps();
      }
      addedQueue.addAll(toAdd);
    }
  }

  /**
   * Add a connection observer.
   *
   * @return whether the observer was successfully added
   */
  public boolean addObserver(ConnectionObserver obs) {
    return connObservers.add(obs);
  }

  /**
   * Remove a connection observer.
   *
   * @return true if the observer existed and now doesn't
   */
  public boolean removeObserver(ConnectionObserver obs) {
    return connObservers.remove(obs);
  }

  private void connected(MemcachedNode qa) {
    assert qa.getChannel().isConnected() : "Not connected.";
    int rt = qa.getReconnectCount();
    qa.connected();
    for (ConnectionObserver observer : connObservers) {
      observer.connectionEstablished(qa.getSocketAddress(), rt);
    }
  }

  private void lostConnection(MemcachedNode qa, ReconnDelay type, String cause) {
    queueReconnect(qa, type, cause);
    for (ConnectionObserver observer : connObservers) {
      observer.connectionLost(qa.getSocketAddress());
    }
  }

  // Handle IO for a specific selector.  Any IOException will cause a
  // reconnect
  private void handleIO(SelectionKey sk) {
    MemcachedNode qa = (MemcachedNode) sk.attachment();
    try {
      getLogger().debug(
              "Handling IO for:  %s (r=%s, w=%s, c=%s, op=%s)",
              sk, sk.isReadable(), sk.isWritable(),
              sk.isConnectable(), sk.attachment());
      if (sk.isConnectable()) {
        getLogger().info("Connection state changed for %s", sk);
        final SocketChannel channel = qa.getChannel();
        if (channel.finishConnect()) {
          connected(qa);
          addedQueue.offer(qa);
          if (qa.getWbuf().hasRemaining()) {
            handleWrites(sk, qa);
          }
        } else {
          assert !channel.isConnected() : "connected";
        }
      } else {
        if (sk.isValid() && sk.isReadable()) {
          handleReads(sk, qa);
        }
        if (sk.isValid() && sk.isWritable()) {
          handleWrites(sk, qa);
        }
      }
    } catch (ClosedChannelException e) {
      // Note, not all channel closes end up here
      if (!shutDown) {
        getLogger().info("Closed channel and not shutting down.  "
                + "Queueing reconnect on %s", qa, e);
        lostConnection(qa, ReconnDelay.DEFAULT, "closed channel");
      }
    } catch (ConnectException e) {
      // Failures to establish a connection should attempt a reconnect
      // without signaling the observers.
      getLogger().info("Reconnecting due to failure to connect to %s",
              qa, e);
      queueReconnect(qa, ReconnDelay.DEFAULT, "failure to connect");
    } catch (OperationException e) {
      qa.setupForAuth("authentication failure"); // noop if !shouldAuth
      getLogger().info("Reconnection due to exception " +
              "handling a memcached operation on %s.  " +
              "This may be due to an authentication failure.", qa, e);
      lostConnection(qa, ReconnDelay.IMMEDIATE, "authentication failure");
    } catch (Exception e) {
      // Any particular error processing an item should simply
      // cause us to reconnect to the server.
      //
      // One cause is just network oddness or servers
      // restarting, which lead here with IOException

      qa.setupForAuth("due to exception"); // noop if !shouldAuth
      getLogger().info("Reconnecting due to exception on %s", qa, e);
      lostConnection(qa, ReconnDelay.DEFAULT, "exception" + e);
    }
    qa.fixupOps();
  }

  private void handleWrites(SelectionKey sk, MemcachedNode qa)
          throws IOException {
    qa.fillWriteBuffer(shouldOptimize);
    boolean canWriteMore = qa.getBytesRemainingToWrite() > 0;
    while (canWriteMore) {
      int wrote = qa.writeSome();
      qa.fillWriteBuffer(shouldOptimize);
      canWriteMore = wrote > 0 && qa.getBytesRemainingToWrite() > 0;
    }
  }

  private void handleReads(SelectionKey sk, MemcachedNode qa)
          throws IOException {
    Operation currentOp = qa.getCurrentReadOp();
    ByteBuffer rbuf = qa.getRbuf();
    final SocketChannel channel = qa.getChannel();
    int read = channel.read(rbuf);
    while (read > 0) {
      getLogger().debug("Read %d bytes", read);
      rbuf.flip();
      while (rbuf.remaining() > 0) {
        if (currentOp == null) {
          throw new IllegalStateException("No read operation.");
        }
        currentOp.readFromBuffer(rbuf);
        if (currentOp.getState() == OperationState.COMPLETE) {
          getLogger().debug(
                  "Completed read op: %s and giving the next %d bytes",
                  currentOp, rbuf.remaining());
          Operation op = qa.removeCurrentReadOp();
          assert op == currentOp
                  : "Expected to pop " + currentOp + " got " + op;
          currentOp = qa.getCurrentReadOp();
        }
        /* ENABLE_REPLICATION if */
        else if (currentOp.getState() == OperationState.MOVING) {
          break;
        }
        /* ENABLE_REPLICATION end */
        /* ENABLE_MIGRATION if */
        else if (currentOp.getState() == OperationState.MIGRATING) {
          Operation op = qa.removeCurrentReadOp();
          assert op == currentOp
                  : "Expected to pop " + currentOp + " got " + op;
          moveMigratingOp(qa, currentOp);
          currentOp = qa.getCurrentReadOp();
        }
        /* ENABLE_MIGRATION end */
      }
      /* ENABLE_REPLICATION if */

      if (currentOp != null && currentOp.getState() == OperationState.MOVING) {
        rbuf.clear();
        switchoverMemcachedReplGroup(qa);
        break;
      }
      /* ENABLE_REPLICATION end */
      rbuf.clear();
      read = channel.read(rbuf);
    }
    if (read < 0) {
      // our model is to keep the connection alive for future ops
      // so we'll queue a reconnect if disconnected via an IOException
      throw new IOException("Disconnected unexpected, will reconnect.");
    }
  }

  // Make a debug string out of the given buffer's values
  static String dbgBuffer(ByteBuffer b, int size) {
    StringBuilder sb = new StringBuilder();
    byte[] bytes = b.array();
    for (int i = 0; i < size; i++) {
      char ch = (char) bytes[i];
      if (Character.isWhitespace(ch) || Character.isLetterOrDigit(ch)) {
        sb.append(ch);
      } else {
        sb.append("\\x");
        sb.append(Integer.toHexString(bytes[i] & 0xff));
      }
    }
    return sb.toString();
  }

  private void queueReconnect(MemcachedNode qa, ReconnDelay type, String cause) {
    if (!shutDown) {
      getLogger().warn("Closing, and reopening %s, attempt %d.", qa,
              qa.getReconnectCount());
      if (qa.getSk() != null) {
        qa.getSk().cancel();
        assert !qa.getSk().isValid() : "Cancelled selection key is valid";
      }
      qa.reconnecting();
      try {
        if (qa.getChannel() != null && qa.getChannel().socket() != null) {
          qa.getChannel().socket().close();
        } else {
          getLogger().info("The channel or socket was null for %s",
                  qa);
        }
      } catch (IOException e) {
        getLogger().warn("IOException trying to close a socket", e);
      }
      qa.setChannel(null);

      long delay;
      switch (type) {
        case IMMEDIATE:
          delay = 0;
          break;
        case DEFAULT:
        default:
          delay = (long) Math.min(maxDelay,
                  Math.pow(2, qa.getReconnectCount())) * 1000;
          break;
      }
      long reconTime = System.currentTimeMillis() + delay;

      // Avoid potential condition where two connections are scheduled
      // for reconnect at the exact same time.  This is expected to be
      // a rare situation.
      while (reconnectQueue.containsKey(reconTime)) {
        reconTime++;
      }

      reconnectQueue.put(reconTime, qa);

      // Need to do a little queue management.
      qa.setupResend(failureMode == FailureMode.Cancel && type == ReconnDelay.DEFAULT, cause);

      if (type == ReconnDelay.DEFAULT) {
        if (failureMode == FailureMode.Redistribute) {
          redistributeOperations(qa.destroyInputQueue(), cause);
        } else if (failureMode == FailureMode.Cancel) {
          cancelOperations(qa.destroyInputQueue(), cause);
        }
      }
    }
  }

  private void cancelOperations(Collection<Operation> ops, String cause) {
    for (Operation op : ops) {
      op.cancel(cause);
    }
  }

  private void redistributeOperations(Collection<Operation> ops, String cause) {
    for (Operation op : ops) {
      if (op instanceof KeyedOperation) {
        KeyedOperation ko = (KeyedOperation) op;
        int added = 0;
        for (String k : ko.getKeys()) {
          for (Operation newop : opFact.clone(ko)) {
            addOperation(k, newop);
            added++;
          }
        }
        assert added > 0
                : "Didn't add any new operations when redistributing";
      } else {
        // Cancel things that don't have definite targets.
        op.cancel(cause);
      }
    }
  }

  private void attemptReconnects() throws IOException {
    final long now = System.currentTimeMillis();
    final Map<MemcachedNode, Boolean> seen =
            new IdentityHashMap<MemcachedNode, Boolean>();
    final List<MemcachedNode> rereQueue = new ArrayList<MemcachedNode>();
    SocketChannel ch = null;
    for (Iterator<MemcachedNode> i =
         reconnectQueue.headMap(now).values().iterator(); i.hasNext(); ) {
      final MemcachedNode qa = i.next();
      i.remove();
      try {
        if (!seen.containsKey(qa)) {
          seen.put(qa, Boolean.TRUE);
          getLogger().info("Reconnecting %s", qa);
          ch = SocketChannel.open();
          ch.configureBlocking(false);
          ch.socket().setTcpNoDelay(!f.useNagleAlgorithm());
          ch.socket().setReuseAddress(true);
          int ops = 0;
          if (ch.connect(qa.getSocketAddress())) {
            getLogger().info("Immediately reconnected to %s", qa);
            assert ch.isConnected();
          } else {
            ops = SelectionKey.OP_CONNECT;
          }
          qa.registerChannel(ch, ch.register(selector, ops, qa));
          assert qa.getChannel() == ch : "Channel was lost.";
        } else {
          getLogger().debug(
                  "Skipping duplicate reconnect request for %s", qa);
        }
      } catch (SocketException e) {
        getLogger().warn("Error on reconnect", e);
        rereQueue.add(qa);
      } catch (Exception e) {
        getLogger().error("Exception on reconnect, lost node %s", qa, e);
      } finally {
        //it's possible that above code will leak file descriptors under abnormal
        //conditions (when ch.open() fails and throws IOException.
        //always close non connected channel
        if (ch != null && !ch.isConnected()
                && !ch.isConnectionPending()) {
          try {
            ch.close();
          } catch (IOException x) {
            getLogger().error("Exception closing channel: %s", qa, x);
          }
        }
      }
    }
    // Requeue any fast-failed connects.
    for (MemcachedNode n : rereQueue) {
      queueReconnect(n, ReconnDelay.DEFAULT, "error on reconnect");
    }
  }

  /**
   * Get the node locator used by this connection.
   */
  NodeLocator getLocator() {
    return locator;
  }

  Selector getSelector() {
    return selector;
  }

  /**
   * Add an operation to the given connection.
   *
   * @param key the key the operation is operating upon
   * @param o   the operation
   */
  public void addOperation(final String key, final Operation o) {
    MemcachedNode placeIn = null;
    /* ENABLE_REPLICATION if */
    MemcachedNode primary;
    if (this.arcusReplEnabled) {
      primary = ((ArcusReplKetamaNodeLocator) locator).getPrimary(key, getReplicaPick(o));
    } else
      primary = locator.getPrimary(key);
    /* ENABLE_REPLICATION else */
    /*
    MemcachedNode primary = locator.getPrimary(key);
    /*
    /* ENABLE_REPLICATION end */
    if (primary.isActive() || failureMode == FailureMode.Retry) {
      placeIn = primary;
    } else if (failureMode == FailureMode.Cancel) {
      o.setHandlingNode(primary);
      o.cancel("inactive node");
    } else {
      // Look for another node in sequence that is ready.
      /* ENABLE_REPLICATION if */
      Iterator<MemcachedNode> iter = this.arcusReplEnabled
              ? ((ArcusReplKetamaNodeLocator) locator).getSequence(key, getReplicaPick(o))
              : locator.getSequence(key);
      for (; placeIn == null && iter.hasNext(); ) {
        MemcachedNode n = iter.next();
        if (n.isActive()) {
          placeIn = n;
        }
      }
      /* ENABLE_REPLICATION else */
      /*
      for(Iterator<MemcachedNode> i=locator.getSequence(key);
        placeIn == null && i.hasNext(); ) {
        MemcachedNode n=i.next();
        if(n.isActive()) {
          placeIn=n;
        }
      }
      */
      /* ENABLE_REPLICATION end */
      // If we didn't find an active node, queue it in the primary node
      // and wait for it to come back online.
      if (placeIn == null) {
        placeIn = primary;
      }
    }

    assert o.isCancelled() || placeIn != null
            : "No node found for key " + key;
    if (placeIn != null) {
      addOperation(placeIn, o);
    } else {
      assert o.isCancelled() : "No not found for "
              + key + " (and not immediately cancelled)";
    }
  }

  /* ENABLE_REPLICATION if */
  private ReplicaPick getReplicaPick(final Operation o) {
    ReplicaPick pick = ReplicaPick.MASTER;

    if (o.isReadOperation()) {
      ReadPriority readPriority = f.getAPIReadPriority().get(o.getAPIType());
      if (readPriority != null) {
        if (readPriority == ReadPriority.SLAVE)
          pick = ReplicaPick.SLAVE;
        else if (readPriority == ReadPriority.RR)
          pick = ReplicaPick.RR;
      } else {
        pick = getReplicaPick();
      }
    }

    return pick;
  }

  public ReplicaPick getReplicaPick() {
    ReadPriority readPriority = f.getReadPriority();
    ReplicaPick pick = ReplicaPick.MASTER;

    if (readPriority == ReadPriority.SLAVE)
      pick = ReplicaPick.SLAVE;
    else if (readPriority == ReadPriority.RR)
      pick = ReplicaPick.RR;
    return pick;
  }
  /* ENABLE_REPLICATION end */

  public void insertOperation(final MemcachedNode node, final Operation o) {
    o.setHandlingNode(node);
    o.initialize();
    node.insertOp(o);
    addedQueue.offer(node);
    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
    getLogger().debug("Added %s to %s", o, node);
  }

  public void addOperation(final MemcachedNode node, final Operation o) {
    o.setHandlingNode(node);
    o.initialize();
    node.addOp(o);
    addedQueue.offer(node);
    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
    getLogger().debug("Added %s to %s", o, node);
  }

  public void addOperations(final Map<MemcachedNode, Operation> ops) {

    for (Map.Entry<MemcachedNode, Operation> me : ops.entrySet()) {
      final MemcachedNode node = me.getKey();
      Operation o = me.getValue();
      o.setHandlingNode(node);
      o.initialize();
      node.addOp(o);
      addedQueue.offer(node);
    }
    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
  }

  /**
   * Broadcast an operation to all nodes.
   */
  public CountDownLatch broadcastOperation(BroadcastOpFactory of) {
    return broadcastOperation(of, locator.getAll());
  }

  /**
   * Broadcast an operation to a specific collection of nodes.
   */
  public CountDownLatch broadcastOperation(final BroadcastOpFactory of,
                                           Collection<MemcachedNode> nodes) {
    final CountDownLatch latch = new CountDownLatch(locator.getAll().size());
    for (MemcachedNode node : nodes) {
      Operation op = of.newOp(node, latch);
      op.initialize();
      node.addOp(op);
      op.setHandlingNode(node);
      addedQueue.offer(node);
    }
    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
    return latch;
  }

  /**
   * Shut down all of the connections.
   */
  public void shutdown() throws IOException {
    shutDown = true;
    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
    for (MemcachedNode qa : locator.getAll()) {
      qa.shutdown();
    }
    selector.close();
    getLogger().debug("Shut down selector %s", selector);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{MemcachedConnection to");
    for (MemcachedNode qa : locator.getAll()) {
      sb.append(" ");
      sb.append(qa.getSocketAddress());
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * helper method: increase timeout count on node attached to this op
   *
   * @param op
   */
  public static void opTimedOut(Operation op) {
    MemcachedConnection.setTimeout(op, true);
  }

  /**
   * helper method: reset timeout counter
   *
   * @param op
   */
  public static void opSucceeded(Operation op) {
    MemcachedConnection.setTimeout(op, false);
  }

  /**
   * helper method: do some error checking and set timeout boolean
   *
   * @param op
   * @param isTimeout
   */
  private static void setTimeout(Operation op, boolean isTimeout) {
    try {
      if (op == null) {
        LoggerFactory.getLogger(MemcachedConnection.class).debug("op is null.");
        return; // op may be null in some cases, e.g. flush
      }
      MemcachedNode node = op.getHandlingNode();
      if (node == null) {
        LoggerFactory.getLogger(MemcachedConnection.class).debug("handling node for operation is not set");
      } else {
        if (isTimeout || !op.isCancelled())
          node.setContinuousTimeout(isTimeout);
      }
    } catch (Exception e) {
      LoggerFactory.getLogger(MemcachedConnection.class).error(e.getMessage());
    }
  }

  /**
   * find memcachednode for key
   *
   * @param key
   * @return a memcached node
   */
  public MemcachedNode findNodeByKey(String key) {
    MemcachedNode placeIn = null;
    /* ENABLE_REPLICATION if */
    MemcachedNode primary = null;
    if (this.arcusReplEnabled) {
      /* just used for arrange key */
      primary = ((ArcusReplKetamaNodeLocator) locator).getPrimary(key, getReplicaPick());
    } else
      primary = locator.getPrimary(key);
    /* ENABLE_REPLICATION else */
    /*
    MemcachedNode primary = locator.getPrimary(key);
    /*

    /* ENABLE_REPLICATION end */
    // FIXME.  Support other FailureMode's.  See MemcachedConnection.addOperation.
    if (primary.isActive() || failureMode == FailureMode.Retry) {
      placeIn = primary;
    } else {
      /* ENABLE_REPLICATION if */
      Iterator<MemcachedNode> iter = this.arcusReplEnabled
              ? ((ArcusReplKetamaNodeLocator) locator).getSequence(key, getReplicaPick())
              : locator.getSequence(key);
      for (; placeIn == null && iter.hasNext(); ) {
        MemcachedNode n = iter.next();
        if (n.isActive()) {
          placeIn = n;
        }
      }
      /* ENABLE_REPLICATION else */
      /*
      for (Iterator<MemcachedNode> i = locator.getSequence(key); placeIn == null
          && i.hasNext();) {
        MemcachedNode n = i.next();
        if (n.isActive()) {
          placeIn = n;
        }
      }
      */
      /* ENABLE_REPLICATION end */
      if (placeIn == null) {
        placeIn = primary;
      }
    }
    return placeIn;
  }

  public int getAddedQueueSize() {
    return addedQueue.size();
  }
  /* ENABLE_REPLICATION if */

  private interface Task {
    void doTask();
  }

  private class SetupResendTask implements Task {
    private MemcachedNode node;
    private boolean cancelWrite;
    private String cause;

    public SetupResendTask(MemcachedNode node, boolean cancelWrite, String cause) {
      this.node = node;
      this.cancelWrite = cancelWrite;
      this.cause = cause;
    }

    public void doTask() {
      node.setupResend(cancelWrite, cause);
    }
  }

  private class QueueReconnectTask implements Task {
    private MemcachedNode node;
    private ReconnDelay delay;
    private String cause;

    public QueueReconnectTask(MemcachedNode node, ReconnDelay delay, String cause) {
      this.node = node;
      this.delay = delay;
      this.cause = cause;
    }

    public void doTask() {
      queueReconnect(node, delay, cause);
    }
  }

  private class MoveOperationTask implements Task {
    private MemcachedNode fromNode;
    private MemcachedNode toNode;

    public MoveOperationTask(MemcachedNode from, MemcachedNode to) {
      fromNode = from;
      toNode = to;
    }

    public void doTask() {
      if (fromNode.moveOperations(toNode) > 0)
        addedQueue.offer(toNode);
    }
  }
  /* ENABLE_REPLICATION end */
}
