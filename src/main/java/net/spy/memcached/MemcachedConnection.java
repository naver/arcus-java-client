/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
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
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.internal.ReconnDelay;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.MultiOperationCallback;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;

/**
 * Connection to a cluster of memcached servers.
 */
public final class MemcachedConnection extends SpyObject {

  private int emptySelects = 0;
  // The number of empty selects we'll allow before assuming we may have
  // missed one and should check the current selectors.  This generally
  // indicates a bug, but we'll check it nonetheless.
  private static final int DOUBLE_CHECK_EMPTY = 256;
  // The number of empty selects we'll allow before blowing up.  It's too
  // easy to write a bug that causes it to loop uncontrollably.  This helps
  // find those bugs and often works around them.
  private static final int EXCESSIVE_EMPTY = 0x1000000;

  private final int timeoutExceptionThreshold;
  private final int timeoutRatioThreshold;
  private final int timeoutDurationThreshold;

  private final String connName;
  private Selector selector = null;
  private final NodeLocator locator;
  private final FailureMode failureMode;
  // If true, optimization will collapse multiple sequential get ops
  private final boolean optimizeGetOp;

  // AddedQueue is used to track the QueueAttachments for which operations
  // have recently been queued.
  private final ConcurrentLinkedQueue<MemcachedNode> addedQueue;
  // reconnectQueue contains the attachments that need to be reconnected
  private final ReconnectQueue reconnectQueue;
  private final AtomicReference<List<InetSocketAddress>> cacheNodesChange
          = new AtomicReference<>(null);
  /* ENABLE_MIGRATION if */
  private final AtomicReference<List<InetSocketAddress>> alterNodesChange
          = new AtomicReference<>(null);
  private final AtomicReference<List<InetSocketAddress>> delayedAlterNodesChange
          = new AtomicReference<>(null);
  /* ENABLE_MIGRATION end */

  private final OperationFactory opFactory;
  private final ConnectionFactory connFactory;
  private final Collection<ConnectionObserver> connObservers =
          new ConcurrentLinkedQueue<>();
  private final Set<MemcachedNode> nodesNeedVersionOp = new HashSet<>();

  /* ENABLE_MIGRATION if */
  private boolean arcusMigrEnabled = false;
  private MigrationType mgType = MigrationType.UNKNOWN;
  private MigrationState mgState = MigrationState.UNKNOWN;
  private boolean mgInProgress = false;
  /* ENABLE_MIGRATION end */

  /* ENABLE_REPLICATION if */
  private static final long DELAYED_SWITCHOVER_TIMEOUT_MILLISECONDS = 50;
  private boolean arcusReplEnabled;
  private final DelayedSwitchoverGroups delayedSwitchoverGroups =
      new DelayedSwitchoverGroups(DELAYED_SWITCHOVER_TIMEOUT_MILLISECONDS);
  /* ENABLE_REPLICATION end */

  /**
   * Construct a memcached connection.
   *
   * @param name      the name of memcached connection
   * @param f         the factory that will provide an operation queue
   * @param a         the addresses of the servers to connect to
   * @param obs       the observers that see the first connection established.
   * @param fm        the failure mode for the underlying connection :
   *                  Cancel(default), Redistribute, Retry.
   * @param opfactory the operation factory.
   * @throws IOException if a connection attempt fails early
   */
  public MemcachedConnection(String name, ConnectionFactory f,
                             List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
                             FailureMode fm, OperationFactory opfactory)
          throws IOException {
    this.connFactory = f;
    connName = name;
    connObservers.addAll(obs);
    addedQueue = new ConcurrentLinkedQueue<>();
    failureMode = fm;
    optimizeGetOp = f.shouldOptimize();
    opFactory = opfactory;
    timeoutExceptionThreshold = f.getTimeoutExceptionThreshold();
    timeoutRatioThreshold = f.getTimeoutRatioThreshold();
    timeoutDurationThreshold = f.getTimeoutDurationThreshold();
    selector = Selector.open();
    List<MemcachedNode> connections = new ArrayList<>(a.size());
    for (SocketAddress sa : a) {
      connections.add(makeMemcachedNode(connName, sa));
    }
    locator = f.createLocator(connections);
    reconnectQueue = new ReconnectQueue(f.getMaxReconnectDelay());
  }

  /* ENABLE_REPLICATION if */
  // handleNodesChangeQueue and updateConnections behave slightly differently
  // depending on the Arcus version.  We could have created a subclass and overload
  // those methods.  But, MemcachedConnection is a final class.
  void setArcusReplEnabled(boolean b) {
    arcusReplEnabled = b;
  }

  boolean getArcusReplEnabled() {
    return arcusReplEnabled;
  }
  /* ENABLE_REPLICATION end */

  /* ENABLE_MIGRATION if */
  void setArcusMigrEnabled(boolean b) {
    arcusMigrEnabled = b;
  }

  void setMigrationTypeAndState(MigrationType type, MigrationState state) {
    if (state != this.mgState && state == MigrationState.PREPARED) {
      this.mgInProgress = false;
    }
    this.mgType = type;
    this.mgState = state;
  }
  /* ENABLE_MIGRATION end */

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
                  : "Not connected, and not watching for connect: " + sops;
        }
      }
    }
    getLogger().debug("Checked the selectors.");
    return true;
  }

  private void addVersionOpToVersionAbsentNodes() {
    Iterator<MemcachedNode> it = nodesNeedVersionOp.iterator();
    while (it.hasNext()) {
      MemcachedNode qa = it.next();
      try {
        prepareVersionInfo(qa);
      } catch (IllegalStateException e) {
        // queue overflow occurs. retry later
        continue;
      }
      it.remove();
    }
  }

  /**
   * MemcachedClient calls this method to handle IO over the connections.
   */
  public void handleIO() throws IOException {
    // add versionOp to the node that need it.
    addVersionOpToVersionAbsentNodes();

    // Deal with all of the stuff that's been added, but may not be marked writable.
    handleInputQueue();
    getLogger().debug("Done dealing with queue.");

    long delay = 0;
    if (cacheNodesChange.get() != null) {
      delay = 1;
    } else if (!reconnectQueue.isEmpty()) {
      delay = reconnectQueue.getMinDelayMillis();
    }
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled && !delayedSwitchoverGroups.isEmpty()) {
      long minSwitchoverDelay = delayedSwitchoverGroups.getMinDelayMillis();
      delay = (delay > 0) ? Math.min(minSwitchoverDelay, delay) : minSwitchoverDelay;
    }
    /* ENABLE_REPLICATION end */
    getLogger().debug("Selecting with delay of %sms", delay);
    assert selectorsMakeSense() : "Selectors don't make sense.";
    int selected = selector.select(delay);
    Set<SelectionKey> selectedKeys = selector.selectedKeys();

    if (selectedKeys.isEmpty()) {
      getLogger().debug("No selectors ready, interrupted: " + Thread.interrupted());
      if (++emptySelects > DOUBLE_CHECK_EMPTY) {
        getLogger().info(
            "Reached to the double check of emptySelect. Selected with delay of %dms", delay);
        for (SelectionKey sk : selector.keys()) {
          getLogger().info("%s has %s, interested in %s",
                  sk, sk.readyOps(), sk.interestOps());
          if (sk.readyOps() != 0) {
            getLogger().info("%s has a ready op, handling IO", sk);
            handleIO(sk);
          } else {
            lostConnection((MemcachedNode) sk.attachment(),
                ReconnDelay.DEFAULT, "too many empty selects");
          }
        }
        assert emptySelects < EXCESSIVE_EMPTY : "Too many empty selects";
      }
    } else {
      getLogger().debug("Selected %d, selected %d keys", selected, selectedKeys.size());
      emptySelects = 0;

      for (SelectionKey sk : selectedKeys) {
        handleIO(sk);
      }
      selectedKeys.clear();
    }

    // see if any connections blew up with large number of timeouts
    for (SelectionKey sk : selector.keys()) {
      Object attachment = sk.attachment();
      // attachment might be null, because some node has already closed the channel to reconnect.
      if (attachment == null) {
        continue;
      }
      MemcachedNode mn = (MemcachedNode) attachment;
      if (mn.getContinuousTimeout() > timeoutExceptionThreshold &&
          (timeoutDurationThreshold == 0 || mn.getTimeoutDuration() > timeoutDurationThreshold)) {
        getLogger().warn(
            "%s exceeded continuous timeout threshold. >%s(count), >%s(duration) (%s)",
            mn.getNodeName(),
            timeoutExceptionThreshold, timeoutDurationThreshold, mn.getOpQueueStatus());
        lostConnection(mn, ReconnDelay.DEFAULT, "continuous timeout");
      } else if (timeoutRatioThreshold > 0 && mn.getTimeoutRatioNow() > timeoutRatioThreshold) {
        getLogger().warn("%s exceeded timeout ratio threshold. >%s (%s)",
                mn.getNodeName(), timeoutRatioThreshold, mn.getOpQueueStatus());
        lostConnection(mn, ReconnDelay.DEFAULT, "high timeout ratio");
      }
    }

    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      // Deal with the memcached server group that need delayed switchover.
      handleDelayedSwitchover();
    }
    /* ENABLE_REPLICATION end */

    // Deal with the memcached server group that's been added by CacheManager.
    handleCacheNodesChange();

    if (!reconnectQueue.isEmpty()) {
      attemptReconnects();
    }
  }

  private void handleNodesToRemove(final List<MemcachedNode> nodesToRemove) {
    for (MemcachedNode node : nodesToRemove) {
      getLogger().info("old memcached node removed %s", node);
      reconnectQueue.remove(node);

      /* ENABLE_MIGRATION if */
      if (mgType == MigrationType.LEAVE) {
        if (node.hasReadOp()) {
          redistributeOperationsForMigration(node.destroyReadQueue(false));
        }
        if (node.hasWriteOp()) {
          redistributeOperationsForMigration(node.destroyWriteQueue(false));
        }
        redistributeOperationsForMigration(node.destroyInputQueue());
        continue;
      }
      /* ENABLE_MIGRATION end */

      // removing node is not related to failure mode.
      // so, cancel operations regardless of failure mode.
      String cause = "node removed.";
      cancelOperations(node.destroyReadQueue(false), cause);
      cancelOperations(node.destroyWriteQueue(false), cause);
      cancelOperations(node.destroyInputQueue(), cause);
    }
  }

  private void updateConnections(List<InetSocketAddress> addrs) throws IOException {
    List<MemcachedNode> attachNodes = new ArrayList<>();
    List<MemcachedNode> removeNodes = new ArrayList<>();

    for (MemcachedNode node : locator.getAll()) {
      if (addrs.contains(node.getSocketAddress())) {
        addrs.remove(node.getSocketAddress());
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

    // Remove the unavailable nodes.
    handleNodesToRemove(removeNodes);
  }

  /* ENABLE_REPLICATION if */
  private Set<String> findChangedGroups(List<InetSocketAddress> addrs,
                                        Collection<MemcachedNode> nodes) {
    Map<String, InetSocketAddress> addrMap = new HashMap<>();
    for (InetSocketAddress each : addrs) {
      addrMap.put(each.toString(), each);
    }

    Set<String> changedGroupSet = new HashSet<>();
    for (MemcachedNode node : nodes) {
      String nodeAddr = ((InetSocketAddress) node.getSocketAddress()).toString();
      if (addrMap.remove(nodeAddr) == null) { // removed node
        changedGroupSet.add(node.getReplicaGroup().getGroupName());
      }
    }
    for (String addr : addrMap.keySet()) { // newly added node
      ArcusReplNodeAddress a = (ArcusReplNodeAddress) addrMap.get(addr);
      changedGroupSet.add(a.getGroupName());
    }
    return changedGroupSet;
  }

  private List<InetSocketAddress> findAddrsOfChangedGroups(List<InetSocketAddress> addrs,
                                                           Set<String> changedGroups) {
    List<InetSocketAddress> changedGroupAddrs = new ArrayList<>();
    for (InetSocketAddress addr : addrs) {
      if (changedGroups.contains(((ArcusReplNodeAddress) addr).getGroupName())) {
        changedGroupAddrs.add(addr);
      }
    }
    return changedGroupAddrs;
  }

  private void updateReplConnections(List<InetSocketAddress> addrs) throws IOException {
    List<MemcachedNode> attachNodes = new ArrayList<>();
    List<MemcachedNode> removeNodes = new ArrayList<>();
    List<MemcachedReplicaGroup> changeRoleGroups = new ArrayList<>();
    List<Task> taskList = new ArrayList<>(); // tasks executed after locator update

    /* In replication, after SWITCHOVER or REPL_SLAVE is received from a group
     * and switchover is performed, but before the group's znode is changed,
     * another group's znode can be changed.
     *
     * In this case, there is a problem that the switchover is restored
     * because the state of the switchover group and the znode state are different.
     *
     * In order to remove the abnormal phenomenon,
     * we find out the changed groups with the comparison of previous and current znode list,
     * and update the state of groups based on them.
     */
    Set<String> changedGroups = findChangedGroups(addrs, locator.getAll());

    Map<String, List<ArcusReplNodeAddress>> newAllGroups =
            ArcusReplNodeAddress.makeGroupAddrsList(findAddrsOfChangedGroups(addrs, changedGroups));

    // remove invalidated groups in changedGroups
    for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllGroups.entrySet()) {
      if (!ArcusReplNodeAddress.validateGroup(entry)) {
        changedGroups.remove(entry.getKey());
      }
    }

    Map<String, MemcachedReplicaGroup> oldAllGroups =
            ((ArcusReplKetamaNodeLocator) locator).getAllGroups();

    for (String changedGroupName : changedGroups) {
      MemcachedReplicaGroup oldGroup = oldAllGroups.get(changedGroupName);
      List<ArcusReplNodeAddress> newGroupAddrs = newAllGroups.get(changedGroupName);

      if (oldGroup == null) {
        // Newly added group
        for (ArcusReplNodeAddress newAddr : newGroupAddrs) {
          attachNodes.add(attachMemcachedNode(newAddr));
        }
        continue;
      }

      if (newGroupAddrs == null) {
        // Old group nodes have disappeared. Remove the old group nodes.
        removeNodes.add(oldGroup.getMasterNode());
        removeNodes.addAll(oldGroup.getSlaveNodes());
        delayedSwitchoverGroups.remove(oldGroup);
        continue;
      }

      if (oldGroup.isDelayedSwitchover()) {
        delayedSwitchoverGroups.remove(oldGroup);
        switchoverMemcachedReplGroup(oldGroup, true);
      }

      MemcachedNode oldMasterNode = oldGroup.getMasterNode();
      List<MemcachedNode> oldSlaveNodes = oldGroup.getSlaveNodes();

      getLogger().debug("New group nodes : " + newGroupAddrs);
      getLogger().debug("Old group nodes : [" + oldGroup + "]");

      ArcusReplNodeAddress oldMasterAddr = (ArcusReplNodeAddress) oldMasterNode.getSocketAddress();
      ArcusReplNodeAddress newMasterAddr = newGroupAddrs.get(0);
      assert oldMasterAddr != null : "invalid old rgroup";
      assert newMasterAddr != null : "invalid new rgroup";

      Set<ArcusReplNodeAddress> oldSlaveAddrs = getAddrsFromNodes(oldSlaveNodes);
      Set<ArcusReplNodeAddress> newSlaveAddrs = getSlaveAddrsFromGroupAddrs(newGroupAddrs);

      if (oldMasterAddr.isSameAddress(newMasterAddr)) {
        // add newly added slave node
        for (ArcusReplNodeAddress newSlaveAddr : newSlaveAddrs) {
          if (!oldSlaveAddrs.contains(newSlaveAddr)) {
            attachNodes.add(attachMemcachedNode(newSlaveAddr));
          }
        }

        // remove not exist old slave node
        for (MemcachedNode oldSlaveNode : oldSlaveNodes) {
          if (!newSlaveAddrs.contains((ArcusReplNodeAddress) oldSlaveNode.getSocketAddress())) {
            removeNodes.add(oldSlaveNode);
            // move operation slave -> master.
            taskList.add(new MoveOperationTask(
                oldSlaveNode, oldMasterNode, false));
            // clear the masterCandidate if the removed slave is the masterCandidate.
            if (oldGroup.getMasterCandidate() == oldSlaveNode) {
              oldGroup.clearMasterCandidate();
            }
          }
        }
      } else if (oldSlaveAddrs.contains(newMasterAddr)) {
        if (newSlaveAddrs.contains(oldMasterAddr)) {
          // Switchover
          MemcachedNode oldMasterCandidate = oldGroup.getMasterCandidate();
          if (oldMasterCandidate != null) {
            if (!newMasterAddr.isSameAddress(
                    ((ArcusReplNodeAddress) oldMasterCandidate.getSocketAddress()))) {
              /**
               * Moves ops from oldMasterCandidate set by cache server to newMasterCandidate.
               * Handling the below case.
               * old group : [oldMaster, oldSlave1, oldSlave2]
               * old group after switchover response :
               * [oldMaster, oldSlave1-masterCandidate, oldSlave2]
               * new group from zk cache list: [slave1, X, newMaster]
               */
              oldGroup.setMasterCandidateByAddr(newMasterAddr.getIPPort());
              taskList.add(new MoveOperationTask(
                      oldMasterCandidate, oldGroup.getMasterCandidate(), false));
            }
            changeRoleGroups.add(oldGroup);
          } else {
            // ZK event occurs before cache server response.
            oldGroup.setMasterCandidateByAddr(newMasterAddr.getIPPort());
            if (oldMasterNode.hasNonIdempotentOperationInReadQ()) {
              // delay to change role and move operations
              // by the time switchover timeout occurs or
              // "SWITCHOVER", "REPL_SLAVE" response received.
              delayedSwitchoverGroups.put(oldGroup);
            } else {
              changeRoleGroups.add(oldGroup);
              taskList.add(new MoveOperationTask(
                      oldMasterNode, oldGroup.getMasterCandidate(), false));
              taskList.add(new QueueReconnectTask(
                      oldMasterNode, ReconnDelay.IMMEDIATE,
                      "Discarded all pending reading state operation to move operations."));
            }
          }
        } else {
          oldGroup.setMasterCandidateByAddr(newMasterAddr.getIPPort());
          changeRoleGroups.add(oldGroup);
          // Failover
          removeNodes.add(oldMasterNode);
          // move operation: master -> slave.
          taskList.add(new MoveOperationTask(
              oldMasterNode, oldGroup.getMasterCandidate(), true));
        }

        // add newly added slave node
        for (ArcusReplNodeAddress newSlaveAddr : newSlaveAddrs) {
          if (!oldSlaveAddrs.contains(newSlaveAddr) && !oldMasterAddr.isSameAddress(newSlaveAddr)) {
            attachNodes.add(attachMemcachedNode(newSlaveAddr));
          }
        }
        // remove not exist old slave node
        for (MemcachedNode oldSlaveNode : oldSlaveNodes) {
          ArcusReplNodeAddress oldSlaveAddr
                  = (ArcusReplNodeAddress) oldSlaveNode.getSocketAddress();
          if (!newSlaveAddrs.contains(oldSlaveAddr) && !newMasterAddr.isSameAddress(oldSlaveAddr)) {
            removeNodes.add(oldSlaveNode);
            // move operation slave -> master.
            taskList.add(new MoveOperationTask(
                oldSlaveNode, oldGroup.getMasterCandidate(), false));
          }
        }
      } else {
        // Old master has gone away. And, new group has appeared.
        MemcachedNode newMasterNode = attachMemcachedNode(newMasterAddr);
        attachNodes.add(newMasterNode);
        for (ArcusReplNodeAddress newSlaveAddr : newSlaveAddrs) {
          attachNodes.add(attachMemcachedNode(newSlaveAddr));
        }
        removeNodes.add(oldMasterNode);
        // move operation: master -> master.
        taskList.add(new MoveOperationTask(
            oldMasterNode, newMasterNode, true));
        for (MemcachedNode oldSlaveNode : oldSlaveNodes) {
          removeNodes.add(oldSlaveNode);
          // move operation slave -> master.
          taskList.add(new MoveOperationTask(
              oldSlaveNode, newMasterNode, false));
          // clear the masterCandidate if the removed slave is the masterCandidate.
          if (oldGroup.getMasterCandidate() == oldSlaveNode) {
            oldGroup.clearMasterCandidate();
          }
        }
      }
    }
    // Update the hash.
    ((ArcusReplKetamaNodeLocator) locator).update(attachNodes, removeNodes, changeRoleGroups);

    // do task after locator update
    for (Task task : taskList) {
      task.doTask();
    }

    // Remove the unavailable nodes.
    handleNodesToRemove(removeNodes);
  }

  private Set<ArcusReplNodeAddress> getAddrsFromNodes(List<MemcachedNode> nodes) {
    Set<ArcusReplNodeAddress> addrs = Collections.emptySet();
    if (!nodes.isEmpty()) {
      addrs = new HashSet<>((int) (nodes.size() / .75f) + 1);
      for (MemcachedNode node : nodes) {
        addrs.add((ArcusReplNodeAddress) node.getSocketAddress());
      }
    }
    return addrs;
  }

  private Set<ArcusReplNodeAddress> getSlaveAddrsFromGroupAddrs(
          List<ArcusReplNodeAddress> groupAddrs) {
    Set<ArcusReplNodeAddress> slaveAddrs = Collections.emptySet();
    int groupSize = groupAddrs.size();
    if (groupSize > 1) {
      slaveAddrs = new HashSet<>((int) ((groupSize - 1) / .75f) + 1);
      for (int i = 1; i < groupSize; i++) {
        slaveAddrs.add(groupAddrs.get(i));
      }
    }
    return slaveAddrs;
  }
  /* ENABLE_REPLICATION end */

  /* ENABLE_REPLICATION if */
  private void switchoverMemcachedReplGroup(MemcachedReplicaGroup group,
                                            boolean cancelNonIdempotent) {

    if (group.getMasterCandidate() == null) {
      getLogger().warn("Delay switchover because invalid group state : " + group);
      return;
    }

    MemcachedNode oldMaster = group.getMasterNode();

    /*  must keep the following execution order when switchover
     * - first moveOperations
     * - second, queueReconnect
     *
     * because moves all operations
     */
    ((ArcusReplKetamaNodeLocator) locator).switchoverReplGroup(group);
    MemcachedNode newMaster = group.getMasterNode();

    oldMaster.moveOperations(newMaster, cancelNonIdempotent);
    addedQueue.offer(newMaster);
    queueReconnect(oldMaster, ReconnDelay.IMMEDIATE,
        "Discarded all pending reading state operation to move operations.");
  }
  /* ENABLE_REPLICATION end */

  private MemcachedNode attachMemcachedNode(SocketAddress sa) throws IOException {
    /* ENABLE_MIGRATION if */
    if (mgType == MigrationType.JOIN) {
      /* Only joining nodes can be attached */
      MemcachedNode node = locator.getAlterNode(sa);
      if (node != null) {
        return node;
      }
    }
    /* ENABLE_MIGRATION end */
    return makeMemcachedNode(connName, sa);
  }

  private MemcachedNode makeMemcachedNode(String name,
                                          SocketAddress sa) throws IOException {
    MemcachedNode qa = connFactory.createMemcachedNode(name, sa, connFactory.getReadBufSize());
    if (timeoutRatioThreshold > 0) {
      qa.enableTimeoutRatio();
    }

    SocketChannel ch = SocketChannel.open();
    ch.configureBlocking(false);
    ch.socket().setTcpNoDelay(!connFactory.useNagleAlgorithm());
    ch.socket().setKeepAlive(connFactory.getKeepAlive());
    ch.socket().setReuseAddress(true);
    /* The codes above can be replaced by the codes below since java 1.7 */
    // ch.setOption(StandardSocketOptions.TCP_NODELAY, !f.useNagleAlgorithm());
    // ch.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    qa.setChannel(ch);
    int ops = 0;
    // Initially I had attempted to skirt this by queueing every
    // connect, but it considerably slowed down start time.
    try {
      if (ch.connect(sa)) {
        getLogger().info("new memcached node connected to %s immediately", qa);
        connected(qa);
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
    prepareVersionInfo(qa);
    return qa;
  }

  private void prepareVersionInfo(final MemcachedNode node) {
    Operation op = opFactory.version(new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (status.isSuccess()) {
          node.setVersion(status.getMessage());
        } else {
          getLogger().warn("VersionOp failed : " + status.getMessage());
        }
      }

      @Override
      public void complete() {
        if (node.getVersion() == null) {
          nodesNeedVersionOp.add(node);
        }
      }
    });
    addOperation(node, op);
  }

  // Handle the memcached server group that's been added by CacheManager.
  void handleCacheNodesChange() throws IOException {
    /* ENABLE_MIGRATION if */
    /*
     * handleCacheNodesChange() and handleAlterNodesChange() have been integrated
     * to fix bug that occurs when Context Switching from Java Client IO Thread to ZK IO Thread
     * is executed after handleCacheNodesChange() and before handleAlterNodesChange().
     * If change of cache_list and alter_list are received when after handleCacheNodesChange()
     * and before handleAlterNodesChange(), there MUST be bug because alter_list change
     * will be applied without application of dependent cache_list change.
     */
    List<InetSocketAddress> alterList = alterNodesChange.getAndSet(null);
    /* ENABLE_MIGRATION end */
    List<InetSocketAddress> cacheList = cacheNodesChange.getAndSet(null);
    if (cacheList != null) {
      // Update the memcached server group.
      /* ENABLE_REPLICATION if */
      if (arcusReplEnabled) {
        updateReplConnections(cacheList);
        return;
      }
      /* ENABLE_REPLICATION end */
      updateConnections(cacheList);
    }
    /* ENABLE_MIGRATION if */
    if (arcusMigrEnabled && alterList != null) {
      if (mgState == MigrationState.PREPARED) {
        if (!mgInProgress) {
          // prepare connections of alter nodes
          prepareAlterConnections(alterList);
        } else {
          // check joining node down
          updateAlterConnections(alterList);
        }
      }
      if (alterList.isEmpty()) { // end of migration
        mgState = MigrationState.DONE;
        mgInProgress = false;
      }
    }
    /* ENABLE_MIGRATION end */
  }

  // Called by CacheManger to add the memcached server group.
  public void setCacheNodesChange(List<InetSocketAddress> addrs) {
    List<InetSocketAddress> old = cacheNodesChange.getAndSet(addrs);
    if (old != null) {
      getLogger().info("Ignored previous cache nodes change.");
    }
    /* ENABLE_MIGRATION if */
    old = delayedAlterNodesChange.getAndSet(null);
    if (old != null) {
      alterNodesChange.set(old);
    }
    /* ENABLE_MIGRATION end */
    selector.wakeup();
  }

  /* ENABLE_MIGRATION if */
  /* Called by CacheManger to add the alter memcached server group. */
  public void setAlterNodesChange(List<InetSocketAddress> addrs, boolean readingCacheList) {
    if (readingCacheList) {
      List<InetSocketAddress> old = delayedAlterNodesChange.getAndSet(addrs);
      if (old != null) {
        getLogger().info("Ignored previous delayed alter nodes change.");
      }
    } else {
      List<InetSocketAddress> old = alterNodesChange.getAndSet(addrs);
      if (old != null) {
        getLogger().info("Ignored previous alter nodes change.");
      }
      delayedAlterNodesChange.set(null);
      selector.wakeup();
    }
  }

  public void prepareAlterConnections(List<InetSocketAddress> addrs) throws IOException {
    getLogger().info("Prepare connection of alter nodes. addrs=" + addrs);
    List<MemcachedNode> alterNodes = new ArrayList<>();
    if (mgType == MigrationType.JOIN) {
      for (SocketAddress sa : addrs) {
        alterNodes.add(makeMemcachedNode(connName, sa));
      }
    } else { // MigrationType.LEAVE
      for (MemcachedNode node : locator.getAll()) {
        if (addrs.contains(node.getSocketAddress())) {
          alterNodes.add(node);
        }
      }
    }
    locator.prepareMigration(alterNodes, mgType);
    mgInProgress = true;
  }

  private void updateAlterConnections(List<InetSocketAddress> addrs) throws IOException {
    List<MemcachedNode> attachNodes = new ArrayList<>();
    List<MemcachedNode> removeNodes = new ArrayList<>();

    for (MemcachedNode node : locator.getAlterAll()) {
      if (addrs.contains(node.getSocketAddress())) {
        addrs.remove(node.getSocketAddress());
      } else {
        if (mgType == MigrationType.JOIN) {
          removeNodes.add(node);
        }
      }
    }
    if (mgType == MigrationType.JOIN) {
      for (MemcachedNode node : locator.getAll()) {
        if (addrs.contains(node.getSocketAddress())) {
          addrs.remove(node.getSocketAddress());
        }
      }
    }

    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      // Make connections to the newly added alter nodes with slave role.
      for (SocketAddress sa : addrs) {
        attachNodes.add(makeMemcachedNode(connName, sa));
        getLogger().info("Address of new alter node to attach: %s.", sa.toString());
      }
    }
    /* ENABLE_REPLICATION end */

    // Update the hash.
    locator.updateAlter(attachNodes, removeNodes);

    // Remove the unavailable nodes.
    handleNodesToRemove(removeNodes);
  }
  /* ENABLE_MIGRATION end */

  // Handle the memcached server group that need delayed switchover.
  private void handleDelayedSwitchover() {
    if (!delayedSwitchoverGroups.isEmpty()) {
      delayedSwitchoverGroups.switchover();
    }
  }

  // Handle any requests that have been made against the client.
  private void handleInputQueue() {
    if (!addedQueue.isEmpty()) {
      getLogger().debug("Handling queue");
      // If there's stuff in the added queue.  Try to process it.
      Collection<MemcachedNode> toAdd = new HashSet<>();
      // Transfer the queue into a hashset.  There are very likely more
      // additions than there are nodes.
      Collection<MemcachedNode> todo = new HashSet<>();

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
              handleWrites(qa);
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
        getLogger().info("Connection state changed for %s", qa);
        final SocketChannel channel = qa.getChannel();
        if (channel.finishConnect()) {
          connected(qa);
          addedQueue.offer(qa);
          if (qa.getWbuf().hasRemaining()) {
            handleWrites(qa);
          }
        } else {
          assert !channel.isConnected() : "connected";
        }
      } else {
        if (sk.isValid() && sk.isReadable()) {
          handleReads(qa);
        }
        if (sk.isValid() && sk.isWritable()) {
          handleWrites(qa);
        }
      }
    } catch (ClosedChannelException e) {
      // Note, not all channel closes end up here
      getLogger().warn("Closed channel.  "
              + "Queueing reconnect on %s", qa, e);
      lostConnection(qa, ReconnDelay.DEFAULT, "closed channel");
    } catch (ConnectException e) {
      // Failures to establish a connection should attempt a reconnect
      // without signaling the observers.
      getLogger().warn("Reconnecting due to failure to connect to %s", qa, e);
      queueReconnect(qa, ReconnDelay.DEFAULT, "failure to connect");
    } catch (OperationException e) {
      qa.setupForAuth("operation exception"); // noop if !shouldAuth
      getLogger().warn("Reconnection due to exception " +
              "handling a memcached exception on %s.", qa, e);
      lostConnection(qa, ReconnDelay.IMMEDIATE, "operation exception");
    } catch (Exception e) {
      // Any particular error processing an item should simply
      // cause us to reconnect to the server.
      //
      // One cause is just network oddness or servers
      // restarting, which lead here with IOException

      qa.setupForAuth("due to exception"); // noop if !shouldAuth
      getLogger().warn("Reconnecting due to exception on %s", qa, e);
      lostConnection(qa, ReconnDelay.DEFAULT, e.getMessage());
    }
    qa.fixupOps();
  }

  private void handleWrites(MemcachedNode qa)
      throws IOException {
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      if (qa.getReplicaGroup().isDelayedSwitchover()) {
        return;
      }
    }
    /* ENABLE_REPLICATION end */
    qa.fillWriteBuffer(optimizeGetOp);
    boolean canWriteMore = qa.getBytesRemainingToWrite() > 0;
    while (canWriteMore) {
      int wrote = qa.writeSome();
      qa.fillWriteBuffer(optimizeGetOp);
      canWriteMore = wrote > 0 && qa.getBytesRemainingToWrite() > 0;
    }
  }

  private void handleReads(MemcachedNode qa)
          throws IOException {
    Operation currentOp = qa.getCurrentReadOp();
    ByteBuffer rbuf = qa.getRbuf();
    final SocketChannel channel = qa.getChannel();
    int read = channel.read(rbuf);
    while (read > 0) {
      getLogger().debug("Read %d bytes", read);
      ((Buffer) rbuf).flip();
      while (rbuf.remaining() > 0) {
        if (currentOp == null) {
          throw new IllegalStateException("No read operation.");
        }
        currentOp.readFromBuffer(rbuf);
        if (currentOp.getState() == OperationState.COMPLETE) {
          getLogger().debug("Completed read op: %s and giving the next %d bytes",
                  currentOp, rbuf.remaining());
          Operation op = qa.removeCurrentReadOp();
          assert op == currentOp : "Expected to pop " + currentOp + " got " + op;
          currentOp = qa.getCurrentReadOp();
        /* ENABLE_REPLICATION if */
        } else if (currentOp.getState() == OperationState.NEED_SWITCHOVER) {
          break;
        /* ENABLE_REPLICATION end */
        /* ENABLE_MIGRATION if */
        } else if (currentOp.getState() == OperationState.REDIRECT) {
          Operation op = qa.removeCurrentReadOp();
          assert op == currentOp : "Expected to pop " + currentOp + " got " + op;
          if (currentOp == qa.getCurrentWriteOp()) { // partially written
            qa.removeCurrentWriteOp();
          }
          redirectOperation(currentOp);
          currentOp = qa.getCurrentReadOp();
        }
        /* ENABLE_MIGRATION end */
      }
      /* ENABLE_REPLICATION if */
      if (currentOp != null && currentOp.getState() == OperationState.NEED_SWITCHOVER) {
        ((Buffer) rbuf).clear();
        MemcachedReplicaGroup group = qa.getReplicaGroup();
        if (group.isDelayedSwitchover()) {
          delayedSwitchoverGroups.remove(group);
          switchoverMemcachedReplGroup(group, false);
        } else {
          MemcachedNode masterCandidate = group.getMasterCandidate();
          group.getMasterNode().moveOperations(masterCandidate, false);
          addedQueue.offer(masterCandidate);
          queueReconnect(group.getMasterNode(), ReconnDelay.IMMEDIATE,
                  "Discarded all pending reading state operation to move operations.");
        }
        break;
      }
      /* ENABLE_REPLICATION end */
      ((Buffer) rbuf).clear();
      read = channel.read(rbuf);
    }
    if (read < 0) {
      // our model is to keep the connection alive for future ops
      // so we'll queue a reconnect if disconnected via an IOException
      throw new IOException("Disconnected unexpected, will reconnect.");
    }
    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      if (currentOp == null) { // readQ is empty
        MemcachedReplicaGroup group = qa.getReplicaGroup();
        if (group.isDelayedSwitchover() && group.getMasterNode() == qa) {
          delayedSwitchoverGroups.remove(group);
          switchoverMemcachedReplGroup(group, false);
        }
      }
    }
    /* ENABLE_REPLICATION end */
  }

  /* ENABLE_MIGRATION if */
  /* There is a possibility that NOT_MY_KEY will be received
   * regardless of migration in the future. (to solve old hashring problem by zookeeper disconnect)
   */
  private void redirectOperation(Operation op) {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Redirect Operation. op=" + op);
    }
    // Get RedirectHandler
    RedirectHandler rh = op.getAndClearRedirectHandler();
    if (rh == null) {
      // Probably code bug
      op.cancel("Redirect failure. RedirectHandler is not registered.");
      return;
    }

    // Hashring update by migration
    locator.updateMigration(rh.getMigrationBasePoint(), rh.getMigrationEndPoint());

    // Redirect operation
    boolean success;
    if (rh instanceof RedirectHandler.RedirectHandlerSingleKey) {
      success = redirectSingleKeyOperation((RedirectHandler.RedirectHandlerSingleKey) rh, op);
    } else {
      success = redirectMultiKeyOperation((RedirectHandler.RedirectHandlerMultiKey) rh, op);
    }
    if (success) {
      Selector s = selector.wakeup();
      assert s == selector : "Wakeup returned the wrong selector.";
    }
  }

  private boolean redirectSingleKeyOperation(
      RedirectHandler.RedirectHandlerSingleKey redirectHandler,
      Operation op) {

    String owner = redirectHandler.getOwner();
    if (owner != null) {
      return redirectSingleKeyOperation(findNodeByOwner(owner), op);
    } else { // single key pipe operation
      return redirectSingleKeyOperation(redirectHandler.getKey(), op); // hashring lookup
    }
  }

  private boolean redirectSingleKeyOperation(String key, Operation op) {
    return redirectSingleKeyOperation(findNodeByKey(key), op);
  }

  private boolean redirectSingleKeyOperation(MemcachedNode node, Operation op) {
    if (node == null) {
      op.cancel("Redirect failure. No node.");
      return false;
    }
    if (!node.isActive() && !node.isFirstConnecting()) {
      op.cancel("Redirect failure. Inactive node.");
      return false;
    }
    node.addOpToWriteQ(op);
    addedQueue.offer(node);
    return true;
  }

  private boolean redirectMultiKeyOperation(
      RedirectHandler.RedirectHandlerMultiKey redirectHandler,
      Operation op) {

    Map<MemcachedNode, List<String>> keysByNode = redirectHandler.groupRedirectKeys(this);
    return redirectMultiKeyOperation(keysByNode, op);
  }

  private boolean redirectMultiKeyOperation(Map<MemcachedNode, List<String>> keysByNode,
                                            Operation op) {

    if (keysByNode == null || keysByNode.isEmpty()) {
      op.cancel("Redirect failure. No keysByNode.");
      return false;
    }

    MemcachedNode node = null;
    Map<MemcachedNode, Operation> ops = new HashMap<>();
    MultiOperationCallback mcb = opFactory.createMultiOperationCallback(
        (KeyedOperation) op, keysByNode.size());

    for (Map.Entry<MemcachedNode, List<String>> entry : keysByNode.entrySet()) {
      node = entry.getKey();
      Operation clonedOp = opFactory.cloneMultiOperation(
          (KeyedOperation) op, node, entry.getValue(), mcb
      );
      if (node == null) {
        clonedOp.cancel("Redirect failure. No node.");
        continue;
      }
      if (!node.isActive() && !node.isFirstConnecting()) {
        clonedOp.cancel("Redirect failure. Inactive node.");
        continue;
      }
      ops.put(node, clonedOp);
    }
    if (ops.isEmpty()) {
      return false;
    }
    for (Map.Entry<MemcachedNode, Operation> entry : ops.entrySet()) {
      node = entry.getKey();
      node.addOpToWriteQ(entry.getValue());
      addedQueue.offer(node);
    }
    return true;
  }

  public MemcachedNode findNodeByOwner(String owner) {
    return locator.getOwnerNode(owner, mgType);
  }
  /* ENABLE_MIGRATION end */

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
    if (reconnectQueue.contains(qa)) {
      reconnectQueue.replace(qa, type);
      return;
    }

    /* ENABLE_REPLICATION if */
    if (arcusReplEnabled) {
      MemcachedReplicaGroup group = qa.getReplicaGroup();
      if (group.isDelayedSwitchover() && group.getMasterNode() == qa) {
        delayedSwitchoverGroups.remove(group);
        switchoverMemcachedReplGroup(group, true);
        return;
      }
    }
    /* ENABLE_REPLICATION end */

    getLogger().warn("Closing, and reopening %s, attempt %d.", qa,
            qa.getReconnectCount());
    try {
      qa.closeChannel();
    } catch (IOException e) {
      getLogger().warn("IOException trying to close a socket", e);
    }
    qa.reconnecting();

    // Need to do a little queue management.
    qa.setupResend(cause);

    if (type == ReconnDelay.DEFAULT) {
      if (failureMode == FailureMode.Redistribute) {
        redistributeOperations(qa.destroyWriteQueue(true), cause);
        redistributeOperations(qa.destroyInputQueue(), cause);
      } else if (failureMode == FailureMode.Cancel) {
        cancelOperations(qa.destroyWriteQueue(false), cause);
        cancelOperations(qa.destroyInputQueue(), cause);
      }
    }

    reconnectQueue.add(qa, type);
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
          for (Operation newop : opFactory.clone(ko)) {
            addOperation(k, newop);
            added++;
          }
        }
        assert added > 0 : "Didn't add any new operations when redistributing";
      } else {
        // Cancel things that don't have definite targets.
        op.cancel(cause);
      }
    }
  }

  /* ENABLE_MIGRATION if */
  private void redistributeOperationsForMigration(Collection<Operation> ops) {
    boolean success = false;
    for (Operation op : ops) {
      if (op instanceof KeyedOperation) {
        KeyedOperation ko = (KeyedOperation) op;
        Collection<String> keys = ko.getKeys();

        if (keys.size() == 1) {
          String key = keys.toArray()[0].toString();
          success = success || redirectSingleKeyOperation(key, op);
        } else {
          Map<MemcachedNode, List<String>> nodeByKeys = groupKeysByNode(keys);
          success = success || redirectMultiKeyOperation(nodeByKeys, op);
        }
      } else {
        op.cancel("by redistribution.");
      }
    }
    if (success) {
      Selector s = selector.wakeup();
      assert s == selector : "Wakeup returned the wrong selector.";
    }
  }

  public Map<MemcachedNode, List<String>> groupKeysByNode(Collection<String> keys) {
    Map<MemcachedNode, List<String>> keysByNode = new HashMap<>();
    for (String key : keys) {
      MemcachedNode node = findNodeByKey(key);
      if (node == null) {
        return null;
      }
      List<String> k = keysByNode.get(node);
      if (k == null) {
        k = new ArrayList<>();
        keysByNode.put(node, k);
      }
      k.add(key);
    }
    return keysByNode;
  }
  /* ENABLE_MIGRATION end */

  private void attemptReconnects() {
    final List<MemcachedNode> rereQueue = new ArrayList<>();
    final long nanoTime = System.nanoTime();
    SocketChannel ch = null;
    MemcachedNode node = reconnectQueue.popReady(nanoTime);
    while (node != null) {
      if (node.getChannel() != null) {
        // Below the code cannot be executed.
        // Because the reconnect queue are not allowed to add the same node.
        // But if this logger is called, there is a bug in reconnect queue.
        getLogger().warn(
            "Skipping reconnect request that already reconnected to %s", node);
        continue;
      }
      try {
        getLogger().info("Reconnecting %s", node);
        ch = SocketChannel.open();
        ch.configureBlocking(false);
        ch.socket().setTcpNoDelay(!connFactory.useNagleAlgorithm());
        ch.socket().setKeepAlive(connFactory.getKeepAlive());
        ch.socket().setReuseAddress(true);
        /* The codes above can be replaced by the codes below since java 1.7 */
        // ch.setOption(StandardSocketOptions.TCP_NODELAY, !f.useNagleAlgorithm());
        // ch.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        node.setChannel(ch);
        int ops = 0;
        if (ch.connect(node.getSocketAddress())) {
          getLogger().info("Immediately reconnected to %s", node);
          connected(node);
          addedQueue.offer(node);
        } else {
          ops = SelectionKey.OP_CONNECT;
        }
        node.setSk(ch.register(selector, ops, node));
        assert node.getChannel() == ch : "Channel was lost.";
      } catch (SocketException e) {
        getLogger().warn("Error on reconnect", e);
        rereQueue.add(node);
      } catch (Exception e) {
        getLogger().error("Exception on reconnect, lost node %s", node, e);
      } finally {
        //it's possible that above code will leak file descriptors under abnormal
        //conditions (when ch.open() fails and throws IOException.
        //always close non connected channel
        if (ch != null && !ch.isConnected() && !ch.isConnectionPending()) {
          try {
            ch.close();
          } catch (IOException x) {
            getLogger().error("Exception closing channel: %s", node, x);
          }
        }
      }
      node = reconnectQueue.popReady(nanoTime);
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

  /* ENABLE_REPLICATION if */
  private ReplicaPick getReplicaPick(final Operation o) {
    ReplicaPick pick = ReplicaPick.MASTER;

    if (o.isReadOperation()) {
      ReadPriority readPriority = connFactory.getAPIReadPriority().get(o.getAPIType());
      if (readPriority != null) {
        if (readPriority == ReadPriority.SLAVE) {
          pick = ReplicaPick.SLAVE;
        } else if (readPriority == ReadPriority.RR) {
          pick = ReplicaPick.RR;
        }
      } else {
        pick = getReplicaPick();
      }
    }
    return pick;
  }

  private ReplicaPick getReplicaPick() {
    ReadPriority readPriority = connFactory.getReadPriority();
    ReplicaPick pick = ReplicaPick.MASTER;

    if (readPriority == ReadPriority.SLAVE) {
      pick = ReplicaPick.SLAVE;
    } else if (readPriority == ReadPriority.RR) {
      pick = ReplicaPick.RR;
    }
    return pick;
  }
  /* ENABLE_REPLICATION end */

  /**
   * Get the primary node for the key string.
   *
   * @param key the key the operation is operating upon
   */
  public MemcachedNode getPrimaryNode(final String key) {
    /* ENABLE_REPLICATION if */
    if (this.arcusReplEnabled) {
      return ((ArcusReplKetamaNodeLocator) locator).getPrimary(key, getReplicaPick());
    }
    /* ENABLE_REPLICATION end */
    return locator.getPrimary(key);
  }

  /**
   * Get the primary node for the key string and the operation.
   *
   * @param key the key the operation is operating upon
   * @param o   the operation
   */
  public MemcachedNode getPrimaryNode(final String key, final Operation o) {
    /* ENABLE_REPLICATION if */
    if (this.arcusReplEnabled) {
      return ((ArcusReplKetamaNodeLocator) locator).getPrimary(key, getReplicaPick(o));
    }
    /* ENABLE_REPLICATION end */
    return locator.getPrimary(key);
  }

  /**
   * Get the another node sequence for the key string.
   *
   * @param key the key the operation is operating upon
   */
  public Iterator<MemcachedNode> getNodeSequence(final String key) {
    /* ENABLE_REPLICATION if */
    if (this.arcusReplEnabled) {
      return ((ArcusReplKetamaNodeLocator) locator).getSequence(key, getReplicaPick());
    }
    /* ENABLE_REPLICATION end */
    return locator.getSequence(key);
  }

  /**
   * Get the another node sequence for the key string and the operation.
   *
   * @param key the key the operation is operating upon
   * @param o   the operation
   */
  public Iterator<MemcachedNode> getNodeSequence(final String key, final Operation o) {
    /* ENABLE_REPLICATION if */
    if (this.arcusReplEnabled) {
      return ((ArcusReplKetamaNodeLocator) locator).getSequence(key, getReplicaPick(o));
    }
    /* ENABLE_REPLICATION end */
    return locator.getSequence(key);
  }

  /**
   * Add an operation to the given connection.
   *
   * @param key the key the operation is operating upon
   * @param o   the operation
   */
  public void addOperation(final String key, final Operation o) {
    addOperation(findNodeByKey(key), o);
  }

  public void insertOperation(final MemcachedNode node, final Operation o) {
    node.insertOp(o);
    addedQueue.offer(node);
    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
    getLogger().debug("Added %s to %s", o, node);
  }

  public void addOperation(final MemcachedNode node, final Operation o) {
    if (node == null) {
      o.cancel("no node");
      return;
    }
    if ((!node.isActive() && !node.isFirstConnecting()) &&
        failureMode == FailureMode.Cancel) {
      o.setHandlingNode(node);
      o.cancel("inactive node");
      return;
    }
    node.addOpToInputQ(o);
    addedQueue.offer(node);
    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
    getLogger().debug("Added %s to %s", o, node);
  }

  public void addOperations(final Map<MemcachedNode, Operation> ops) {
    for (Map.Entry<MemcachedNode, Operation> me : ops.entrySet()) {
      addOperation(me.getKey(), me.getValue());
    }
  }

  public void wakeUpSelector() {
    if (selector != null) {
      selector.wakeup();
    }
  }

  /**
   * Shut down all the connections.
   */
  public void shutdown() throws IOException {
    for (MemcachedNode qa : locator.getAll()) {
      try {
        qa.shutdown();
      } catch (IOException e) {
        getLogger().error("Exception closing channel: %s", qa, e);
      }
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
   * helper method: increase timeout count on a distinct attached node to this op
   *
   * @param ops
   */
  public static void opsTimedOut(Collection<Operation> ops) {
    Collection<String> timedOutNodes = new HashSet<>();
    for (Operation op : ops) {
      try {
        MemcachedNode node = op.getHandlingNode();
        if (node == null) {
          continue;
        }
        // set timeout only once for the same node.
        String key = node.getSocketAddress().toString();
        if (!timedOutNodes.contains(key)) {
          timedOutNodes.add(key);
          node.setContinuousTimeout(true);
        }
      } catch (Exception e) {
        LoggerFactory.getLogger(MemcachedConnection.class).error(e.getMessage());
      }
    }
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
   * helper method: reset timeout counter
   *
   * @param ops
   */
  public static void opsSucceeded(Collection<Operation> ops) {
    for (Operation op : ops) {
      MemcachedConnection.setTimeout(op, false);
    }
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
        LoggerFactory.getLogger(MemcachedConnection.class).debug(
            "handling node for operation is not set");
      } else {
        if (isTimeout || !op.isCancelled()) {
          node.setContinuousTimeout(isTimeout);
        }
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
    MemcachedNode node = getPrimaryNode(key);
    if (node == null) {
      return null;
    }
    if (node.isActive() || node.isFirstConnecting()) {
      return node;
    }
    if (failureMode == FailureMode.Redistribute) {
      Iterator<MemcachedNode> iter = getNodeSequence(key);
      while (iter.hasNext()) {
        MemcachedNode n = iter.next();
        if (n != null && n.isActive()) {
          node = n;
          break;
        }
      }
    }
    return node;
  }

  public int getAddedQueueSize() {
    return addedQueue.size();
  }

  public static class ReconnectQueue {
    // maximum amount of time to wait between reconnect attempts
    private final long maxReconnectDelaySeconds;

    public ReconnectQueue(long maxReconnectDelaySeconds) {
      this.maxReconnectDelaySeconds = maxReconnectDelaySeconds;
    }

    private final Map<MemcachedNode, Long/*reconnect nano time*/> reconMap =
            new HashMap<>();
    private final NavigableMap<Long/*reconnect nano time*/, MemcachedNode> reconSortedMap =
            new TreeMap<>();

    private long newReconnectNanoTime(MemcachedNode node, ReconnDelay type) {
      long newReconTime = System.nanoTime();
      if (type == ReconnDelay.DEFAULT) {
        newReconTime += TimeUnit.SECONDS.toNanos(
            (long) Math.min(maxReconnectDelaySeconds,
                Math.pow(2, node.getReconnectCount() + 1)));
      }
      // Avoid potential condition where two connections are scheduled
      // for reconnect at the exact same time.  This is expected to be
      // a rare situation.
      while (reconSortedMap.containsKey(newReconTime)) {
        newReconTime++;
      }
      return newReconTime;
    }

    public boolean contains(MemcachedNode node) {
      return reconMap.containsKey(node);
    }

    public void replace(MemcachedNode node, ReconnDelay type) {
      long oldReconTime = reconMap.get(node);
      long newReconTime = newReconnectNanoTime(node, type);
      if (newReconTime < oldReconTime) {
        reconSortedMap.remove(oldReconTime);
        reconMap.put(node, newReconTime);
        reconSortedMap.put(newReconTime, node);
      }
    }

    public void add(MemcachedNode node, ReconnDelay type) {
      Long reconTime = newReconnectNanoTime(node, type);
      reconMap.put(node, reconTime);
      reconSortedMap.put(reconTime, node);
    }

    public void remove(MemcachedNode node) {
      Long reconTime = reconMap.remove(node);
      if (reconTime != null) {
        reconSortedMap.remove(reconTime);
      }
    }

    public MemcachedNode popReady(long nanoTime) {
      Entry<Long, MemcachedNode> entry = reconSortedMap.firstEntry();
      if (entry == null || nanoTime < entry.getKey()) {
        return null;
      }
      remove(entry.getValue());
      return entry.getValue();
    }

    public boolean isEmpty() {
      return reconMap.isEmpty();
    }

    public long getMinDelayMillis() {
      if (isEmpty()) {
        return 0;
      }
      return Math.max(
          TimeUnit.MILLISECONDS.convert(
              reconSortedMap.firstKey() - System.nanoTime(),
              TimeUnit.NANOSECONDS),
          1);
    }
  }

  /* ENABLE_REPLICATION if */
  private interface Task {
    void doTask();
  }

  private class QueueReconnectTask implements Task {
    private final MemcachedNode node;
    private final ReconnDelay delay;
    private final String cause;

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
    private final MemcachedNode from;
    private final MemcachedNode to;
    private final boolean cancelNonIdempotent;

    public MoveOperationTask(MemcachedNode from, MemcachedNode to, boolean cancelNonIdempotent) {
      this.from = from;
      this.to = to;
      this.cancelNonIdempotent = cancelNonIdempotent;
    }

    public void doTask() {
      if (from.moveOperations(to, cancelNonIdempotent) > 0) {
        addedQueue.offer(to);
      }
    }
  }

  private class DelayedSwitchoverGroups {
    private final long delayedSwitchoverTimeoutNanos;
    private final SortedMap<Long/*switchover nano time*/, MemcachedReplicaGroup> groups =
            new TreeMap<>();

    public DelayedSwitchoverGroups(long delayedSwitchoverTimeoutMillis) {
      this.delayedSwitchoverTimeoutNanos = TimeUnit.NANOSECONDS.convert(
          delayedSwitchoverTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    public void put(MemcachedReplicaGroup group) {
      if (group.isDelayedSwitchover()) {
        return;
      }
      long delay = System.nanoTime() + delayedSwitchoverTimeoutNanos;
      while (groups.containsKey(delay)) {
        delay++;
      }
      groups.put(delay, group);
      group.setDelayedSwitchover(true);
    }

    public void remove(MemcachedReplicaGroup group) {
      if (!group.isDelayedSwitchover()) {
        return;
      }
      group.setDelayedSwitchover(false);
      Iterator<MemcachedReplicaGroup> iterator = groups.values().iterator();
      while (iterator.hasNext()) {
        if (iterator.next() == group) {
          iterator.remove();
          return;
        }
      }
    }

    public boolean isEmpty() {
      return groups.isEmpty();
    }

    public long getMinDelayMillis() {
      if (groups.isEmpty()) {
        return 0;
      }
      return Math.max(
          TimeUnit.MILLISECONDS.convert(
              groups.firstKey() - System.nanoTime(),
              TimeUnit.NANOSECONDS),
          1);
    }

    public void switchover() {
      long now = System.nanoTime();
      Iterator<Entry<Long, MemcachedReplicaGroup>> iterator = groups.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<Long, MemcachedReplicaGroup> entry = iterator.next();
        long switchoverTime = entry.getKey();
        MemcachedReplicaGroup group = entry.getValue();

        if (now < switchoverTime) {
          return;
        } else {
          iterator.remove();
          group.setDelayedSwitchover(false);
          switchoverMemcachedReplGroup(group, true);
        }
      }
    }
  }
  /* ENABLE_REPLICATION end */
}
