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
/* ENABLE_REPLICATION if */
package net.spy.memcached;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.ArcusReplKetamaNodeLocatorConfiguration;

public class ArcusReplKetamaNodeLocator extends SpyObject implements NodeLocator {

  private final TreeMap<Long, SortedSet<MemcachedReplicaGroup>> ketamaGroups;
  private final HashMap<String, MemcachedReplicaGroup> allGroups;
  private final Collection<MemcachedNode> allNodes;

  /* ENABLE_MIGRATION if */
  private TreeMap<Long, SortedSet<MemcachedReplicaGroup>> ketamaAlterGroups;
  private HashMap<String, MemcachedReplicaGroup> alterGroups;
  private HashMap<String, MemcachedReplicaGroup> existGroups;
  private HashSet<MemcachedNode> alterNodes;

  private MigrationType migrationType;
  private Long migrationBasePoint;
  private Long migrationLastPoint;
  private boolean migrationInProgress;
  /* ENABLE_MIGRATION end */

  private final Collection<MemcachedReplicaGroup> toDeleteGroups;
  private final HashAlgorithm hashAlg = HashAlgorithm.KETAMA_HASH;
  private final ArcusReplKetamaNodeLocatorConfiguration config
          = new ArcusReplKetamaNodeLocatorConfiguration();

  private final Lock lock = new ReentrantLock();

  public ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes) {
    super();
    allNodes = nodes;
    ketamaGroups = new TreeMap<Long, SortedSet<MemcachedReplicaGroup>>();
    allGroups = new HashMap<String, MemcachedReplicaGroup>();

    /* ENABLE_MIGRATION if */
    alterNodes = new HashSet<MemcachedNode>();
    ketamaAlterGroups = new TreeMap<Long, SortedSet<MemcachedReplicaGroup>>();
    alterGroups = new HashMap<String, MemcachedReplicaGroup>();
    existGroups = new HashMap<String, MemcachedReplicaGroup>();
    clearMigration();
    /* ENABLE_MIGRATION end */

    // create all memcached replica group
    for (MemcachedNode node : nodes) {
      MemcachedReplicaGroup mrg =
              allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
      if (mrg == null) {
        mrg = new MemcachedReplicaGroupImpl(node);
        getLogger().info("new memcached replica group added %s", mrg.getGroupName());
        allGroups.put(mrg.getGroupName(), mrg);
      } else {
        mrg.setMemcachedNode(node);
      }
    }

    int numReps = config.getNodeRepetitions();
    // Ketama does some special work with md5 where it reuses chunks.
    for (MemcachedReplicaGroup group : allGroups.values()) {
      insertHash(group);
    }
    /* ketamaNodes.size() < numReps*nodes.size() : hash collision */
    assert ketamaGroups.size() <= (numReps * allGroups.size());

    // prepare toDeleteGroups
    toDeleteGroups = new HashSet<MemcachedReplicaGroup>();
  }

  private ArcusReplKetamaNodeLocator(TreeMap<Long, SortedSet<MemcachedReplicaGroup>> kg,
                                     HashMap<String, MemcachedReplicaGroup> ag,
                                     Collection<MemcachedNode> an) {
    super();
    ketamaGroups = kg;
    allGroups = ag;
    allNodes = an;
    toDeleteGroups = new HashSet<MemcachedReplicaGroup>();

    /* ENABLE_MIGRATION if */
    alterNodes = new HashSet<MemcachedNode>();
    ketamaAlterGroups = new TreeMap<Long, SortedSet<MemcachedReplicaGroup>>();
    alterGroups = new HashMap<String, MemcachedReplicaGroup>();
    existGroups = new HashMap<String, MemcachedReplicaGroup>();
    clearMigration();
    /* ENABLE_MIGRATION end */
  }

  public Collection<MemcachedNode> getAll() {
    return Collections.unmodifiableCollection(allNodes);
  }

  public Map<String, MemcachedReplicaGroup> getAllGroups() {
    return Collections.unmodifiableMap(allGroups);
  }

  /* ENABLE_MIGRATION if */
  public Collection<MemcachedNode> getAlterAll() {
    return Collections.unmodifiableCollection(alterNodes);
  }

  public MemcachedNode getAlterNode(SocketAddress sa) {
    /* The alter node to attach should be found */
    for (MemcachedNode node : alterNodes) {
      if (sa.equals(node.getSocketAddress())) {
        return node;
      }
    }
    /* If a slave node has started during migration, it may not exist */ 
    return null;
  }
  /* ENABLE_MIGRATION end */

  public Collection<MemcachedNode> getMasterNodes() {
    List<MemcachedNode> masterNodes = new ArrayList<MemcachedNode>(allGroups.size());

    for (MemcachedReplicaGroup g : allGroups.values()) {
      masterNodes.add(g.getMasterNode());
    }
    return masterNodes;
  }

  public MemcachedNode getPrimary(final String k) {
    return getNodeForKey(hashAlg.hash(k), ReplicaPick.MASTER);
  }

  public MemcachedNode getPrimary(final String k, ReplicaPick pick) {
    return getNodeForKey(hashAlg.hash(k), pick);
  }

  public long getMaxKey() {
    return ketamaGroups.lastKey();
  }

  private MemcachedNode getNodeForKey(long hash, ReplicaPick pick) {
    MemcachedReplicaGroup rg;
    MemcachedNode rv = null;

    lock.lock();
    try {
      if (!ketamaGroups.isEmpty()) {
        if (!ketamaGroups.containsKey(hash)) {
          Long nodeHash = ketamaGroups.ceilingKey(hash);
          if (nodeHash == null) {
            hash = ketamaGroups.firstKey();
          } else {
            hash = nodeHash;
          }
        }
        rg = ketamaGroups.get(hash).first();
        // return a node (master / slave) for the replica pick request.
        rv = rg.getNodeByReplicaPick(pick);
      }
    } finally {
      lock.unlock();
    }
    return rv;
  }

  public Iterator<MemcachedNode> getSequence(String k) {
    return new ReplKetamaIterator(k, ReplicaPick.MASTER, allGroups.size());
  }

  public Iterator<MemcachedNode> getSequence(String k, ReplicaPick pick) {
    return new ReplKetamaIterator(k, pick, allGroups.size());
  }

  public NodeLocator getReadonlyCopy() {
    TreeMap<Long, SortedSet<MemcachedReplicaGroup>> smg =
            new TreeMap<Long, SortedSet<MemcachedReplicaGroup>>(ketamaGroups);
    HashMap<String, MemcachedReplicaGroup> ag =
            new HashMap<String, MemcachedReplicaGroup>(allGroups.size());
    Collection<MemcachedNode> an = new ArrayList<MemcachedNode>(allNodes.size());

    lock.lock();
    try {
      // Rewrite the values a copy of the map
      for (Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> mge : smg.entrySet()) {
        SortedSet<MemcachedReplicaGroup> groupROSet =
            new TreeSet<MemcachedReplicaGroup>(
                new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
        for (MemcachedReplicaGroup mrg : mge.getValue()) {
          groupROSet.add(new MemcachedReplicaGroupROImpl(mrg));
        }
        mge.setValue(groupROSet);
      }
      // copy the allGroups collection.
      for (Map.Entry<String, MemcachedReplicaGroup> me : allGroups.entrySet()) {
        ag.put(me.getKey(), new MemcachedReplicaGroupROImpl(me.getValue()));
      }
      // copy the allNodes collection.
      for (MemcachedNode n : allNodes) {
        an.add(new MemcachedNodeROImpl(n));
      }
    } finally {
      lock.unlock();
    }

    return new ArcusReplKetamaNodeLocator(smg, ag, an);
  }

  public void update(Collection<MemcachedNode> toAttach, Collection<MemcachedNode> toDelete) {
    update(toAttach, toDelete, new ArrayList<MemcachedReplicaGroup>(0));
  }

  public void update(Collection<MemcachedNode> toAttach,
                     Collection<MemcachedNode> toDelete,
                     Collection<MemcachedReplicaGroup> changeRoleGroups) {
    /* We must keep the following execution order
     * - first, remove nodes.
     * - second, change role.
     * - third, add nodes
     */
    lock.lock();
    try {
      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        allNodes.remove(node);
        removeNodeFromGroup(node);
        try {
          node.closeChannel();
        } catch (IOException e) {
          getLogger().error("Failed to closeChannel the node : " + node);
        }
      }

      // Change role
      for (MemcachedReplicaGroup g : changeRoleGroups) {
        g.changeRole();
      }

      // Add memcached nodes.
      for (MemcachedNode node : toAttach) {
        allNodes.add(node);
        insertNodeIntoGroup(node);
      }

      // Delete empty group
      for (MemcachedReplicaGroup group : toDeleteGroups) {
        getLogger().info("old memcached replica group removed %s", group.getGroupName());
        allGroups.remove(group.getGroupName());
        removeHash(group);
      }
      toDeleteGroups.clear();
    } finally {
      /* ENABLE_MIGRATION if */
      if (migrationInProgress && alterNodes.isEmpty()) {
        getLogger().info("Migration " + migrationType + " has been finished.");
        clearMigration();
      }
      /* ENABLE_MIGRATION end */
      lock.unlock();
    }
  }

  public void switchoverReplGroup(MemcachedReplicaGroup group) {
    lock.lock();
    group.changeRole();
    lock.unlock();
  }

  private void insertNodeIntoGroup(MemcachedNode node) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      if (alterNodes.contains(node)) {
        alterNodes.remove(node);
        MemcachedReplicaGroup mrg;
        mrg = alterGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
        if (mrg != null) {
          allGroups.put(mrg.getGroupName(), mrg);
          insertHash(mrg);
        }
        return;
      }
      // How to handle the new node ? go downward (FIXME)
    }
    /* ENABLE_MIGRATION end */

    MemcachedReplicaGroup mrg;
    mrg = allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
    if (mrg == null) {
      mrg = new MemcachedReplicaGroupImpl(node);
      getLogger().info("new memcached replica group added %s", mrg.getGroupName());
      allGroups.put(mrg.getGroupName(), mrg);
      insertHash(mrg);
    } else {
      if (mrg.isEmptyGroup()) {
        toDeleteGroups.remove(mrg);
      }
      mrg.setMemcachedNode(node);
    }
  }

  private void removeNodeFromGroup(MemcachedNode node) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      alterNodes.remove(node);
      /* go downward */
    }
    /* ENABLE_MIGRATION end */

    MemcachedReplicaGroup mrg;
    mrg = allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
    mrg.deleteMemcachedNode(node);
    if (mrg.isEmptyGroup()) {
      toDeleteGroups.add(mrg);
    }
  }

  private Long getKetamaHashPoint(byte[] digest, int h) {
    return ((long) (digest[3 + h * 4] & 0xFF) << 24)
         | ((long) (digest[2 + h * 4] & 0xFF) << 16)
         | ((long) (digest[1 + h * 4] & 0xFF) << 8)
         | (digest[h * 4] & 0xFF);
  }

  private void insertHash(MemcachedReplicaGroup group) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      assert migrationType == MigrationType.JOIN;
      alterGroups.remove(group.getGroupName());
      insertHashOfJOIN(group); // joining => joined
      return;
    }
    /* ENABLE_MIGRATION end */

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> nodeSet = ketamaGroups.get(k);
        if (nodeSet == null) {
          nodeSet = new TreeSet<MemcachedReplicaGroup>(
              new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
          ketamaGroups.put(k, nodeSet);
        }
        nodeSet.add(group);
      }
    }
  }

  private void removeHash(MemcachedReplicaGroup group) {
    /* ENABLE_MIGRATION if */
    boolean callAutoLeaveAbort = false;
    if (migrationInProgress) {
      if (alterGroups.remove(group.getGroupName()) != null) {
        /* A leaving group is down or has left */
        assert migrationType == MigrationType.LEAVE;
        removeHashOfAlter(group);
        return;
      }
      if (existGroups.remove(group.getGroupName()) != null) {
        /* An existing group is down */
        if (migrationType == MigrationType.JOIN) {
          automaticJoinCompletion(group);
        } else {
          callAutoLeaveAbort = true;
        }
      } else {
        /* A joined group is down : do nothing */
      }
      /* go downward */
    }
    /* ENABLE_MIGRATION end */

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> nodeSet = ketamaGroups.get(k);
        nodeSet.remove(group);
        if (nodeSet.size() == 0) {
          ketamaGroups.remove(k);
        }
      }
    }

    /* ENABLE_MIGRATION if */
    if (callAutoLeaveAbort) {
      automaticLeaveAbort(group);
    }
    /* ENABLE_MIGRATION end */
  }

  /* ENABLE_MIGRATION if */
  /* Insert the joining hash points into ketamaAlterGroups */
  private void prepareHashOfJOIN(MemcachedReplicaGroup group) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> nodeSet = ketamaAlterGroups.get(k);
        if (nodeSet == null) {
          nodeSet = new TreeSet<MemcachedReplicaGroup>(
              new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
          ketamaAlterGroups.put(k, nodeSet);
        }
        nodeSet.add(group);
      }
    }
  }

  /* Insert the joining hash points into ketamaGroups */
  private void insertHashOfJOIN(MemcachedReplicaGroup group) {
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> alterSet = ketamaAlterGroups.get(k);
        if (alterSet != null && alterSet.remove(group)) {
          if (alterSet.size() == 0) {
            ketamaAlterGroups.remove(k);
          }
          SortedSet<MemcachedReplicaGroup> existSet = ketamaGroups.get(k);
          if (existSet == null) {
            existSet = new TreeSet<MemcachedReplicaGroup>(
                new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
            ketamaGroups.put(k, existSet);
          }
          existSet.add(group); // joining => joined
        } else {
          // The hash point has already been inserted.
        }
      }
    }
  }

  /* Remove all hash points of the alter group */
  private void removeHashOfAlter(MemcachedReplicaGroup group) {
    // The alter hpoints can be in both ketamaAlterGroups and ketamaGroups.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> groupSet = ketamaAlterGroups.get(k);
        if (groupSet != null && groupSet.remove(group)) {
          if (groupSet.isEmpty()) {
            ketamaAlterGroups.remove(k);
          }
        } else {
          groupSet = ketamaGroups.get(k);
          assert groupSet != null;
          boolean removed = groupSet.remove(group);
          if (groupSet.isEmpty()) {
            ketamaGroups.remove(k);
          }
          assert removed;
        }
      }
    }
  }

  /* Move an alter hash range from ketamaAlterNodes to ketamaNodes */
  private void moveAlterHashRangeFromAlterToExist(Long spoint, boolean sInclusive,
                                                  Long epoint, boolean eInclusive) {
    NavigableMap<Long, SortedSet<MemcachedReplicaGroup>> moveRange
        = ketamaAlterGroups.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<Long>();
    for (Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> entry : moveRange.entrySet()) {
      SortedSet<MemcachedReplicaGroup> groupSet = ketamaGroups.get(entry.getKey());
      if (groupSet == null) {
        groupSet = new TreeSet<MemcachedReplicaGroup>(
            new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
        ketamaGroups.put(entry.getKey(), groupSet);
      }
      groupSet.addAll(entry.getValue());
      removeList.add(entry.getKey());
    }
    for (Long key : removeList) {
      ketamaAlterGroups.remove(key);
    }
  }

  /* Move an alter hash range from ketamaNode to ketamaAlterNodes */
  private void moveAlterHashRangeFromExistToAlter(Long spoint, boolean sInclusive,
                                             Long epoint, boolean eInclusive) {
    NavigableMap<Long, SortedSet<MemcachedReplicaGroup>> moveRange
        = ketamaGroups.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<Long>();
    for (Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> entry : moveRange.entrySet()) {
      Iterator<MemcachedReplicaGroup> groupIter = entry.getValue().iterator();
      while (groupIter.hasNext()) {
        MemcachedReplicaGroup group = groupIter.next();
        if (alterGroups.containsKey(group.getGroupName())) {
          groupIter.remove(); // leaving => leaved
          SortedSet<MemcachedReplicaGroup> alterSet = ketamaAlterGroups.get(entry.getKey());
          if (alterSet == null) {
            alterSet = new TreeSet<MemcachedReplicaGroup>(
                new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
            ketamaAlterGroups.put(entry.getKey(), alterSet); // for auto leave abort
          }
          alterSet.add(group);
        }
      }
      if (entry.getValue().isEmpty()) {
        removeList.add(entry.getKey());
      }
    }
    for (Long key : removeList) {
      ketamaGroups.remove(key);
    }
  }

  private void insertAlterHashRange(Long spoint, Long epoint, boolean inclusive) {
    if (spoint < epoint) {
      moveAlterHashRangeFromAlterToExist(spoint, inclusive, epoint, true);
    } else {
      moveAlterHashRangeFromAlterToExist(spoint, inclusive, 0xFFFFFFFFL, true);
      moveAlterHashRangeFromAlterToExist(0L, true, epoint, true);
    }
  }

  private void removeAlterHashRange(Long spoint, Long epoint, boolean inclusive) {
    if (spoint < epoint) {
      moveAlterHashRangeFromExistToAlter(spoint, inclusive, epoint, true);
    } else {
      moveAlterHashRangeFromExistToAlter(0L, true, epoint, true);
      moveAlterHashRangeFromExistToAlter(spoint, inclusive, 0xFFFFFFFFL, true);
    }
  }

  private void automaticJoinCompletion(MemcachedReplicaGroup mine) {
    getLogger().info("Started automatic join completion. group=" + mine.getGroupName());
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(mine, i));
      for (int h = 0; h < 4; h++) {
        Long currPoint = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> existSet = ketamaGroups.get(currPoint);
        assert existSet != null && existSet.size() > 0;
        if (existSet.size() == 1 || existSet.first() == mine) { // visible (FIXME)
          Long prevPoint = ketamaGroups.lowerKey(currPoint);
          insertAlterHashRange(prevPoint, currPoint, true); // inclusive: true
        }
      }
    }
    getLogger().info("Completed automatic join completion. group=" + mine.getGroupName());
  }

  private Long findMigrationBasePointLEAVE() {
    Long key = ketamaGroups.lastKey();
    while (key != null) {
      for (MemcachedReplicaGroup group : ketamaGroups.get(key)) {
        if (existGroups.containsKey(group.getGroupName())) {
          break;
        }
      }
      key = ketamaGroups.lowerKey(key);
    }
    assert key != null;
    return key;
  }

  private void automaticLeaveAbort(MemcachedReplicaGroup mine) {
    if (migrationBasePoint != -1L && existGroups.size() > 0) {
      Long newBasePoint = findMigrationBasePointLEAVE();
      if (newBasePoint == migrationBasePoint) {
        return; // nothing to abort
      }

      getLogger().info("Started automatic leave abort. group=" + mine.getGroupName());
      /* abort (newBasePoint ~ migrationBasePoint) leaved hpoints */
      insertAlterHashRange(newBasePoint, migrationBasePoint, true); // inclusive: true
      migrationBasePoint = newBasePoint;
      if (migrationBasePoint == migrationLastPoint) {
        migrationBasePoint = -1L;
        migrationLastPoint = -1L;
      }
      getLogger().info("Completed automatic leave abort. group=" + mine.getGroupName());
    }
  }

  public void updateAlter(Collection<MemcachedNode> toAttach,
                          Collection<MemcachedNode> toDelete) {
    lock.lock();
    try {
      // Add the alter nodes with slave role */
      for (MemcachedNode node : toAttach) {
        String groupName = MemcachedReplicaGroup.getGroupNameFromNode(node);
        MemcachedReplicaGroup mrg = alterGroups.get(groupName);
        if (mrg == null) {
          mrg = allGroups.get(groupName);
        }
        if (mrg == null) { // close the unknown alter node (FIXME) */
          try {
            node.closeChannel();
          } catch (IOException e) {
            getLogger().error("Failed to closeChannel the node : " + node);
          }
        } else {
          mrg.setMemcachedNode(node);
          alterNodes.add(node);
        }
      }
      // Remove the failed or left alter nodes.
      for (MemcachedNode node : toDelete) {
        if (alterNodes.remove(node)) {
          MemcachedReplicaGroup mrg;
          mrg = alterGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
          if (mrg != null) {
            mrg.deleteMemcachedNode(node);
            if (mrg.isEmptyGroup()) {
              removeHashOfAlter(mrg);
            }
          }
        }
        try {
          node.closeChannel();
        } catch (IOException e) {
          getLogger().error("Failed to closeChannel the node : " + node);
        }
      }
    } finally {
      if (alterNodes.isEmpty()) {
        getLogger().info("Migration " + migrationType + " has been finished.");
        clearMigration();
      }
      lock.unlock();
    }
  }

  private void clearMigration() {
    alterNodes.clear();
    alterGroups.clear();
    existGroups.clear();
    ketamaAlterGroups.clear();
    migrationBasePoint = -1L;
    migrationLastPoint = -1L;
    migrationType = MigrationType.UNKNOWN;
    migrationInProgress = false;
  }

  public void prepareMigration(Collection<MemcachedNode> toAlter, MigrationType type) {
    getLogger().info("Prepare ketama info for migration. type=" + type);
    assert type != MigrationType.UNKNOWN;

    clearMigration();
    migrationType = type;
    migrationInProgress = true;

    MemcachedReplicaGroup mrg;
    if (type == MigrationType.JOIN) {
      for (MemcachedNode node : toAlter) {
        alterNodes.add(node);
        String groupName = MemcachedReplicaGroup.getGroupNameFromNode(node);
        mrg = alterGroups.get(groupName);
        if (mrg == null) {
          mrg = new MemcachedReplicaGroupImpl(node);
          alterGroups.put(groupName, mrg);
          prepareHashOfJOIN(mrg);
        } else {
          mrg.setMemcachedNode(node);
        }
      }
      for (String groupName : allGroups.keySet()) {
        mrg = allGroups.get(groupName);
        existGroups.put(groupName, mrg);
      }
    } else { // MigrationType.LEAVE
      for (MemcachedNode node : toAlter) {
        alterNodes.add(node);
        String groupName = MemcachedReplicaGroup.getGroupNameFromNode(node);
        mrg = alterGroups.get(groupName);
        if (mrg == null) {
          mrg = allGroups.get(groupName);
          alterGroups.put(groupName, mrg);
        }
      }
      for (String groupName : allGroups.keySet()) {
        if (!alterGroups.containsKey(groupName)) {
          mrg = allGroups.get(groupName);
          existGroups.put(groupName, mrg);
        }
      }
    }
  }

  /* check migrationLastPoint belongs to the (spoint, epoint) range. */
  private boolean needToMigrateRange(Long spoint, Long epoint) {
    if (spoint != 0 || epoint != 0) { // Valid migration range
      if (migrationLastPoint == -1) {
        return true;
      }
      if (spoint == epoint) { // full range
        return spoint != migrationLastPoint;
      }
      if (spoint < epoint) {
        if (spoint < migrationLastPoint && migrationLastPoint < epoint) {
          return true;
        }
      } else { // spoint > epoint
        if (spoint < migrationLastPoint || migrationLastPoint < epoint) {
          return true;
        }
      }
    }
    return false;
  }

  private void migrateJoinRange(Long spoint, Long epoint) {
    lock.lock();
    try {
      if (migrationLastPoint == -1) {
        migrationBasePoint = spoint;
      } else {
        spoint = migrationLastPoint;
      }
      insertAlterHashRange(spoint, epoint, false); // inclusive: false
      migrationLastPoint = epoint;
    } finally {
      lock.unlock();
    }
    getLogger().info("Applied JOIN range. spoint=" + spoint + ", epoint=" + epoint);
  }

  private void migrateLeaveRange(Long spoint, Long epoint) {
    lock.lock();
    try {
      if (migrationLastPoint == -1) {
        migrationBasePoint = epoint;
      } else {
        epoint = migrationLastPoint;
      }
      removeAlterHashRange(spoint, epoint, false); // inclusive: false
      migrationLastPoint = spoint;
    } finally {
      lock.unlock();
    }
    getLogger().info("Applied LEAVE range. spoint=" + spoint + ", epoint=" + epoint);
  }

  public void updateMigration(Long spoint, Long epoint) {
    if (migrationInProgress && needToMigrateRange(spoint, epoint)) {
      if (migrationType == MigrationType.JOIN) {
        migrateJoinRange(spoint, epoint);
      } else {
        migrateLeaveRange(spoint, epoint);
      }
    }
  }
  /* ENABLE_MIGRATION end */

  private class ReplKetamaIterator implements Iterator<MemcachedNode> {
    private final String key;
    private long hashVal;
    private int remainingTries;
    private int numTries = 0;
    private final ReplicaPick pick;

    public ReplKetamaIterator(final String k, ReplicaPick p, final int t) {
      super();
      hashVal = hashAlg.hash(k);
      remainingTries = t;
      key = k;
      pick = p;
    }

    private void nextHash() {
      long tmpKey = hashAlg.hash((numTries++) + key);
      // This echos the implementation of Long.hashCode()
      hashVal += (int) (tmpKey ^ (tmpKey >>> 32));
      hashVal &= 0xffffffffL; /* truncate to 32-bits */
      remainingTries--;
    }

    public boolean hasNext() {
      return remainingTries > 0;
    }

    public MemcachedNode next() {
      try {
        return getNodeForKey(hashVal, pick);
      } finally {
        nextHash();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException("remove not supported");
    }
  }
}
/* ENABLE_REPLICATION end */
