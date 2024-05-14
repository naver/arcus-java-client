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
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.ArcusReplKetamaNodeLocatorConfiguration;

import static net.spy.memcached.util.ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator;

public class ArcusReplKetamaNodeLocator extends SpyObject implements NodeLocator {

  private static final HashAlgorithm HASH_ALG = HashAlgorithm.KETAMA_HASH;

  private final TreeMap<Long, SortedSet<MemcachedReplicaGroup>> ketamaGroups;
  private final HashMap<String, MemcachedReplicaGroup> allGroups;
  private final Collection<MemcachedNode> allNodes;

  /* ENABLE_MIGRATION if */
  private final TreeMap<Long, SortedSet<MemcachedReplicaGroup>> ketamaAlterGroups = new TreeMap<>();
  private final HashMap<String, MemcachedReplicaGroup> alterGroups = new HashMap<>();
  private final HashMap<String, MemcachedReplicaGroup> existGroups = new HashMap<>();
  private final HashSet<MemcachedNode> alterNodes = new HashSet<>();

  private MigrationType migrationType;
  private Long migrationBasePoint; // FIXME: Remove this field
  private Long migrationLastPoint;
  private boolean migrationInProgress;
  /* ENABLE_MIGRATION end */

  private final Collection<MemcachedReplicaGroup> toDeleteGroups = new HashSet<>();
  private final ArcusReplKetamaNodeLocatorConfiguration config
          = new ArcusReplKetamaNodeLocatorConfiguration();

  private final Lock lock = new ReentrantLock();

  public ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes) {
    super();
    allNodes = nodes;
    ketamaGroups = new TreeMap<>();
    allGroups = new HashMap<>();

    // create all memcached replica group
    for (MemcachedNode node : nodes) {
      MemcachedReplicaGroup mrg;
      mrg = allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
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
    // ketamaNodes.size() < numReps*nodes.size() : hash collision
    assert ketamaGroups.size() <= (numReps * allGroups.size());

    /* ENABLE_MIGRATION if */
    clearMigration();
    /* ENABLE_MIGRATION end */
  }

  private ArcusReplKetamaNodeLocator(TreeMap<Long, SortedSet<MemcachedReplicaGroup>> kg,
                                     HashMap<String, MemcachedReplicaGroup> ag,
                                     Collection<MemcachedNode> an) {
    super();
    ketamaGroups = kg;
    allGroups = ag;
    allNodes = an;

    /* ENABLE_MIGRATION if */
    clearMigration();
    /* ENABLE_MIGRATION end */
  }

  @Override
  public Collection<MemcachedNode> getAll() {
    return Collections.unmodifiableCollection(allNodes);
  }

  public Map<String, MemcachedReplicaGroup> getAllGroups() {
    return Collections.unmodifiableMap(allGroups);
  }

  /* ENABLE_MIGRATION if */
  @Override
  public Collection<MemcachedNode> getAlterAll() {
    return Collections.unmodifiableCollection(alterNodes);
  }

  @Override
  public MemcachedNode getAlterNode(SocketAddress sa) {
    // The alter node to attach should be found
    for (MemcachedNode node : alterNodes) {
      if (sa.equals(node.getSocketAddress())) {
        return node;
      }
    }
    // If a slave node has started during migration, it may not exist
    return null;
  }

  @Override
  public MemcachedNode getOwnerNode(String owner, MigrationType mgType) {
    MemcachedReplicaGroup group;
    if (mgType == MigrationType.JOIN) {
      group = alterGroups.get(owner);
      if (group == null) {
        group = allGroups.get(owner);
      }
    } else { // MigrationType.LEAVE
      group = existGroups.get(owner);
    }
    if (group != null) {
      return group.getMasterNode();
    }
    return null;
  }
  /* ENABLE_MIGRATION end */

  @Override
  public MemcachedNode getPrimary(final String k) {
    return getNodeForKey(HASH_ALG.hash(k), ReplicaPick.MASTER);
  }

  MemcachedNode getPrimary(final String k, ReplicaPick pick) {
    return getNodeForKey(HASH_ALG.hash(k), pick);
  }

  private MemcachedNode getNodeForKey(long hash, ReplicaPick pick) {
    lock.lock();
    try {
      if (ketamaGroups.isEmpty()) {
        return null;
      }
      Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> entry = ketamaGroups.ceilingEntry(hash);
      if (entry == null) {
        entry = ketamaGroups.firstEntry();
      }
      MemcachedReplicaGroup rg = entry.getValue().first();
      // return a node (master / slave) for the replica pick request.
      return rg.getNodeByReplicaPick(pick);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Iterator<MemcachedNode> getSequence(String k) {
    return new ReplKetamaIterator(k, ReplicaPick.MASTER, allGroups.size());
  }

  Iterator<MemcachedNode> getSequence(String k, ReplicaPick pick) {
    return new ReplKetamaIterator(k, pick, allGroups.size());
  }

  @Override
  public NodeLocator getReadonlyCopy() {
    TreeMap<Long, SortedSet<MemcachedReplicaGroup>> smg =
            new TreeMap<>(ketamaGroups);
    HashMap<String, MemcachedReplicaGroup> ag =
            new HashMap<>(allGroups.size());
    Collection<MemcachedNode> an = new ArrayList<>(allNodes.size());

    lock.lock();
    try {
      // Rewrite the values a copy of the map
      for (Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> mge : smg.entrySet()) {
        SortedSet<MemcachedReplicaGroup> groupROSet = createGroupSet();
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

  @Override
  public void update(Collection<MemcachedNode> toAttach, Collection<MemcachedNode> toDelete) {
    update(toAttach, toDelete, new ArrayList<>(0));
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
        safeCloseNode(node);
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
      // FIXME: How to handle the new node? go downward
      getLogger().debug("new node, but NOT in alterNodes: " + node);
    }
    /* ENABLE_MIGRATION end */

    MemcachedReplicaGroup mrg = allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
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
      // go downward
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
        ketamaGroups.computeIfAbsent(k, a -> createGroupSet()).add(group);
      }
    }
  }

  private void removeHash(MemcachedReplicaGroup group) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      if (alterGroups.remove(group.getGroupName()) != null) {
        // A leaving group is down or has left
        assert migrationType == MigrationType.LEAVE;
        removeHashOfAlter(group);
        return;
      }
      // An existing or joined group is down. go downward.
      getLogger().debug("removed group, but NOT in alterGroups: " + group);
    }
    /* ENABLE_MIGRATION end */

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> nodeSet = ketamaGroups.get(k);
        nodeSet.remove(group);
        if (nodeSet.isEmpty()) {
          ketamaGroups.remove(k);
        }
      }
    }
  }

  /* ENABLE_MIGRATION if */
  /* Insert the joining hash points into ketamaAlterGroups */
  private void prepareHashOfJOIN(MemcachedReplicaGroup group) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        ketamaGroups.computeIfAbsent(k, a -> createGroupSet()).add(group);
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
          if (alterSet.isEmpty()) {
            ketamaAlterGroups.remove(k);
          }
          ketamaGroups.computeIfAbsent(k, a -> createGroupSet()).add(group); // joining => joined
        } else {
          getLogger().debug("The hash point has already been inserted.");
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
          continue;
        }

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

  /* Move the hash range of joining nodes from ketamaAlterNodes to ketamaNodes */
  private void moveHashRangeFromAlterToExist(Long spoint, boolean sInclusive,
                                             Long epoint) {

    final boolean eInclusive = true; // FIXME: Is this variable really can be false?
    NavigableMap<Long, SortedSet<MemcachedReplicaGroup>> moveRange
        = ketamaAlterGroups.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<>();
    for (Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> entry : moveRange.entrySet()) {
      ketamaGroups.computeIfAbsent(entry.getKey(), a -> createGroupSet()).addAll(entry.getValue());
      removeList.add(entry.getKey());
    }
    for (Long key : removeList) {
      ketamaAlterGroups.remove(key);
    }
  }

  /* Move the hash range of leaving nodes from ketamaNode to ketamaAlterNodes */
  private void moveHashRangeFromExistToAlter(Long spoint, boolean sInclusive,
                                             Long epoint) {

    final boolean eInclusive = true; // FIXME: Is this variable really can be false?
    NavigableMap<Long, SortedSet<MemcachedReplicaGroup>> moveRange
        = ketamaGroups.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<>();
    for (Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> entry : moveRange.entrySet()) {
      Iterator<MemcachedReplicaGroup> groupIter = entry.getValue().iterator();
      while (groupIter.hasNext()) {
        MemcachedReplicaGroup group = groupIter.next();
        if (existGroups.containsKey(group.getGroupName())) {
          continue;
        }
        groupIter.remove(); // leaving => leaved
        // for auto leave abort
        ketamaAlterGroups.computeIfAbsent(entry.getKey(), a -> createGroupSet()).add(group);
      }
      if (entry.getValue().isEmpty()) {
        removeList.add(entry.getKey());
      }
    }
    for (Long key : removeList) {
      ketamaGroups.remove(key);
    }
  }

  @Override
  public void updateAlter(Collection<MemcachedNode> toAttach,
                          Collection<MemcachedNode> toDelete) {
    lock.lock();
    try {
      // Add the alter nodes with slave role
      for (MemcachedNode node : toAttach) {
        String groupName = MemcachedReplicaGroup.getGroupNameFromNode(node);
        MemcachedReplicaGroup mrg = alterGroups.get(groupName);
        if (mrg == null) {
          mrg = allGroups.get(groupName);
        }
        if (mrg == null) {
          // FIXME: close the unknown alter node
          getLogger().warn("Unknown alter node to attach : " + node);
          safeCloseNode(node);
        } else {
          mrg.setMemcachedNode(node);
          alterNodes.add(node);
        }
      }
      // Remove the failed or left alter nodes.
      for (MemcachedNode node : toDelete) {
        alterNodes.remove(node);
        MemcachedReplicaGroup mrg;
        mrg = alterGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
        if (mrg != null) {
          mrg.deleteMemcachedNode(node);
          if (mrg.isEmptyGroup()) {
            removeHashOfAlter(mrg);
          }
        }
        safeCloseNode(node);
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

  @Override
  public void prepareMigration(Collection<MemcachedNode> toAlter, MigrationType type) {
    if (type == MigrationType.UNKNOWN) {
      throw new IllegalArgumentException("MigrationType must NOT be UNKNOWN.");
    }
    getLogger().info("Prepare ketama info for migration. type=" + type);

    clearMigration();
    migrationType = type;
    migrationInProgress = true;

    if (type == MigrationType.JOIN) {
      for (MemcachedNode node : toAlter) {
        alterNodes.add(node);
        String groupName = MemcachedReplicaGroup.getGroupNameFromNode(node);
        MemcachedReplicaGroup mrg = alterGroups.get(groupName);
        if (mrg != null) {
          mrg.setMemcachedNode(node);
          continue;
        }

        mrg = new MemcachedReplicaGroupImpl(node);
        alterGroups.put(groupName, mrg);
        prepareHashOfJOIN(mrg);
      }
      existGroups.putAll(allGroups);
      return;
    }

    // MigrationType.LEAVE
    for (MemcachedNode node : toAlter) {
      alterNodes.add(node);
      String groupName = MemcachedReplicaGroup.getGroupNameFromNode(node);
      alterGroups.computeIfAbsent(groupName, a -> allGroups.get(groupName));
    }
    existGroups.putAll(allGroups.entrySet().stream()
            .filter(entry -> !alterGroups.containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  /* check migrationLastPoint belongs to the (spoint, epoint) range. */
  private boolean needToMigrate(Long spoint, Long epoint) {
    if (spoint == 0 && epoint == 0) {
      return true;
    }

    // Valid migration range
    if (migrationLastPoint == -1) {
      return true;
    }
    if (Objects.equals(spoint, epoint)) { // full range
      return !Objects.equals(spoint, migrationLastPoint);
    }
    if (spoint < epoint) {
      return spoint < migrationLastPoint && migrationLastPoint < epoint;
    }
    // spoint > epoint
    return spoint < migrationLastPoint || migrationLastPoint < epoint;
  }

  private void migrateJoinHashRange(Long spoint, Long epoint) {
    lock.lock();
    try {
      if (migrationLastPoint == -1) {
        migrationBasePoint = spoint;
      } else {
        spoint = migrationLastPoint;
      }
      if (spoint < epoint) {
        moveHashRangeFromAlterToExist(spoint, false, epoint);
      } else {
        moveHashRangeFromAlterToExist(spoint, false, 0xFFFFFFFFL);
        moveHashRangeFromAlterToExist(0L, true, epoint);
      }
      migrationLastPoint = epoint;
    } finally {
      lock.unlock();
    }
    getLogger().info("Applied JOIN range. spoint=" + spoint + ", epoint=" + epoint);
  }

  private void migrateLeaveHashRange(Long spoint, Long epoint) {
    lock.lock();
    try {
      if (migrationLastPoint == -1) {
        migrationBasePoint = epoint;
      } else {
        epoint = migrationLastPoint;
      }
      if (spoint < epoint) {
        moveHashRangeFromExistToAlter(spoint, false, epoint);
      } else {
        moveHashRangeFromExistToAlter(0L, true, epoint);
        moveHashRangeFromExistToAlter(spoint, false, 0xFFFFFFFFL);
      }
      migrationLastPoint = spoint;
    } finally {
      lock.unlock();
    }
    getLogger().info("Applied LEAVE range. spoint=" + spoint + ", epoint=" + epoint);
  }

  @Override
  public void updateMigration(Long spoint, Long epoint) {
    if (migrationInProgress && needToMigrate(spoint, epoint)) {
      if (migrationType == MigrationType.JOIN) {
        migrateJoinHashRange(spoint, epoint);
      } else {
        migrateLeaveHashRange(spoint, epoint);
      }
    }
  }
  /* ENABLE_MIGRATION end */

  private void safeCloseNode(MemcachedNode node) {
    try {
      node.closeChannel();
    } catch (IOException e) {
      getLogger().error("Failed to closeChannel the node : " + node);
    }
  }

  private static SortedSet<MemcachedReplicaGroup> createGroupSet() {
    return new TreeSet<>(new MemcachedReplicaGroupComparator());
  }

  private class ReplKetamaIterator implements Iterator<MemcachedNode> {
    private final String key;
    private long hashVal;
    private int remainingTries;
    private int numTries = 0;
    private final ReplicaPick pick;

    public ReplKetamaIterator(final String k, ReplicaPick p, final int t) {
      super();
      hashVal = HASH_ALG.hash(k);
      remainingTries = t;
      key = k;
      pick = p;
    }

    private void nextHash() {
      long tmpKey = HASH_ALG.hash((numTries++) + key);
      // This echos the implementation of Long.hashCode()
      hashVal += Long.hashCode(tmpKey);
      hashVal &= 0xffffffffL; // truncate to 32-bits
      remainingTries--;
    }

    @Override
    public boolean hasNext() {
      return remainingTries > 0;
    }

    @Override
    public MemcachedNode next() {
      try {
        return getNodeForKey(hashVal, pick);
      } finally {
        nextHash();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not supported");
    }
  }
}
/* ENABLE_REPLICATION end */
