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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.ArcusKetamaNodeLocatorConfiguration;

public class ArcusKetamaNodeLocator extends SpyObject implements NodeLocator {

  private final TreeMap<Long, SortedSet<MemcachedNode>> ketamaNodes;
  private Collection<MemcachedNode> allNodes;

  /* ENABLE_MIGRATION if */
  private TreeMap<Long, SortedSet<MemcachedNode>> ketamaAlterNodes;
  private HashSet<MemcachedNode> alterNodes;
  private HashSet<MemcachedNode> existNodes;

  private MigrationType migrationType;
  private Long migrationBasePoint;
  private Long migrationLastPoint;
  private boolean migrationInProgress;
  /* ENABLE_MIGRATION end */

  private final HashAlgorithm hashAlg = HashAlgorithm.KETAMA_HASH;
  private final ArcusKetamaNodeLocatorConfiguration config;

  private final Lock lock = new ReentrantLock();

  public ArcusKetamaNodeLocator(List<MemcachedNode> nodes) {
    this(nodes, new ArcusKetamaNodeLocatorConfiguration());
  }

  public ArcusKetamaNodeLocator(List<MemcachedNode> nodes,
                                ArcusKetamaNodeLocatorConfiguration conf) {
    super();
    allNodes = nodes;
    ketamaNodes = new TreeMap<>();
    config = conf;

    int numReps = config.getNodeRepetitions();
    // Ketama does some special work with md5 where it reuses chunks.
    for (MemcachedNode node : nodes) {
      insertHash(node);
    }

    // ketamaNodes.size() < numReps*nodes.size() : hash collision
    assert ketamaNodes.size() <= numReps * nodes.size();

    /* ENABLE_MIGRATION if */
    existNodes = new HashSet<>();
    alterNodes = new HashSet<>();
    ketamaAlterNodes = new TreeMap<>();
    clearMigration();
    /* ENABLE_MIGRATION end */
  }

  private ArcusKetamaNodeLocator(TreeMap<Long, SortedSet<MemcachedNode>> smn,
                                 Collection<MemcachedNode> an,
                                 ArcusKetamaNodeLocatorConfiguration conf) {
    super();
    ketamaNodes = smn;
    allNodes = an;
    config = conf;

    /* ENABLE_MIGRATION if */
    existNodes = new HashSet<>();
    alterNodes = new HashSet<>();
    ketamaAlterNodes = new TreeMap<>();
    clearMigration();
    /* ENABLE_MIGRATION end */
  }

  public Collection<MemcachedNode> getAll() {
    return Collections.unmodifiableCollection(allNodes);
  }

  /* ENABLE_MIGRATION if */
  public Collection<MemcachedNode> getAlterAll() {
    return Collections.unmodifiableCollection(alterNodes);
  }

  public MemcachedNode getAlterNode(SocketAddress sa) {
    // The alter node to attach must be found
    for (MemcachedNode node : alterNodes) {
      if (sa.equals(node.getSocketAddress())) {
        return node;
      }
    }
    return null;
  }

  public MemcachedNode getOwnerNode(String owner, MigrationType mgType) {
    InetSocketAddress ownerAddress = AddrUtil.getAddress(owner);
    if (mgType == MigrationType.JOIN) {
      for (MemcachedNode node : alterNodes) {
        if (node.getSocketAddress().equals(ownerAddress)) {
          return node;
        }
      }
      for (MemcachedNode node : allNodes) {
        if (node.getSocketAddress().equals(ownerAddress)) {
          return node;
        }
      }
    } else { // MigrationType.LEAVE
      for (MemcachedNode node : existNodes) {
        if (node.getSocketAddress().equals(ownerAddress)) {
          return node;
        }
      }
    }
    return null;
  }
  /* ENABLE_MIGRATION end */

  public SortedMap<Long, SortedSet<MemcachedNode>> getKetamaNodes() {
    return Collections.unmodifiableSortedMap(ketamaNodes);
  }

  public MemcachedNode getPrimary(final String k) {
    return getNodeForKey(hashAlg.hash(k));
  }

  MemcachedNode getNodeForKey(long hash) {
    lock.lock();
    try {
      if (ketamaNodes.isEmpty()) {
        return null;
      }
      Map.Entry<Long, SortedSet<MemcachedNode>> entry = ketamaNodes.ceilingEntry(hash);
      if (entry == null) {
        entry = ketamaNodes.firstEntry();
      }
      return entry.getValue().first();
    } finally {
      lock.unlock();
    }
  }

  public Iterator<MemcachedNode> getSequence(String k) {
    return new KetamaIterator(k, allNodes.size());
  }

  public NodeLocator getReadonlyCopy() {
    TreeMap<Long, SortedSet<MemcachedNode>> smn =
            new TreeMap<>(ketamaNodes);
    Collection<MemcachedNode> an = new ArrayList<>(
            allNodes.size());

    lock.lock();
    try {
      // Rewrite the values a copy of the map.
      for (Map.Entry<Long, SortedSet<MemcachedNode>> me : smn.entrySet()) {
        SortedSet<MemcachedNode> nodeROSet =
                new TreeSet<>(config.new NodeNameComparator());
        for (MemcachedNode mn : me.getValue()) {
          nodeROSet.add(new MemcachedNodeROImpl(mn));
        }
        me.setValue(nodeROSet);
      }
      // Copy the allNodes collection.
      for (MemcachedNode n : allNodes) {
        an.add(new MemcachedNodeROImpl(n));
      }
    } finally {
      lock.unlock();
    }

    return new ArcusKetamaNodeLocator(smn, an, config);
  }

  public void update(Collection<MemcachedNode> toAttach,
                     Collection<MemcachedNode> toDelete) {
    lock.lock();
    try {
      ArrayList<MemcachedNode> newAllNodes = new ArrayList<>(allNodes);
      // Add memcached nodes.
      for (MemcachedNode node : toAttach) {
        newAllNodes.add(node);
        insertHash(node);
      }

      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        newAllNodes.remove(node);
        removeHash(node);
        try {
          node.closeChannel();
        } catch (IOException e) {
          getLogger().error(
                  "Failed to closeChannel the node : " + node);
        }
      }
      allNodes = newAllNodes;
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

  private Long getKetamaHashPoint(byte[] digest, int h) {
    return ((long) (digest[3 + h * 4] & 0xFF) << 24)
         | ((long) (digest[2 + h * 4] & 0xFF) << 16)
         | ((long) (digest[1 + h * 4] & 0xFF) << 8)
         | (digest[h * 4] & 0xFF);
  }

  private void insertHash(MemcachedNode node) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      if (alterNodes.contains(node)) {
        alterNodes.remove(node);
        insertHashOfJOIN(node); // joining => joined
        return;
      }
      // How to handle the new node ? go downward (FIXME)
    }
    /* ENABLE_MIGRATION end */

    config.insertNode(node);

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> nodeSet = ketamaNodes.get(k);
        if (nodeSet == null) {
          nodeSet = new TreeSet<>(config.new NodeNameComparator());
          ketamaNodes.put(k, nodeSet);
        }
        nodeSet.add(node);
      }
    }
  }

  private void removeHash(MemcachedNode node) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      if (alterNodes.remove(node)) {
        // A leaving node is down or has left
        assert migrationType == MigrationType.LEAVE;
        removeHashOfAlter(node);
        return;
      }
      // An existing or joined node is down. go downward
    }
    /* ENABLE_MIGRATION end */

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> nodeSet = ketamaNodes.get(k);
        assert nodeSet != null;
        nodeSet.remove(node);
        if (nodeSet.isEmpty()) {
          ketamaNodes.remove(k);
        }
      }
    }
    config.removeNode(node);
  }

  /* ENABLE_MIGRATION if */
  /* Insert the joining hash points into ketamaAlterNodes */
  private void prepareHashOfJOIN(MemcachedNode node) {
    config.insertNode(node); // register the node in config

    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> nodeSet = ketamaAlterNodes.get(k);
        if (nodeSet == null) {
          nodeSet = new TreeSet<>(config.new NodeNameComparator());
          ketamaAlterNodes.put(k, nodeSet);
        }
        nodeSet.add(node);
      }
    }
  }

  /* Insert the joining hash points into ketamaNodes. */
  private void insertHashOfJOIN(MemcachedNode node) {
    config.insertNode(node); // register the node in config

    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> alterSet = ketamaAlterNodes.get(k);
        if (alterSet != null && alterSet.remove(node)) {
          if (alterSet.isEmpty()) {
            ketamaAlterNodes.remove(k);
          }
          SortedSet<MemcachedNode> existSet = ketamaNodes.get(k);
          if (existSet == null) {
            existSet = new TreeSet<>(config.new NodeNameComparator());
            ketamaNodes.put(k, existSet);
          }
          existSet.add(node); // joining => joined
        } else {
          // The hash points have already been inserted.
        }
      }
    }
  }

  /* Remove all hash points of the alter node */
  private void removeHashOfAlter(MemcachedNode node) {
    // The alter hpoints can be in both ketamaAlterNodes and ketamaNodes.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> nodeSet = ketamaAlterNodes.get(k);
        if (nodeSet != null && nodeSet.remove(node)) {
          if (nodeSet.isEmpty()) {
            ketamaAlterNodes.remove(k);
          }
        } else {
          nodeSet = ketamaNodes.get(k);
          assert nodeSet != null;
          boolean removed = nodeSet.remove(node);
          if (nodeSet.isEmpty()) {
            ketamaNodes.remove(k);
          }
          assert removed;
        }
      }
    }
    config.removeNode(node); // unregister the node in config
  }

  /* Move the hash range of joining nodes from ketamaAlterNodes to ketamaNodes */
  private void moveHashRangeFromAlterToExist(Long spoint, boolean sInclusive,
                                             Long epoint, boolean eInclusive) {
    NavigableMap<Long, SortedSet<MemcachedNode>> moveRange
        = ketamaAlterNodes.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<>();
    for (Map.Entry<Long, SortedSet<MemcachedNode>> entry : moveRange.entrySet()) {
      SortedSet<MemcachedNode> nodeSet = ketamaNodes.get(entry.getKey());
      if (nodeSet == null) {
        nodeSet = new TreeSet<>(config.new NodeNameComparator());
        ketamaNodes.put(entry.getKey(), nodeSet);
      }
      nodeSet.addAll(entry.getValue());
      removeList.add(entry.getKey());
    }
    for (Long key : removeList) {
      ketamaAlterNodes.remove(key);
    }
  }

  /* Move the hash range of leaving nodes from ketamaNode to ketamaAlterNodes */
  private void moveHashRangeFromExistToAlter(Long spoint, boolean sInclusive,
                                             Long epoint, boolean eInclusive) {
    NavigableMap<Long, SortedSet<MemcachedNode>> moveRange
        = ketamaNodes.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<>();
    for (Map.Entry<Long, SortedSet<MemcachedNode>> entry : moveRange.entrySet()) {
      Iterator<MemcachedNode> nodeIter = entry.getValue().iterator();
      while (nodeIter.hasNext()) {
        MemcachedNode node = nodeIter.next();
        if (!existNodes.contains(node)) {
          nodeIter.remove(); // leaving => leaved
          SortedSet<MemcachedNode> alterSet = ketamaAlterNodes.get(entry.getKey());
          if (alterSet == null) {
            alterSet = new TreeSet<>(config.new NodeNameComparator());
            ketamaAlterNodes.put(entry.getKey(), alterSet); // for auto leave abort
          }
          alterSet.add(node);
        }
      }
      if (entry.getValue().isEmpty()) {
        removeList.add(entry.getKey());
      }
    }
    for (Long key : removeList) {
      ketamaNodes.remove(key);
    }
  }

  public void updateAlter(Collection<MemcachedNode> toAttach,
                          Collection<MemcachedNode> toDelete) {
    assert toAttach.isEmpty() == true;
    lock.lock();
    try {
      // Remove the failed or left alter nodes.
      for (MemcachedNode node : toDelete) {
        alterNodes.remove(node);
        removeHashOfAlter(node);
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
    existNodes.clear();
    alterNodes.clear();
    ketamaAlterNodes.clear();
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

    /* prepare existNodes, alterNodes and ketamaAlterNodes */
    if (type == MigrationType.JOIN) {
      for (MemcachedNode node : toAlter) {
        alterNodes.add(node);
        prepareHashOfJOIN(node);
      }
      for (MemcachedNode node : allNodes) {
        existNodes.add(node);
      }
    } else { // MigrationType.LEAVE
      for (MemcachedNode node : toAlter) {
        alterNodes.add(node);
      }
      for (MemcachedNode node : allNodes) {
        if (!alterNodes.contains(node)) {
          existNodes.add(node);
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

  private void migrateJoinHashRange(Long spoint, Long epoint) {
    lock.lock();
    try {
      if (migrationLastPoint == -1) {
        migrationBasePoint = spoint;
      } else {
        spoint = migrationLastPoint;
      }
      if (spoint < epoint) {
        moveHashRangeFromAlterToExist(spoint, false, epoint, true);
      } else {
        moveHashRangeFromAlterToExist(spoint, false, 0xFFFFFFFFL, true);
        moveHashRangeFromAlterToExist(0L, true, epoint, true);
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
        moveHashRangeFromExistToAlter(spoint, false, epoint, true);
      } else {
        moveHashRangeFromExistToAlter(0L, true, epoint, true);
        moveHashRangeFromExistToAlter(spoint, false, 0xFFFFFFFFL, true);
      }
      migrationLastPoint = spoint;
    } finally {
      lock.unlock();
    }
    getLogger().info("Applied LEAVE range. spoint=" + spoint + ", epoint=" + epoint);
  }

  public void updateMigration(Long spoint, Long epoint) {
    if (migrationInProgress && needToMigrateRange(spoint, epoint)) {
      if (migrationType == MigrationType.JOIN) {
        migrateJoinHashRange(spoint, epoint);
      } else {
        migrateLeaveHashRange(spoint, epoint);
      }
    }
  }
  /* ENABLE_MIGRATION end */

  class KetamaIterator implements Iterator<MemcachedNode> {

    private final String key;
    private long hashVal;
    private int remainingTries;
    private int numTries = 0;

    public KetamaIterator(final String k, final int t) {
      super();
      hashVal = hashAlg.hash(k);
      remainingTries = t;
      key = k;
    }

    private void nextHash() {
      long tmpKey = hashAlg.hash((numTries++) + key);
      // This echos the implementation of Long.hashCode()
      hashVal += (int) (tmpKey ^ (tmpKey >>> 32));
      hashVal &= 0xffffffffL; // truncate to 32-bits
      remainingTries--;
    }

    public boolean hasNext() {
      return remainingTries > 0;
    }

    public MemcachedNode next() {
      try {
        return getNodeForKey(hashVal);
      } finally {
        nextHash();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException("remove not supported");
    }

  }
}
