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
import java.util.TreeMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.ArcusKetamaNodeLocatorConfiguration;

public class ArcusKetamaNodeLocator extends SpyObject implements NodeLocator {

  private final TreeMap<Long, SortedSet<MemcachedNode>> ketamaNodes;
  private final Collection<MemcachedNode> allNodes;

  /* ENABLE_MIGRATION if */
  private TreeMap<Long, SortedSet<MemcachedNode>> ketamaAlterNodes;
  private Collection<MemcachedNode> alterNodes;
  private Collection<MemcachedNode> existNodes;

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
    ketamaNodes = new TreeMap<Long, SortedSet<MemcachedNode>>();
    config = conf;

    int numReps = config.getNodeRepetitions();
    // Ketama does some special work with md5 where it reuses chunks.
    for (MemcachedNode node : nodes) {
      insertHash(node);
    }

    // ketamaNodes.size() < numReps*nodes.size() : hash collision
    assert ketamaNodes.size() <= numReps * nodes.size();

    /* ENABLE_MIGRATION if */
    existNodes = new HashSet<MemcachedNode>();
    alterNodes = new HashSet<MemcachedNode>();
    ketamaAlterNodes = new TreeMap<Long, SortedSet<MemcachedNode>>();
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
    existNodes = new HashSet<MemcachedNode>();
    alterNodes = new HashSet<MemcachedNode>();
    ketamaAlterNodes = new TreeMap<Long, SortedSet<MemcachedNode>>();
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

  long getMaxKey() {
    return ketamaNodes.lastKey();
  }

  MemcachedNode getNodeForKey(long hash) {
    MemcachedNode rv = null;

    lock.lock();
    try {
      if (!ketamaNodes.isEmpty()) {
        if (!ketamaNodes.containsKey(hash)) {
          Long nodeHash = ketamaNodes.ceilingKey(hash);
          if (nodeHash == null) {
            hash = ketamaNodes.firstKey();
          } else {
            hash = nodeHash;
          }
        }
        rv = ketamaNodes.get(hash).first();
      }
    } finally {
      lock.unlock();
    }
    return rv;
  }

  public Iterator<MemcachedNode> getSequence(String k) {
    return new KetamaIterator(k, allNodes.size());
  }

  public NodeLocator getReadonlyCopy() {
    TreeMap<Long, SortedSet<MemcachedNode>> smn =
            new TreeMap<Long, SortedSet<MemcachedNode>>(ketamaNodes);
    Collection<MemcachedNode> an = new ArrayList<MemcachedNode>(
            allNodes.size());

    lock.lock();
    try {
      // Rewrite the values a copy of the map.
      for (Map.Entry<Long, SortedSet<MemcachedNode>> me : smn.entrySet()) {
        SortedSet<MemcachedNode> nodeROSet =
                new TreeSet<MemcachedNode>(config.new NodeNameComparator());
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
      // Add memcached nodes.
      for (MemcachedNode node : toAttach) {
        allNodes.add(node);
        insertHash(node);
      }

      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        allNodes.remove(node);
        removeHash(node);
        try {
          node.closeChannel();
        } catch (IOException e) {
          getLogger().error(
                  "Failed to closeChannel the node : " + node);
        }
      }
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
          nodeSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
          ketamaNodes.put(k, nodeSet);
        }
        nodeSet.add(node);
      }
    }
  }

  private void removeHash(MemcachedNode node) {
    /* ENABLE_MIGRATION if */
    /* DISABLE_AUTO_MIGRATION
    boolean callAutoLeaveAbort = false;
    */
    if (migrationInProgress) {
      if (alterNodes.remove(node)) {
        // A leaving node is down or has left
        assert migrationType == MigrationType.LEAVE;
        removeHashOfAlter(node);
        return;
      }
      if (existNodes.remove(node)) {
        // An existing node is down
        /* DISABLE_AUTO_MIGRATION
        if (migrationType == MigrationType.JOIN) {
          automaticJoinCompletion(node);
        } else {
          callAutoLeaveAbort = true;
        }
        */
      } else {
        // A joined node is down : do nothing
      }
      // go downward
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
        if (nodeSet.size() == 0) {
          ketamaNodes.remove(k);
        }
      }
    }
    config.removeNode(node);

    /* ENABLE_MIGRATION if */
    /* DISABLE_AUTO_MIGRATION
    if (callAutoLeaveAbort) {
      automaticLeaveAbort(node);
    }
    */
    /* ENABLE_MIGRATION end */
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
          nodeSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
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
          if (alterSet.size() == 0) {
            ketamaAlterNodes.remove(k);
          }
          SortedSet<MemcachedNode> existSet = ketamaNodes.get(k);
          if (existSet == null) {
            existSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
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

  /* Move an alter hash range from ketamaAlterNodes to ketamaNodes */
  private void moveAlterHashRangeFromAlterToExist(Long spoint, boolean sInclusive,
                                                  Long epoint, boolean eInclusive) {
    NavigableMap<Long, SortedSet<MemcachedNode>> moveRange
        = ketamaAlterNodes.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<Long>();
    for (Map.Entry<Long, SortedSet<MemcachedNode>> entry : moveRange.entrySet()) {
      SortedSet<MemcachedNode> nodeSet = ketamaNodes.get(entry.getKey());
      if (nodeSet == null) {
        nodeSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
        ketamaNodes.put(entry.getKey(), nodeSet);
      }
      nodeSet.addAll(entry.getValue());
      removeList.add(entry.getKey());
    }
    for (Long key : removeList) {
      ketamaAlterNodes.remove(key);
    }
  }

  /* Move an alter hash range from ketamaNode to ketamaAlterNodes */
  private void moveAlterHashRangeFromExistToAlter(Long spoint, boolean sInclusive,
                                                  Long epoint, boolean eInclusive) {
    NavigableMap<Long, SortedSet<MemcachedNode>> moveRange
        = ketamaNodes.subMap(spoint, sInclusive, epoint, eInclusive);

    List<Long> removeList = new ArrayList<Long>();
    for (Map.Entry<Long, SortedSet<MemcachedNode>> entry : moveRange.entrySet()) {
      Iterator<MemcachedNode> nodeIter = entry.getValue().iterator();
      while (nodeIter.hasNext()) {
        MemcachedNode node = nodeIter.next();
        if (alterNodes.contains(node)) {
          nodeIter.remove(); // leaving => leaved
          SortedSet<MemcachedNode> alterSet = ketamaAlterNodes.get(entry.getKey());
          if (alterSet == null) {
            alterSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
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

  /* DISABLE_AUTO_MIGRATION
  private void automaticJoinCompletion(MemcachedNode mine) {
    getLogger().info("Started automatic join completion. node=" + mine.getNodeName());
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(mine, i));
      for (int h = 0; h < 4; h++) {
        Long currPoint = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> existSet = ketamaNodes.get(currPoint);
        assert existSet != null && existSet.size() > 0;
        if (existSet.size() == 1 || existSet.first() == mine) { // visible (FIXME)
          Long prevPoint = ketamaNodes.lowerKey(currPoint);
          insertAlterHashRange(prevPoint, currPoint, true); // inclusive: true
        }
      }
    }
    getLogger().info("Completed automatic join completion. node=" + mine.getNodeName());
  }

  private Long findMigrationBasePointLEAVE() {
    Long key = ketamaNodes.lastKey();
    while (key != null) {
      for (MemcachedNode node : ketamaNodes.get(key)) {
        if (existNodes.contains(node)) {
          break;
        }
      }
      key = ketamaNodes.lowerKey(key);
    }
    assert key != null;
    return key;
  }

  private void automaticLeaveAbort(MemcachedNode mine) {
    if (migrationBasePoint != -1L && existNodes.size() > 0) {
      Long newBasePoint = findMigrationBasePointLEAVE();
      if (newBasePoint == migrationBasePoint) {
        return; // nothing to abort
      }

      getLogger().info("Started automatic leave abort. node=" + mine.getNodeName());
      // abort (newBasePoint ~ migrationBasePoint) leaved hpoints
      insertAlterHashRange(newBasePoint, migrationBasePoint, true); // inclusive: true
      migrationBasePoint = newBasePoint;
      if (migrationBasePoint == migrationLastPoint) {
        migrationBasePoint = -1L;
        migrationLastPoint = -1L;
      }
      getLogger().info("Completed automatic leave abort. node=" + mine.getNodeName());
    }
  }
  */

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

    /* prepare alterNodes, ketamaAlterNodes and existNodes */
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
