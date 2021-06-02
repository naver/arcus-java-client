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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
  private Collection<MemcachedNode> existNodes;
  private Collection<MemcachedNode> alterNodes;

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
    /* ENABLE_MIGRATION if */
    existNodes = new ArrayList<MemcachedNode>();
    alterNodes = new ArrayList<MemcachedNode>();
    ketamaAlterNodes = new TreeMap<Long, SortedSet<MemcachedNode>>();
    /* ENABLE_MIGRATION end */
    config = conf;

    int numReps = config.getNodeRepetitions();
    // Ketama does some special work with md5 where it reuses chunks.
    for (MemcachedNode node : nodes) {
      insertHash(node, ketamaNodes);
    }

    /* ketamaNodes.size() < numReps*nodes.size() : hash collision */
    assert ketamaNodes.size() <= numReps * nodes.size();
  }

  private ArcusKetamaNodeLocator(TreeMap<Long, SortedSet<MemcachedNode>> smn,
                                 Collection<MemcachedNode> an,
                                 ArcusKetamaNodeLocatorConfiguration conf) {
    super();
    ketamaNodes = smn;
    allNodes = an;
    config = conf;
  }

  public Collection<MemcachedNode> getAll() {
    return Collections.unmodifiableCollection(allNodes);
  }

  /* ENABLE_MIGRATION if */
  public Collection<MemcachedNode> getAlterAll() {
    return Collections.unmodifiableCollection(alterNodes);
  }

  public Collection<MemcachedNode> getExistAll() {
    return Collections.unmodifiableCollection(existNodes);
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
        /* ENABLE_MIGRATION if */
        if (migrationInProgress) {
          if (alterNodes.contains(node)) { /* joining node has joined */
            migrateKetamaEntireJOIN(node);
            alterNodes.remove(node);
          }
        /* ENABLE_MIGRATION end */
        } else {
          insertHash(node, ketamaNodes);
        }
      }

      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        allNodes.remove(node);
        /* ENABLE_MIGRATION if */
        if (migrationInProgress) {
          if (existNodes.contains(node)) { /* existing node down */
            if (migrationType == MigrationType.JOIN) {
              automaticJoinCompletion(node);
            } else { /* MigrationType.LEAVE */
              if (existNodes.size() > 1) {
                automaticLeaveAbort(node);
              }
              removeHash(node, ketamaNodes);
            }
            existNodes.remove(node);
          } else if (alterNodes.contains(node)) { /* alter node down or leaving node has left */
            if (migrationType == MigrationType.JOIN) {
              removeKetamaJOIN(node); /* remove down joining hpoint */
            } else { /* MigrationType.LEAVE */
              migrateKetamaEntireLEAVE(node); /* change leaving hpoint. leaving => leaved */
            }
            config.removeNode(node);
            alterNodes.remove(node);
          }
        /* ENABLE_MIGRATION end */
        } else {
          removeHash(node, ketamaNodes);
        }
        try {
          node.closeChannel();
        } catch (IOException e) {
          getLogger().error(
                  "Failed to closeChannel the node : " + node);
        }
      }
    } finally {
      /* ENABLE_MIGRATION if */
      if (migrationInProgress && (alterNodes.isEmpty() || existNodes.isEmpty())) {
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

  private void insertHash(MemcachedNode node, TreeMap<Long, SortedSet<MemcachedNode>> continuum) {
    config.insertNode(node);
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> nodeSet = continuum.get(k);
        if (nodeSet == null) {
          nodeSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
          continuum.put(k, nodeSet);
        }
        nodeSet.add(node);
      }
    }
  }

  private void removeHash(MemcachedNode node, TreeMap<Long, SortedSet<MemcachedNode>> continuum) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> nodeSet = continuum.get(k);
        assert nodeSet != null;
        nodeSet.remove(node);
        if (nodeSet.size() == 0) {
          continuum.remove(k);
        }
      }
    }
    config.removeNode(node);
  }

  /* ENABLE_MIGRATION if */
  public void prepareMigration(Collection<MemcachedNode> toAlter, MigrationType type) {
    getLogger().info("Prepare ketama info for migration. type=" + type);
    clearMigration();
    assert type != MigrationType.UNKNOWN;
    migrationType = type;
    migrationInProgress = true;
    /* prepare alterNodes, existNodes, ketamaAlterNodes */
    for (MemcachedNode node : toAlter) {
      alterNodes.add(node);
      if (migrationType == MigrationType.JOIN) {
        insertHash(node, ketamaAlterNodes);
      }
    }
    for (MemcachedNode node : allNodes) {
      if (!alterNodes.contains(node)) {
        existNodes.add(node);
      }
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

  /* remove all hpoints of the joining node */
  private void removeKetamaJOIN(MemcachedNode node) {
    /* joining hpoints can be in ketamaNodes or ketamaAlterNodes. */
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        boolean removed;
        SortedSet<MemcachedNode> nodeSet = ketamaNodes.get(k);
        if (nodeSet != null) {
          removed = nodeSet.remove(node);
          if (nodeSet.isEmpty()) {
            ketamaNodes.remove(k);
          }
          if (removed) {
            continue;
          }
        }
        nodeSet = ketamaAlterNodes.get(k);
        assert nodeSet != null;
        removed = nodeSet.remove(node);
        if (nodeSet.isEmpty()) {
          ketamaAlterNodes.remove(k);
        }
        assert removed;
      }
    }
  }

  /* remove joining hpoints from ketamaAlterNodes and add to ketamaNodes. */
  private void migrateKetamaEntireJOIN(MemcachedNode node) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long hpoint = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> alterSet = ketamaAlterNodes.get(hpoint);
        if (alterSet == null) {
          continue; /* already joined hpoint */
        }
        if (alterSet.remove(node)) { /* remove joining hpoint */
          continue;
        }
        if (alterSet.size() == 0) {
          ketamaAlterNodes.remove(hpoint);
        }
        SortedSet<MemcachedNode> existSet = ketamaNodes.get(hpoint);
        if (existSet == null) {
          existSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
          ketamaNodes.put(hpoint, existSet);
        }
        existSet.add(node); /* joining => joined */
      }
    }
  }

  /* remove leaving hpoints from ketamaNodes. */
  private void migrateKetamaEntireLEAVE(MemcachedNode node) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedNode> nodeSet = ketamaNodes.get(k);
        if (nodeSet == null) {
          continue; /* already leaved hpoint */
        }
        nodeSet.remove(node); /* leaving => leaved */
        if (nodeSet.size() == 0) {
          ketamaNodes.remove(k);
        }
      }
    }
  }

  private void migrateKetamaPartialJOIN(Map<Long, SortedSet<MemcachedNode>> range) {
    Iterator<Map.Entry<Long, SortedSet<MemcachedNode>>> itr = range.entrySet().iterator();
    Map.Entry<Long, SortedSet<MemcachedNode>> entry;
    Long hpoint;
    SortedSet<MemcachedNode> alterSet, existSet;
    while (itr.hasNext()) {
      entry = itr.next();
      hpoint = entry.getKey();
      alterSet = entry.getValue();
      assert alterSet != null;
      existSet = ketamaNodes.get(hpoint);
      if (existSet == null) {
        existSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
        ketamaNodes.put(hpoint, existSet);
      }
      existSet.addAll(alterSet); /* joining => joined */
      itr.remove();
    }
  }

  private void migrateKetamaPartialLEAVE(Map<Long, SortedSet<MemcachedNode>> range) {
    Iterator<Map.Entry<Long, SortedSet<MemcachedNode>>> itr = range.entrySet().iterator();
    Iterator<MemcachedNode> nodeItr;
    Map.Entry<Long, SortedSet<MemcachedNode>> entry;
    Long hpoint;
    SortedSet<MemcachedNode> allSet, leavingSet;
    while (itr.hasNext()) {
      entry = itr.next();
      hpoint = entry.getKey();
      allSet = entry.getValue();
      nodeItr = allSet.iterator();
      while (nodeItr.hasNext()) {
        MemcachedNode node = nodeItr.next();
        if (alterNodes.contains(node)) {
          nodeItr.remove(); /* leaving => leaved */
          leavingSet = ketamaAlterNodes.get(hpoint);
          if (leavingSet == null) {
            leavingSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
            ketamaAlterNodes.put(hpoint, leavingSet); /* for auto leave abort */
          }
          leavingSet.add(node);
          if (allSet.isEmpty()) {
            itr.remove();
          }
        }
      }
    }
  }

  /* check migrationLastPoint belongs to the (spoint, epoint) range. */
  private boolean needMigrateRange(Long spoint, Long epoint) {
    Long hash = migrationLastPoint;
    if (spoint == epoint) { /* full range */
      return spoint != hash;
    }
    if (spoint < epoint) {
      if (spoint < hash && hash < epoint) {
        return true;
      }
    } else { /* spoint > epoint */
      if (spoint < hash || hash < epoint) {
        return true;
      }
    }
    return false;
  }

  private void migrateJOIN(Long spoint, Long epoint) {
    lock.lock();
    try {
      spoint = (migrationLastPoint == -1 ? spoint : migrationLastPoint);
      NavigableMap<Long, SortedSet<MemcachedNode>> targetRange;
      if (spoint <= epoint) {
        targetRange = ketamaAlterNodes.subMap(spoint, true, epoint, true);
        migrateKetamaPartialJOIN(targetRange);
      } else {
        targetRange = ketamaAlterNodes.subMap(spoint, true, ketamaAlterNodes.lastKey(), true);
        migrateKetamaPartialJOIN(targetRange);
        targetRange = ketamaAlterNodes.subMap(0L, true, epoint, true);
        migrateKetamaPartialJOIN(targetRange);
      }
      migrationLastPoint = epoint;
    } finally {
      lock.unlock();
    }
    getLogger().info("Applied migration completion range. spoint=" + spoint + ", epoint=" + epoint);
  }

  private void migrateLEAVE(Long spoint, Long epoint) {
    lock.lock();
    try {
      spoint = (migrationLastPoint == -1 ? spoint : migrationLastPoint);
      NavigableMap<Long, SortedSet<MemcachedNode>> migratedRange;
      if (spoint < epoint) {
        migratedRange = ketamaNodes.subMap(spoint, true, epoint, true);
        migrateKetamaPartialLEAVE(migratedRange);
      } else {
        migratedRange = ketamaNodes.subMap(spoint, true, ketamaNodes.lastKey(), true);
        migrateKetamaPartialLEAVE(migratedRange);
        migratedRange = ketamaNodes.subMap(0L, true, epoint, true);
        migrateKetamaPartialLEAVE(migratedRange);
      }
      migrationLastPoint = spoint;
    } finally {
      lock.unlock();
    }
    getLogger().info("Applied migration completion range. spoint=" + spoint + ", epoint=" + epoint);
  }

  public boolean updateMigration(Long spoint, Long epoint) {
    if (migrationInProgress == false) {
      return true;
    }
    if (migrationLastPoint != -1) { /* first migrated ? */
      /* skip if the range is already migrated. */
      if (!needMigrateRange(spoint, epoint)) {
        return true;
      }
    }
    if (migrationType == MigrationType.JOIN) {
      migrateJOIN(spoint, epoint);
    } else {
      migrateLEAVE(spoint, epoint);
    }
    return true;
  }

  private void automaticJoinCompletion(MemcachedNode mine) {
    getLogger().info("Started automatic join completion. node=" + mine.getNodeName());
    ArrayList<Long> hpoints = new ArrayList<Long>();

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(mine, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        hpoints.add(k);
      }
    }

    SortedSet<MemcachedNode> existSet;
    SortedSet<MemcachedNode> joinSet;
    Long existPoint, joinPoint;
    boolean visible;
    for (Long currPoint : hpoints) {
      existSet = ketamaNodes.get(currPoint);
      assert existSet != null;
      visible = existSet.first() == mine;

      /* remove currPoint */
      existSet.remove(mine);
      if (existSet.size() == 0) {
        ketamaNodes.remove(currPoint);
      }

      /* is visible EXIST hpoint? */
      if (!visible) {
        /* it's a hidden hpoint. (<EXIST>, <JOIN>, <EXIST (mine)>) */
        continue; /* skip */
      }

      /* move the hidden joining hpoints on currPoint */
      if (ketamaAlterNodes.containsKey(currPoint)) {
        joinSet = ketamaAlterNodes.get(currPoint);
        existSet.addAll(joinSet); /* joining => joined */
        ketamaAlterNodes.remove(currPoint); /* remove all joining hpoint */
      }

      /* move all joining hpoints (existPoint ~ currPoint) range */
      joinPoint = ketamaAlterNodes.lowerKey(currPoint);
      existPoint = ketamaNodes.lowerKey(currPoint);
      if (existPoint == null) {
        existPoint = ketamaNodes.lastKey(); /* will be existPoint > currPoint */
      }

      if (existPoint > currPoint) {
        /* move [0 ~ joinPoint] range */
        while (joinPoint != null && 0L <= joinPoint) {
          ketamaNodes.put(joinPoint, ketamaAlterNodes.get(joinPoint)); /* joining => joined */
          ketamaAlterNodes.remove(joinPoint); /* remove all joining hpoint */
          joinPoint = ketamaAlterNodes.lowerKey(joinPoint);
        }
        joinPoint = ketamaAlterNodes.lastKey();
      }
      /* move (existPoint ~ joinPoint] range */
      while (joinPoint != null && existPoint < joinPoint) {
        ketamaNodes.put(joinPoint, ketamaAlterNodes.get(joinPoint)); /* joining => joined */
        ketamaAlterNodes.remove(joinPoint); /* remove all joining hpoint */
        joinPoint = ketamaAlterNodes.lowerKey(joinPoint);
      }

      /* move the hidden joining hpoints on existPoint */
      if (existPoint.equals(joinPoint)) {
        /*    <JOIN>, <EXIST>, <JOIN>, <JOIN>
         * => <JOIN>, <EXIST>, <EXIST>, <EXIST>
         */
        existSet = ketamaNodes.get(joinPoint);
        joinSet = ketamaAlterNodes.get(joinPoint);
        SortedSet<MemcachedNode> hiddenJoinSet = joinSet.tailSet(existSet.last());
        existSet.addAll(hiddenJoinSet);
        joinSet.removeAll(hiddenJoinSet);
        if (joinSet.isEmpty()) {
          ketamaAlterNodes.remove(joinPoint);
        }
      }
    }
    config.removeNode(mine);
    getLogger().info("Completed automatic join completion. node=" + mine.getNodeName());
  }

  private Long findMigrationBasePointLEAVE(Long hpoint) {
    Long key = ketamaNodes.lowerKey(hpoint);
    assert key != null;
    while (key >= 0L) {
      for (MemcachedNode node : ketamaNodes.get(key)) {
        if (existNodes.contains(node)) {
          return key;
        }
      }
      key = ketamaNodes.lowerKey(key);
    }
    assert key != null && key != -1L;
    return key;
  }

  private void automaticLeaveAbort(MemcachedNode mine) {
    if (migrationLastPoint == -1L) {
      return; /* nothing to abort */
    }
    MemcachedNode visibleExist = mine;
    migrationBasePoint = findMigrationBasePointLEAVE(0xFFFFFFFFL);
    for (MemcachedNode mn : ketamaNodes.get(migrationBasePoint)) { /* duplicate hpoints */
      if (existNodes.contains(mn)) {
        visibleExist = mn;
        break;
      }
    }
    if (visibleExist != mine) {
      return; /* not first exist */
    }

    getLogger().info("Started automatic leave abort. node=" + mine.getNodeName());

    /* find new base point */
    Long curBasePoint = migrationBasePoint;
    Long newBasePoint = migrationBasePoint;
    while (true) {
      newBasePoint = findMigrationBasePointLEAVE(newBasePoint);
      SortedSet<MemcachedNode> mg = ketamaNodes.get(newBasePoint);
      if (mg.size() == 1 && mg.contains(mine)) {
        continue; /* next exist point is also mine.. skip */
      }
      /* found new base point or all migration range is aborted */
      break;
    }

    /* abort (newBasePoint ~ curBasePoint) leaved hpoints */
    List<Long> aborts = new ArrayList<Long>();
    NavigableMap<Long, SortedSet<MemcachedNode>> abortRange =
        ketamaAlterNodes.subMap(newBasePoint, false, curBasePoint, false);
    for (Map.Entry<Long, SortedSet<MemcachedNode>> e : abortRange.entrySet()) {
      ketamaNodes.put(e.getKey(), e.getValue()); /* leaved => leaving */
      aborts.add(e.getKey());
    }
    for (Long key : aborts) {
      ketamaAlterNodes.remove(key); /* remove abort hpoints */
    }

    /* abort the hidden leaved hpoints on curBasePoint */
    SortedSet<MemcachedNode> leavedSet = ketamaAlterNodes.get(curBasePoint);
    if (leavedSet != null) {
      ketamaNodes.get(curBasePoint).addAll(leavedSet);
    }

    /* abort the hidden leaved hpoints on newBasePoint */
    leavedSet = ketamaAlterNodes.get(newBasePoint);
    if (leavedSet != null) {
      SortedSet<MemcachedNode> existSet = ketamaNodes.get(newBasePoint);
      SortedSet<MemcachedNode> hiddenLeavedSet = leavedSet.tailSet(existSet.last());
      if (!hiddenLeavedSet.isEmpty()) {
        existSet.addAll(hiddenLeavedSet); /* leaved => leaving */
        leavedSet.removeAll(hiddenLeavedSet);
        if (leavedSet.isEmpty()) {
          ketamaAlterNodes.remove(newBasePoint); /* remove leaved hpoint */
        }
      }
    }
    migrationBasePoint = -1L;
    migrationLastPoint = -1L;
    getLogger().info("Completed automatic leave abort. node=" + mine.getNodeName());
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
      hashVal &= 0xffffffffL; /* truncate to 32-bits */
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
