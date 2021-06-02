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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
  private Collection<MemcachedNode> existNodes;
  private Collection<MemcachedNode> alterNodes;

  private MigrationType migrationType;
  private Long migrationBasePoint;
  private Long migrationLastPoint;
  private boolean migrationInProgress;
  /* ENABLE_MIGRATION end */

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
    existNodes = new ArrayList<MemcachedNode>();
    alterNodes = new ArrayList<MemcachedNode>();
    ketamaAlterGroups = new TreeMap<Long, SortedSet<MemcachedReplicaGroup>>();
    alterGroups = new HashMap<String, MemcachedReplicaGroup>();
    existGroups = new HashMap<String, MemcachedReplicaGroup>();
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
      insertHash(group, ketamaGroups);
    }

    /* ketamaNodes.size() < numReps*nodes.size() : hash collision */
    assert ketamaGroups.size() <= (numReps * allGroups.size());
  }

  private ArcusReplKetamaNodeLocator(TreeMap<Long, SortedSet<MemcachedReplicaGroup>> kg,
                                     HashMap<String, MemcachedReplicaGroup> ag,
                                     Collection<MemcachedNode> an) {
    super();
    ketamaGroups = kg;
    allGroups = ag;
    allNodes = an;
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

  public Collection<MemcachedNode> getExistAll() {
    return Collections.unmodifiableCollection(existNodes);
  }

  public Map<String, MemcachedReplicaGroup> getAlterGroups() {
    return Collections.unmodifiableMap(alterGroups);
  }

  public Map<String, MemcachedReplicaGroup> getExistGroups() {
    return Collections.unmodifiableMap(existGroups);
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
        /* ENABLE_MIGRATION if */
        if (migrationInProgress) {
          if (existNodes.contains(node)) {
            existNodes.remove(node); /* existing node down */
          } else if (alterNodes.contains(node)) {
            alterNodes.remove(node); /* alter node down or leaving node has left */
            if (migrationType == MigrationType.JOIN) {
              alterGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node))
                      .deleteMemcachedNode(node);
            }
          }
          if (migrationType == MigrationType.LEAVE || existNodes.contains(node)) {
            allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node))
                .deleteMemcachedNode(node);
          }
        /* ENABLE_MIGRATION end */
        } else {
          MemcachedReplicaGroup mrg =
              allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
          mrg.deleteMemcachedNode(node);
        }

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
        MemcachedReplicaGroup mrg;
        /* ENABLE_MIGRATION if */
        if (migrationInProgress) { /* joining group has joined */
          if (alterNodes.contains(node)) {
            alterNodes.remove(node);
          }
          mrg = alterGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
          if (mrg == null) {
            continue;
          }
          migrateKetamaEntireJOIN(mrg);
          allGroups.put(mrg.getGroupName(), mrg);
          alterGroups.remove(mrg.getGroupName());
        /* ENABLE_MIGRATION end */
        } else {
          mrg = allGroups.get(MemcachedReplicaGroup.getGroupNameFromNode(node));
          if (mrg == null) {
            mrg = new MemcachedReplicaGroupImpl(node);
            getLogger().info("new memcached replica group added %s", mrg.getGroupName());
            allGroups.put(mrg.getGroupName(), mrg);
            insertHash(mrg, ketamaGroups);
          } else {
            mrg.setMemcachedNode(node);
          }
        }
      }

      // Delete empty group
      List<MemcachedReplicaGroup> toDeleteGroup = new ArrayList<MemcachedReplicaGroup>();

      for (Map.Entry<String, MemcachedReplicaGroup> entry : allGroups.entrySet()) {
        MemcachedReplicaGroup group = entry.getValue();
        if (group.isEmptyGroup()) {
          toDeleteGroup.add(group);
        }
      }

      /* ENABLE_MIGRATION if */
      if (migrationInProgress && migrationType == MigrationType.JOIN) { /* down joining group */
        for (Map.Entry<String, MemcachedReplicaGroup> entry : alterGroups.entrySet()) {
          MemcachedReplicaGroup group = entry.getValue();
          if (group.isEmptyGroup()) {
            toDeleteGroup.add(group);
          }
        }
      }
      /* ENABLE_MIGRATION end */

      for (MemcachedReplicaGroup group : toDeleteGroup) {
        String groupName = group.getGroupName();
        getLogger().info("old memcached replica group removed %s", groupName);
        allGroups.remove(groupName);
        /* ENABLE_MIGRATION if */
        if (migrationInProgress) {
          if (existGroups.containsKey(groupName)) {
            /* existing group down */
            if (migrationType == MigrationType.JOIN) {
              automaticJoinCompletion(group);
            } else { /* MigrationType.LEAVE */
              if (existGroups.size() > 1) {
                automaticLeaveAbort(group);
              }
              removeHash(group, ketamaGroups);
              existGroups.remove(groupName);
            }
          } else if (alterGroups.containsKey(groupName)) {
            /* alter node down or leaving node has left */
            if (migrationType == MigrationType.JOIN) {
              removeKetamaJOIN(group); /* remove down joining hpoint */
            } else { /* MigrationType.LEAVE */
              migrateKetamaEntireLEAVE(group); /* change leaving hpoint. leaving => leaved */
            }
            alterGroups.remove(groupName);
          }
        /* ENABLE_MIGRATION end */
        } else {
          removeHash(group, ketamaGroups);
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

  public void switchoverReplGroup(MemcachedReplicaGroup group) {
    lock.lock();
    group.changeRole();
    lock.unlock();
  }

  private Long getKetamaHashPoint(byte[] digest, int h) {
    return ((long) (digest[3 + h * 4] & 0xFF) << 24)
         | ((long) (digest[2 + h * 4] & 0xFF) << 16)
         | ((long) (digest[1 + h * 4] & 0xFF) << 8)
         | (digest[h * 4] & 0xFF);
  }

  private void insertHash(MemcachedReplicaGroup group,
                          TreeMap<Long, SortedSet<MemcachedReplicaGroup>> continuum) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> nodeSet = continuum.get(k);
        if (nodeSet == null) {
          nodeSet = new TreeSet<MemcachedReplicaGroup>(
              new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
          continuum.put(k, nodeSet);
        }
        nodeSet.add(group);
      }
    }
  }

  private void removeHash(MemcachedReplicaGroup group,
                          TreeMap<Long, SortedSet<MemcachedReplicaGroup>> continuum) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> nodeSet = continuum.get(k);
        nodeSet.remove(group);
        if (nodeSet.size() == 0) {
          continuum.remove(k);
        }
      }
    }
  }

  /* ENABLE_MIGRATION if */
  public void prepareMigration(Collection<MemcachedNode> toAlter, MigrationType type) {
    getLogger().info("Prepare ketama info for migration. type=" + type);
    clearMigration();
    assert type != MigrationType.UNKNOWN;
    migrationType = type;
    migrationInProgress = true;
    MemcachedReplicaGroup mrg;
    for (MemcachedNode node : toAlter) {
      alterNodes.add(node);
      String groupName = MemcachedReplicaGroup.getGroupNameFromNode(node);
      if (type == MigrationType.JOIN) {
        mrg = alterGroups.get(groupName);
        if (mrg == null) {
          mrg = new MemcachedReplicaGroupImpl(node);
          alterGroups.put(groupName, mrg); /* add joining group to alterGroups */
          insertHash(mrg, ketamaAlterGroups);
        } else {
          mrg.setMemcachedNode(node); /* add joining slave node */
        }
      } else { /* MigrationType.LEAVE */
        mrg = allGroups.get(groupName);
        assert (mrg != null);
        alterGroups.put(groupName, mrg); /* add leaving group to alterGroups */
      }
    }
    for (String groupName : allGroups.keySet()) {
      if (!alterGroups.containsKey(groupName)) {
        mrg = allGroups.get(groupName);
        existGroups.put(groupName, mrg);
        existNodes.add(mrg.getMasterNode());
        existNodes.addAll(mrg.getSlaveNodes());
      }
    }
  }

  private void clearMigration() {
    migrationType = MigrationType.UNKNOWN;
    migrationBasePoint = -1L;
    migrationLastPoint = -1L;
    existNodes.clear();
    alterNodes.clear();
    existGroups.clear();
    alterGroups.clear();
    ketamaAlterGroups.clear();
    migrationInProgress = false;
  }

  /* remove all hpoints of the joining group */
  private void removeKetamaJOIN(MemcachedReplicaGroup group) {
    /* joining hpoints can be in ketamaGroups or ketamaAlterGroups. */
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        boolean removed;
        SortedSet<MemcachedReplicaGroup> groupSet = ketamaGroups.get(k);
        if (groupSet != null) {
          removed = groupSet.remove(group);
          if (groupSet.isEmpty()) {
            ketamaGroups.remove(k);
          }
          if (removed) {
            continue;
          }
        }
        groupSet = ketamaAlterGroups.get(k);
        assert groupSet != null;
        removed = groupSet.remove(group);
        if (groupSet.isEmpty()) {
          ketamaAlterGroups.remove(k);
        }
        assert removed;
      }
    }
  }

  /* remove joining hpoints from ketamaAlterGroups and add to ketamaGroups. */
  private void migrateKetamaEntireJOIN(MemcachedReplicaGroup group) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> alterSet = ketamaAlterGroups.get(k);
        if (alterSet == null) {
          continue; /* already joined hpoint */
        }
        if (!alterSet.remove(group)) { /* remove joining hpoint */
          continue;
        }
        if (alterSet.size() == 0) {
          ketamaAlterGroups.remove(k);
        }
        SortedSet<MemcachedReplicaGroup> existSet = ketamaGroups.get(k);
        if (existSet == null) {
          existSet = new TreeSet<MemcachedReplicaGroup>(
              new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
          ketamaGroups.put(k, existSet);
        }
        existSet.add(group); /* joining => joined */
      }
    }
  }

  /* remove leaving hpoints from ketamaGroups. */
  private void migrateKetamaEntireLEAVE(MemcachedReplicaGroup group) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));
      for (int h = 0; h < 4; h++) {
        Long hpoint = getKetamaHashPoint(digest, h);
        SortedSet<MemcachedReplicaGroup> allGroups = ketamaGroups.get(hpoint);
        if (allGroups == null) {
          continue; /* already leaved hpoint */
        }
        allGroups.remove(group); /* leaving => leaved */
        if (allGroups.size() == 0) {
          ketamaGroups.remove(hpoint);
        }
      }
    }
  }

  private void migrateKetamaPartialJOIN(Map<Long, SortedSet<MemcachedReplicaGroup>> range) {
    Iterator<Map.Entry<Long, SortedSet<MemcachedReplicaGroup>>> itr = range.entrySet().iterator();
    Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> entry;
    Long hpoint;
    SortedSet<MemcachedReplicaGroup> alterSet, existSet;
    while (itr.hasNext()) {
      entry = itr.next();
      hpoint = entry.getKey();
      alterSet = entry.getValue();
      assert alterSet != null;
      existSet = ketamaGroups.get(hpoint);
      if (existSet == null) {
        existSet = new TreeSet<MemcachedReplicaGroup>(
            new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
        ketamaGroups.put(hpoint, existSet);
      }
      existSet.addAll(alterSet); /* joining => joined */
      itr.remove();
    }
  }

  private void migrateKetamaPartialLEAVE(Map<Long, SortedSet<MemcachedReplicaGroup>> range) {
    Iterator<Map.Entry<Long, SortedSet<MemcachedReplicaGroup>>> itr = range.entrySet().iterator();
    Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> entry;
    Iterator<MemcachedReplicaGroup> groupItr;
    Long hpoint;
    SortedSet<MemcachedReplicaGroup> allSet, leavedSet;
    while (itr.hasNext()) {
      entry = itr.next();
      hpoint = entry.getKey();
      allSet = entry.getValue();
      groupItr = allSet.iterator();
      while (groupItr.hasNext()) { /* find leaving group */
        MemcachedReplicaGroup group = groupItr.next();
        if (alterGroups.containsKey(group.getGroupName())) {
          groupItr.remove(); /* leaving => leaved */
          leavedSet = ketamaAlterGroups.get(hpoint);
          if (leavedSet == null) {
            leavedSet = new TreeSet<MemcachedReplicaGroup>(
                new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
            ketamaAlterGroups.put(hpoint, leavedSet); /* for auto leave abort */
          }
          leavedSet.add(group);
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
      NavigableMap<Long, SortedSet<MemcachedReplicaGroup>> targetRange;
      if (spoint <= epoint) {
        targetRange = ketamaAlterGroups.subMap(spoint, true, epoint, true);
        migrateKetamaPartialJOIN(targetRange);
      } else {
        targetRange = ketamaAlterGroups.subMap(spoint, true, ketamaAlterGroups.lastKey(), true);
        migrateKetamaPartialJOIN(targetRange);
        targetRange = ketamaAlterGroups.subMap(0L, true, epoint, true);
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
      epoint = (migrationLastPoint == -1 ? epoint : migrationLastPoint);
      NavigableMap<Long, SortedSet<MemcachedReplicaGroup>> migratedRange;
      if (spoint < epoint) {
        migratedRange = ketamaGroups.subMap(spoint, true, epoint, true);
        migrateKetamaPartialLEAVE(migratedRange);
      } else {
        migratedRange = ketamaGroups.subMap(spoint, true, ketamaGroups.lastKey(), true);
        migrateKetamaPartialLEAVE(migratedRange);
        migratedRange = ketamaGroups.subMap(0L, true, epoint, true);
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
      migrateLEAVE(spoint < 0xFFFFFFFFL ? spoint + 1 : 0, epoint);
    }
    return true;
  }

  private void automaticJoinCompletion(MemcachedReplicaGroup mine) {
    getLogger().info("Started automatic join completion. group=" + mine.getGroupName());
    ArrayList<Long> hpoints = new ArrayList<Long>();

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(mine, i));
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        hpoints.add(k);
      }
    }

    SortedSet<MemcachedReplicaGroup> existSet;
    SortedSet<MemcachedReplicaGroup> joinSet;
    Long existPoint, joinPoint;
    boolean visible;
    for (Long currPoint : hpoints) {
      existSet = ketamaGroups.get(currPoint);
      assert existSet != null;
      visible = existSet.first() == mine;

      /* remove currPoint */
      existSet.remove(mine);
      if (existSet.size() == 0) {
        ketamaGroups.remove(currPoint);
      }

      /* is visible EXIST hpoint? */
      if (!visible) {
        /* it's a hidden hpoint. (<EXIST>, <JOIN>, <EXIST (mine)>) */
        continue; /* skip */
      }

      /* move the hidden joining hpoints on currPoint */
      if (ketamaAlterGroups.containsKey(currPoint)) {
        joinSet = ketamaAlterGroups.get(currPoint);
        existSet.addAll(joinSet); /* joining => joined */
        ketamaAlterGroups.remove(currPoint); /* remove all joining hpoint */
      }

      /* move all joining hpoints (existPoint ~ currPoint) range */
      joinPoint = ketamaAlterGroups.lowerKey(currPoint);
      existPoint = ketamaGroups.lowerKey(currPoint);
      if (existPoint == null) {
        existPoint = ketamaGroups.lastKey(); /* will be existPoint > currPoint */
      }

      if (existPoint > currPoint) {
        /* move [0 ~ joinPoint] range */
        while (joinPoint != null && 0L <= joinPoint) {
          ketamaGroups.put(joinPoint, ketamaAlterGroups.get(joinPoint)); /* joining => joined */
          ketamaAlterGroups.remove(joinPoint); /* remove all joining hpoint */
          joinPoint = ketamaAlterGroups.lowerKey(joinPoint);
        }
        joinPoint = ketamaAlterGroups.lastKey();
      }
      /* move (existPoint ~ joinPoint] range */
      while (joinPoint != null && existPoint < joinPoint) {
        ketamaGroups.put(joinPoint, ketamaAlterGroups.get(joinPoint)); /* joining => joined */
        ketamaAlterGroups.remove(joinPoint); /* remove all joining hpoint */
        joinPoint = ketamaAlterGroups.lowerKey(joinPoint);
      }

      /* move the hidden joining hpoints on existPoint */
      if (existPoint.equals(joinPoint)) {
        /*    <JOIN>, <EXIST>, <JOIN>, <JOIN>
         * => <JOIN>, <EXIST>, <EXIST>, <EXIST>
         */
        existSet = ketamaGroups.get(joinPoint);
        joinSet = ketamaAlterGroups.get(joinPoint);
        SortedSet<MemcachedReplicaGroup> hiddenJoinSet = joinSet.tailSet(existSet.last());
        existSet.addAll(hiddenJoinSet);
        joinSet.removeAll(hiddenJoinSet);
        if (joinSet.isEmpty()) {
          ketamaAlterGroups.remove(joinPoint);
        }
      }
    }
    getLogger().info("Completed automatic join completion. group=" + mine.getGroupName());
  }

  private Long findMigrationBasePointLEAVE(Long hpoint) {
    Long key = ketamaGroups.lowerKey(hpoint);
    assert key != null;
    while (key >= 0L) {
      for (MemcachedReplicaGroup group : ketamaGroups.get(key)) {
        if (existGroups.containsKey(group.getGroupName())) {
          return key;
        }
      }
      key = ketamaGroups.lowerKey(key);
    }
    assert key != null && key != -1L;
    return key;
  }

  private void automaticLeaveAbort(MemcachedReplicaGroup mine) {
    if (migrationLastPoint == -1L) {
      return; /* nothing to abort */
    }
    MemcachedReplicaGroup visibleExist = mine;
    migrationBasePoint = findMigrationBasePointLEAVE(0xFFFFFFFFL);
    for (MemcachedReplicaGroup mg : ketamaGroups.get(migrationBasePoint)) { /* duplicate hpoints */
      if (existGroups.containsKey(mg.getGroupName())) {
        visibleExist = mg;
        break;
      }
    }
    if (visibleExist != mine) {
      return; /* not first exist */
    }

    getLogger().info("Started automatic leave abort. group=" + mine.getGroupName());

    /* find new base point */
    Long curBasePoint = migrationBasePoint;
    Long newBasePoint = migrationBasePoint;
    while (true) {
      newBasePoint = findMigrationBasePointLEAVE(newBasePoint);
      SortedSet<MemcachedReplicaGroup> mg = ketamaGroups.get(newBasePoint);
      if (mg.size() == 1 && mg.contains(mine)) {
        continue; /* next exist point is also mine.. skip */
      }
      /* found new base point or all migration range is aborted */
      break;
    }

    /* abort (newBasePoint ~ curBasePoint) leaved hpoints */
    List<Long> aborts = new ArrayList<Long>();
    NavigableMap<Long, SortedSet<MemcachedReplicaGroup>> abortRange =
            ketamaAlterGroups.subMap(newBasePoint, false, curBasePoint, false);
    for (Map.Entry<Long, SortedSet<MemcachedReplicaGroup>> e : abortRange.entrySet()) {
      ketamaGroups.put(e.getKey(), e.getValue()); /* leaved => leaving */
      aborts.add(e.getKey());
    }
    for (Long key : aborts) {
      ketamaAlterGroups.remove(key); /* remove abort hpoints */
    }

    /* abort the hidden leaved hpoints on curBasePoint */
    SortedSet<MemcachedReplicaGroup> leavedSet = ketamaAlterGroups.get(curBasePoint);
    if (leavedSet != null) {
      ketamaGroups.get(curBasePoint).addAll(leavedSet);
    }

    /* abort the hidden leaved hpoints on newBasePoint */
    leavedSet = ketamaAlterGroups.get(newBasePoint);
    if (leavedSet != null) {
      SortedSet<MemcachedReplicaGroup> existSet = ketamaGroups.get(newBasePoint);
      SortedSet<MemcachedReplicaGroup> hiddenLeavedSet = leavedSet.tailSet(existSet.last());
      if (!hiddenLeavedSet.isEmpty()) {
        existSet.addAll(hiddenLeavedSet); /* leaved => leaving */
        leavedSet.removeAll(hiddenLeavedSet);
        if (leavedSet.isEmpty()) {
          ketamaAlterGroups.remove(newBasePoint); /* remove leaved hpoint */
        }
      }
    }
    migrationBasePoint = -1L;
    migrationLastPoint = -1L;
    getLogger().info("Completed automatic leave abort. group=" + mine.getGroupName());
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
