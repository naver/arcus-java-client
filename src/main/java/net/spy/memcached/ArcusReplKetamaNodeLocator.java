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

  private final HashAlgorithm hashAlg = HashAlgorithm.KETAMA_HASH;
  private final ArcusReplKetamaNodeLocatorConfiguration config;

  private final Lock lock = new ReentrantLock();

  public ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes) {
    // This configuration class is aware that InetSocketAddress is really
    // ArcusReplNodeAddress.  Its getKeyForNode uses the group name, instead
    // of the socket address.
    this(nodes, new ArcusReplKetamaNodeLocatorConfiguration());
  }

  private ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes,
                                     ArcusReplKetamaNodeLocatorConfiguration conf) {
    super();
    allNodes = nodes;
    ketamaGroups = new TreeMap<Long, SortedSet<MemcachedReplicaGroup>>();
    allGroups = new HashMap<String, MemcachedReplicaGroup>();

    config = conf;

    // create all memcached replica group
    for (MemcachedNode node : nodes) {
      MemcachedReplicaGroup mrg =
              allGroups.get(MemcachedReplicaGroup.getGroupNameForNode(node));

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
      updateHash(group, false);
    }

    /* ketamaNodes.size() < numReps*nodes.size() : hash collision */
    assert ketamaGroups.size() <= (numReps * allGroups.size());
  }

  private ArcusReplKetamaNodeLocator(TreeMap<Long, SortedSet<MemcachedReplicaGroup>> kg,
                                     HashMap<String, MemcachedReplicaGroup> ag,
                                     Collection<MemcachedNode> an,
                                     ArcusReplKetamaNodeLocatorConfiguration conf) {
    super();
    ketamaGroups = kg;
    allGroups = ag;
    allNodes = an;
    config = conf;
  }

  public Collection<MemcachedNode> getAll() {
    return Collections.unmodifiableCollection(allNodes);
  }

  public Map<String, MemcachedReplicaGroup> getAllGroups() {
    return Collections.unmodifiableMap(allGroups);
  }

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
        rv = rg.getNodeForReplicaPick(pick);
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

    return new ArcusReplKetamaNodeLocator(smg, ag, an, config);
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
        MemcachedReplicaGroup mrg =
                allGroups.get(MemcachedReplicaGroup.getGroupNameForNode(node));
        mrg.deleteMemcachedNode(node);

        try {
          node.getSk().attach(null);
          node.shutdown();
        } catch (IOException e) {
          getLogger().error("Failed to shutdown the node : " + node.toString());
          node.setSk(null);
        }
      }

      // Change role
      for (MemcachedReplicaGroup g : changeRoleGroups) {
        g.changeRole();
      }

      // Add memcached nodes.
      for (MemcachedNode node : toAttach) {
        allNodes.add(node);
        MemcachedReplicaGroup mrg =
                allGroups.get(MemcachedReplicaGroup.getGroupNameForNode(node));
        if (mrg == null) {
          mrg = new MemcachedReplicaGroupImpl(node);
          getLogger().info("new memcached replica group added %s", mrg.getGroupName());
          allGroups.put(mrg.getGroupName(), mrg);
          updateHash(mrg, false);
        } else {
          mrg.setMemcachedNode(node);
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

      for (MemcachedReplicaGroup group : toDeleteGroup) {
        getLogger().info("old memcached replica group removed %s", group.getGroupName());
        allGroups.remove(group.getGroupName());
        updateHash(group, true);
      }
    } finally {
      lock.unlock();
    }
  }

  public void switchoverReplGroup(MemcachedReplicaGroup group) {
    lock.lock();
    group.changeRole();
    lock.unlock();
  }

  private void updateHash(MemcachedReplicaGroup group, boolean remove) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));

      for (int h = 0; h < 4; h++) {
        Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                | (digest[h * 4] & 0xFF);

        SortedSet<MemcachedReplicaGroup> nodeSet = ketamaGroups.get(k);
        if (remove) {
          nodeSet.remove(group);
          if (nodeSet.size() == 0) {
            ketamaGroups.remove(k);
          }
        } else {
          if (nodeSet == null) {
            nodeSet = new TreeSet<MemcachedReplicaGroup>(
                    new ArcusReplKetamaNodeLocatorConfiguration.MemcachedReplicaGroupComparator());
            ketamaGroups.put(k, nodeSet);
          }
          nodeSet.add(group);
        }
      }
    }
  }

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
