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
      updateHash(node, false);
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
            hash = nodeHash.longValue();
          }
        }
        rv = ketamaNodes.get(hash).first();
      }
    } catch (RuntimeException e) {
      throw e;
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
    } catch (RuntimeException e) {
      throw e;
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
        updateHash(node, false);
      }

      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        allNodes.remove(node);
        updateHash(node, true);

        try {
          node.getSk().attach(null);
          node.shutdown();
        } catch (IOException e) {
          getLogger().error(
                  "Failed to shutdown the node : " + node.toString());
          node.setSk(null);
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } finally {
      lock.unlock();
    }
  }

  private void updateHash(MemcachedNode node, boolean remove) {
    if (!remove) {
      config.insertNode(node);
    }

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {

      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                | (digest[h * 4] & 0xFF);

        SortedSet<MemcachedNode> nodeSet = ketamaNodes.get(k);
        if (remove) {
          assert nodeSet != null;
          nodeSet.remove(node);
          if (nodeSet.size() == 0) {
            ketamaNodes.remove(k);
          }
        } else {
          if (nodeSet == null) {
            nodeSet = new TreeSet<MemcachedNode>(config.new NodeNameComparator());
            ketamaNodes.put(k, nodeSet);
          }
          nodeSet.add(node);
        }
      }
    }

    if (remove) {
      config.removeNode(node);
    }
  }

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
