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
package net.spy.memcached;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;
import net.spy.memcached.util.KetamaNodeLocatorConfiguration;

/**
 * This is an implementation of the Ketama consistent hash strategy from
 * last.fm.  This implementation may not be compatible with libketama as
 * hashing is considered separate from node location.
 *
 * Note that this implementation does not currently supported weighted nodes.
 *
 * @see <a href="http://www.last.fm/user/RJ/journal/2007/04/10/392555/">RJ's blog post</a>
 */
public final class KetamaNodeLocator extends SpyObject implements NodeLocator {

  private final TreeMap<Long, MemcachedNode> ketamaNodes;
  private final Collection<MemcachedNode> allNodes;

  private final HashAlgorithm hashAlg;
  private final KetamaNodeLocatorConfiguration config;

  public KetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
    this(nodes, alg, new DefaultKetamaNodeLocatorConfiguration());
  }

  public KetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg,
                           KetamaNodeLocatorConfiguration conf) {
    super();
    allNodes = nodes;
    hashAlg = alg;
    ketamaNodes = new TreeMap<>();
    config = conf;

    int numReps = config.getNodeRepetitions();
    // Ketama does some special work with md5 where it reuses chunks.
    if (alg == HashAlgorithm.KETAMA_HASH) {
      for (MemcachedNode node : nodes) {
        for (int i = 0; i < numReps / 4; i++) {
          byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
          for (int h = 0; h < 4; h++) {
            Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                    | (digest[h * 4] & 0xFF);
            ketamaNodes.put(k, node);
          }
        }
      }
    } else {
      for (MemcachedNode node : nodes) {
        for (int i = 0; i < numReps; i++) {
          ketamaNodes.put(hashAlg.hash(config.getKeyForNode(node, i)), node);
        }
      }
    }
    assert ketamaNodes.size() == numReps * nodes.size();
  }

  private KetamaNodeLocator(TreeMap<Long, MemcachedNode> smn,
                            Collection<MemcachedNode> an, HashAlgorithm alg,
                            KetamaNodeLocatorConfiguration conf) {
    super();
    ketamaNodes = smn;
    allNodes = an;
    hashAlg = alg;
    config = conf;
  }

  public Collection<MemcachedNode> getAll() {
    return allNodes;
  }

  public MemcachedNode getPrimary(final String k) {
    MemcachedNode rv = getNodeForKey(hashAlg.hash(k));
    assert rv != null : "Found no node for key " + k;
    return rv;
  }

  MemcachedNode getNodeForKey(long hash) {
    final MemcachedNode rv;
    if (!ketamaNodes.containsKey(hash)) {
      Long nodeHash = ketamaNodes.ceilingKey(hash);
      if (nodeHash == null) {
        hash = ketamaNodes.firstKey();
      } else {
        hash = nodeHash.longValue();
      }
    }
    rv = ketamaNodes.get(hash);
    return rv;
  }

  public Iterator<MemcachedNode> getSequence(String k) {
    return new KetamaIterator(k, allNodes.size());
  }

  public NodeLocator getReadonlyCopy() {
    TreeMap<Long, MemcachedNode> smn = new TreeMap<>(
            ketamaNodes);
    Collection<MemcachedNode> an =
            new ArrayList<>(allNodes.size());

    // Rewrite the values a copy of the map.
    for (Map.Entry<Long, MemcachedNode> me : smn.entrySet()) {
      me.setValue(new MemcachedNodeROImpl(me.getValue()));
    }
    // Copy the allNodes collection.
    for (MemcachedNode n : allNodes) {
      an.add(new MemcachedNodeROImpl(n));
    }

    return new KetamaNodeLocator(smn, an, hashAlg, config);
  }

  public void update(Collection<MemcachedNode> toAttach, Collection<MemcachedNode> toDelete) {
    throw new UnsupportedOperationException("update not supported");
  }

  public Collection<MemcachedNode> getDelayedClosingNodes() {
    return new HashSet<MemcachedNode>();
  }

  public void removeDelayedClosingNodes(Collection<MemcachedNode> closedNodes) {
    // do NOT throw UnsupportedOperationException here for test codes.
  }

  public SortedMap<Long, MemcachedNode> getKetamaNodes() {
    return Collections.unmodifiableSortedMap(ketamaNodes);
  }

  /* ENABLE_MIGRATION if */
  public Collection<MemcachedNode> getAlterAll() {
    return new ArrayList<>();
  }

  public MemcachedNode getAlterNode(SocketAddress sa) {
    return null;
  }

  public MemcachedNode getOwnerNode(String owner, MigrationType mgType) {
    return null;
  }

  public void updateAlter(Collection<MemcachedNode> toAttach,
                          Collection<MemcachedNode> toDelete) {
    throw new UnsupportedOperationException("updateAlter not supported");
  }

  public void prepareMigration(Collection<MemcachedNode> toAlter, MigrationType type) {
    throw new UnsupportedOperationException("prepareMigration not supported");
  }

  public void updateMigration(Long spoint, Long epoint) {
    throw new UnsupportedOperationException("updateMigration not supported");
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
      // this.calculateHash(Integer.toString(tries)+key).hashCode();
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
