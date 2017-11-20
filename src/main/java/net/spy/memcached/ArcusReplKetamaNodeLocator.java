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
/* ENABLE_REPLICATION if */
package net.spy.memcached;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.MigrationMode;
import net.spy.memcached.util.ArcusReplKetamaNodeLocatorConfiguration;

public class ArcusReplKetamaNodeLocator extends SpyObject implements NodeLocator {

  private TreeMap<Long, MemcachedReplicaGroup> ketamaGroups;
  private HashMap<String, MemcachedReplicaGroup> allGroups;
  private Collection<MemcachedNode> allNodes;

  /* ENABLE_MIGRATION if */
  private TreeMap<Long, MemcachedReplicaGroup> migrationKetamaGroups;
  private HashMap<String, MemcachedReplicaGroup> allExistGroups;
  private HashMap<String, MemcachedReplicaGroup> allAlterGroups;
  private HashMap<String, MemcachedReplicaGroup> allFailedExistGroups;
  private HashMap<String, MemcachedReplicaGroup> allFailedAlterGroups;
  private HashMap<String, List<Long>> allHashPoints;
  private Collection<MemcachedNode> allMigrationNodes;
  private List<String> sortedExistNames;
  private int migrationExecutionRound = 0;
  private MigrationMode migrationMode = MigrationMode.Init;
  private String prevExistNodeName = null;
  private int prevExistHSliceIndex = -1;
  /* ENABLE_MIGRATION end */

  private HashAlgorithm hashAlg;
  private ArcusReplKetamaNodeLocatorConfiguration config;

  Lock lock = new ReentrantLock();

  public ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
    // This configuration class is aware that InetSocketAddress is really
    // ArcusReplNodeAddress.  Its getKeyForNode uses the group name, instead
    // of the socket address.
    this(nodes, alg, new ArcusReplKetamaNodeLocatorConfiguration());
  }

  private ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg,
                                     ArcusReplKetamaNodeLocatorConfiguration conf) {
    super();
    allNodes = nodes;
    hashAlg = alg;
    ketamaGroups = new TreeMap<Long, MemcachedReplicaGroup>();
    allGroups = new HashMap<String, MemcachedReplicaGroup>();

    /* ENABLE_MIGRATION if */
    migrationKetamaGroups = new TreeMap<Long, MemcachedReplicaGroup>();
    allMigrationNodes = new ArrayList<MemcachedNode>();
    allExistGroups = new HashMap<String, MemcachedReplicaGroup>();
    allFailedExistGroups = new HashMap<String, MemcachedReplicaGroup>();
    allAlterGroups = new HashMap<String, MemcachedReplicaGroup>();
    allFailedAlterGroups = new HashMap<String, MemcachedReplicaGroup>();
    allHashPoints = new HashMap<String, List<Long>>();
    sortedExistNames = new ArrayList<String>();
    /* ENABLE_MIGRATION end */

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

      /* ENABLE_MIGRATION if */
      allHashPoints.put(mrg.getGroupName(), prepareHashPoint(mrg));
      /* ENABLE_MIGRATION end */
    }

    int numReps = config.getNodeRepetitions();
    for (MemcachedReplicaGroup group : allGroups.values()) {
      // Ketama does some special work with md5 where it reuses chunks.
      if (alg == HashAlgorithm.KETAMA_HASH) {
        updateHash(group, false);
      } else {
        for (int i = 0; i < numReps; i++) {
          ketamaGroups.put(
                  hashAlg.hash(config.getKeyForGroup(group, i)), group);
        }
      }
    }

    assert ketamaGroups.size() == (numReps * allGroups.size());
  }

  private ArcusReplKetamaNodeLocator(TreeMap<Long, MemcachedReplicaGroup> kg,
                                     HashMap<String, MemcachedReplicaGroup> ag, Collection<MemcachedNode> an,
                                     HashAlgorithm alg,
                                     ArcusReplKetamaNodeLocatorConfiguration conf) {
    super();
    ketamaGroups = kg;
    allGroups = ag;
    allNodes = an;
    hashAlg = alg;
    config = conf;
  }

  public Collection<MemcachedNode> getAll() {
    return allNodes;
  }

  public Map<String, MemcachedReplicaGroup> getAllGroups() {
    return allGroups;
  }

  /* ENABLE_MIGRATION if */
  public Map<String, MemcachedReplicaGroup> getAllMigrationGroups() {
    return allAlterGroups;
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
    MemcachedNode rv = getNodeForKey(hashAlg.hash(k), ReplicaPick.MASTER);
    assert rv != null : "Found no node for key" + k;
    return rv;
  }

  public MemcachedNode getPrimary(final String k, ReplicaPick pick) {
    MemcachedNode rv = getNodeForKey(hashAlg.hash(k), pick);
    assert rv != null : "Found no node for key" + k;
    return rv;
  }

  public long getMaxKey() {
    return ketamaGroups.lastKey();
  }

  private MemcachedNode getPrimaryNoLocking(final String k) {
    long hash = hashAlg.hash(k);
    ReplicaPick pick = ReplicaPick.MASTER;
    MemcachedReplicaGroup rg;
    MemcachedNode rv;

    try {
      if (!ketamaGroups.containsKey(hash)) {
        Long nodeHash = ketamaGroups.ceilingKey(hash);
        if (nodeHash == null) {
          hash = ketamaGroups.firstKey();
        } else {
          hash = nodeHash.longValue();
        }
        // Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
        // in a lot of places, so I'm doing this myself.
        /*
        SortedMap<Long, MemcachedNode> tailMap = ketamaNodes.tailMap(hash);
        if (tailMap.isEmpty()) {
          hash = ketamaNodes.firstKey();
        } else {
          hash = tailMap.firstKey();
        }
        */
      }
      /* ENABLE_MIGRATION if */
      if (allFailedExistGroups.isEmpty()) {
        rg = ketamaGroups.get(hash);
      } else {
        do {
          rg = ketamaGroups.get(hash);
          Long nodeHash = ketamaGroups.higherKey(hash);
          if (nodeHash == null) {
            hash = ketamaGroups.firstKey();
          } else {
            hash = nodeHash.longValue();
          }
        } while (allFailedExistGroups.containsKey(rg.getGroupName()));
      }
      /* else */
      /* rg = ketamaGroups.get(hash); /*
      /* ENABLE_MIGRATION end */
      // return a node (master / slave) for the replica pick request.
      rv = rg.getNodeForReplicaPick(pick);
    } catch (RuntimeException e) {
      throw e;
    }
    assert rv != null : "Found no node for key" + k;
    return rv;
  }

  private MemcachedNode getNodeForKey(long hash, ReplicaPick pick) {
    MemcachedReplicaGroup rg;
    MemcachedNode rv;

    lock.lock();
    try {
      if (!ketamaGroups.containsKey(hash)) {
        Long nodeHash = ketamaGroups.ceilingKey(hash);
        if (nodeHash == null) {
          hash = ketamaGroups.firstKey();
        } else {
          hash = nodeHash.longValue();
        }
        // Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
        // in a lot of places, so I'm doing this myself.
        /*
        SortedMap<Long, MemcachedNode> tailMap = ketamaNodes.tailMap(hash);
        if (tailMap.isEmpty()) {
          hash = ketamaNodes.firstKey();
        } else {
          hash = tailMap.firstKey();
        }
        */
      }
      /* ENABLE_MIGRATION if */
      if (allFailedExistGroups.isEmpty()) {
        rg = ketamaGroups.get(hash);
      } else {
        do {
          rg = ketamaGroups.get(hash);
          Long nodeHash = ketamaGroups.higherKey(hash);
          if (nodeHash == null) {
            hash = ketamaGroups.firstKey();
          } else {
            hash = nodeHash.longValue();
          }
        } while (allFailedExistGroups.containsKey(rg.getGroupName()));
      }
      /* else */
      /* rg = ketamaGroups.get(hash); /*
      /* ENABLE_MIGRATION end */
      // return a node (master / slave) for the replica pick request.
      rv = rg.getNodeForReplicaPick(pick);
    } catch (RuntimeException e) {
      throw e;
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
    TreeMap<Long, MemcachedReplicaGroup> smg =
            new TreeMap<Long, MemcachedReplicaGroup>(ketamaGroups);
    HashMap<String, MemcachedReplicaGroup> ag =
            new HashMap<String, MemcachedReplicaGroup>(allGroups.size());
    Collection<MemcachedNode> an = new ArrayList<MemcachedNode>(allNodes.size());

    lock.lock();
    try {
      // Rewrite the values a copy of the map
      for (Map.Entry<Long, MemcachedReplicaGroup> mge : smg.entrySet()) {
        mge.setValue(new MemcachedReplicaGroupROImpl(mge.getValue()));
      }
      // copy the allGroups collection.
      for (Map.Entry<String, MemcachedReplicaGroup> me : allGroups.entrySet()) {
        ag.put(me.getKey(), new MemcachedReplicaGroupROImpl(me.getValue()));
      }
      // copy the allNodes collection.
      for (MemcachedNode n : allNodes) {
        an.add(new MemcachedNodeROImpl(n));
      }
    } catch (RuntimeException e) {
      throw e;
    } finally {
      lock.unlock();
    }

    return new ArcusReplKetamaNodeLocator(smg, ag, an, hashAlg, config);
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
          /* ENABLE_MIGRATION if */
          if (migrationMode == MigrationMode.Init) {
            mrg = new MemcachedReplicaGroupImpl(node);
            getLogger().info("new memcached replica group added %s", mrg.getGroupName());
          } else {
            mrg = allAlterGroups.get(MemcachedReplicaGroup.getGroupNameForNode(node));
            if (mrg == null) {
              mrg = new MemcachedReplicaGroupImpl(node);
              getLogger().info("new memcached replica group added %s", mrg.getGroupName());
            } else {
              getLogger().info("altering memcached replica group moved %s", mrg.getGroupName());
            }
          }
          /* else */
          /*
          mrg = new MemcachedReplicaGroupImpl(node);
          getLogger().info("new memcached replica group added %s", mrg.getGroupName());
           */
          /* ENABLE_MIGRATION end */
          allGroups.put(mrg.getGroupName(), mrg);
          updateHash(mrg, false);
        } else {
          mrg.setMemcachedNode(node);
        }
        allHashPoints.put(mrg.getGroupName(), prepareHashPoint(mrg));
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
        /* ENABLE_MIGRATION if */
        if (migrationMode == MigrationMode.Init) {
          updateHash(group, true);
        } else if (migrationMode == MigrationMode.Join) {
          MemcachedReplicaGroup mrg = allFailedExistGroups.get(group.getGroupName());
          if (mrg == null) {
            allFailedExistGroups.put(group.getGroupName(), group);
          }
          allExistGroups.remove(group.getGroupName());
          sortedExistNames.remove(group.getGroupName());
          allHashPoints.remove(group.getGroupName());
          updateHash(group, true);
        } else {
          assert migrationMode == MigrationMode.Leave;

          if (allAlterGroups.containsKey(group.getGroupName())) {
            allHashPoints.remove(group.getGroupName());
            MemcachedReplicaGroup mrg = allFailedAlterGroups.get(group.getGroupName());
            if (mrg == null) {
              allFailedAlterGroups.put(group.getGroupName(), group);
              updateHash(group, true);
            }
          } else {
            if (allFailedAlterGroups.containsKey(group.getGroupName())) {
              /* do nothing */
            } else {
              MemcachedReplicaGroup mrg = allFailedExistGroups.get(group.getGroupName());
              if (mrg == null) {
                allFailedExistGroups.put(group.getGroupName(), group);
              }
            }
          }
        }
        /* else */
        /* updateHash(group, true); */
        /* ENABLE_MIGRATION end */
      }
    } catch (RuntimeException e) {
      throw e;
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

        if (remove) {
          /* ENABLE_MIGRATION if */
          /* auto complete */
          if (migrationMode == MigrationMode.Join) {
            if (!migrationKetamaGroups.isEmpty()) {
              Long prev_k = ketamaGroups.lowerKey(k);
              NavigableMap<Long, MemcachedReplicaGroup> sub;
              if (prev_k == null) {
                /* Ketama Hash point Tree map
                   |            | 0'th | ... | 159'th |              |
                   Migration Ketama Hash point Tree map
                   | alter 0'th |      | ... |        | alter 159'th |
                */
                prev_k = ketamaGroups.lastKey();
                NavigableMap<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
                sub = migrationKetamaGroups.subMap((long) 0, true, k, false);
                for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
                  temp.put(entry.getKey(), entry.getValue());
                }
                if (prev_k < migrationKetamaGroups.lastKey()) {
                  sub = migrationKetamaGroups.subMap(prev_k, false, migrationKetamaGroups.lastKey(), true);
                  for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
                    temp.put(entry.getKey(), entry.getValue());
                  }
                }
                sub = temp;
              } else {
                sub = migrationKetamaGroups.subMap(prev_k, false, k, false);
              }
              if (!sub.isEmpty()) {
                Map<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
                for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
                  temp.put(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<Long, MemcachedReplicaGroup> entry : temp.entrySet()) {
                  ketamaGroups.put(entry.getKey(), entry.getValue());
                  migrationKetamaGroups.remove(entry.getKey());
                }
              }
            }
          }

          /* ENABLE_MIGRATION end */

          ketamaGroups.remove(k);
        } else {
          ketamaGroups.put(k, group);
        }
      }
    }
  }

  /* ENABLE_MIGRATION if */
  public void cleanupMigration() {
    if (!allExistGroups.isEmpty()) {
      allExistGroups.clear();
    }
    if (!allAlterGroups.isEmpty()) {
      allAlterGroups.clear();
    }
    if (!allFailedExistGroups.isEmpty()) {
      lock.lock();
      for (Map.Entry<String, MemcachedReplicaGroup> group : allFailedExistGroups.entrySet()) {
        updateHash(group.getValue(), true);
      }
      lock.unlock();
      allFailedExistGroups.clear();
    }
    if (!allFailedAlterGroups.isEmpty()) {
      allFailedAlterGroups.clear();
    }
    if (!allMigrationNodes.isEmpty()) {
      allMigrationNodes.clear();
    }
    if (!migrationKetamaGroups.isEmpty()) {
      migrationKetamaGroups.clear();
    }
    if (!sortedExistNames.isEmpty()) {
      sortedExistNames.clear();
    }
    migrationExecutionRound = 0;
    prevExistNodeName = null;
    prevExistHSliceIndex = -1;
    migrationMode = MigrationMode.Init;

    getLogger().info("Cleanup Migration");
  }

  public MigrationMode getMigrationMode() {
    return migrationMode;
  }

  private void updateMigrationHash(MemcachedReplicaGroup group, boolean remove) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));

      for (int h = 0; h < 4; h++) {
        Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                | (digest[h * 4] & 0xFF);

        if (remove) {
          ketamaGroups.remove(k);
          migrationKetamaGroups.remove(k);
        } else {
          migrationKetamaGroups.put(k, group);
        }
      }
    }
  }

  public void updateMigration(Collection<MemcachedNode> toAttach,
                              Collection<MemcachedNode> toDelete,
                              MigrationMode mode,
                              Collection<MemcachedReplicaGroup> changeRoleGroups) {
    /* We must keep the following execution order
     * - first, remove nodes.
     * - second, change role.
     * - third, add nodes
     */
    if (allExistGroups.isEmpty()) {
      for (Map.Entry<String, MemcachedReplicaGroup> group : allGroups.entrySet()) {
        allExistGroups.put(group.getKey(), group.getValue());
        sortedExistNames.add(group.getKey());
      }
      Collections.sort(sortedExistNames);
    }

    lock.lock();
    try {
      migrationMode = mode;
      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        allMigrationNodes.remove(node);
        if (!allGroups.containsKey(MemcachedReplicaGroup.getGroupNameForNode(node))) {
          /* Because of the ZK event process order guarantee, it is failed node */
          MemcachedReplicaGroup mrg =
                  allAlterGroups.get(MemcachedReplicaGroup.getGroupNameForNode(node));
          mrg.deleteMemcachedNode(node);
          if (mode == MigrationMode.Join) {
            try {
              node.getSk().attach(null);
              node.shutdown();
            } catch (IOException e) {
              getLogger().error("Failed to shutdown the node : " + node.toString());
              node.setSk(null);
            }
          }
        }
      }

      // Change role
      for (MemcachedReplicaGroup g : changeRoleGroups) {
        g.changeRole();
      }

      // Add memcached nodes.
      for (MemcachedNode node : toAttach) {
        if (node == null)
          continue;
        allMigrationNodes.add(node);
        MemcachedReplicaGroup mrg =
                allAlterGroups.get(MemcachedReplicaGroup.getGroupNameForNode(node));
        if (mrg == null) {
          mrg = new MemcachedReplicaGroupImpl(node);
          getLogger().info("new memcached migration replica group added %s", mrg.getGroupName());
          allAlterGroups.put(mrg.getGroupName(), mrg);
          if (mode == MigrationMode.Join) {
            updateMigrationHash(mrg, false);
          }
        } else {
          mrg.setMemcachedNode(node);
        }
        allHashPoints.put(mrg.getGroupName(), prepareHashPoint(mrg));

        /* make exist group list */
        if (mode == MigrationMode.Leave) {
          MemcachedReplicaGroup group =
                  allExistGroups.get(MemcachedReplicaGroup.getGroupNameForNode(node));
          if (group != null) {
            allExistGroups.remove(group.getGroupName());
            sortedExistNames.remove(group.getGroupName());
          }
        }
      }

      // Delete empty group
      List<MemcachedReplicaGroup> toDeleteGroup = new ArrayList<MemcachedReplicaGroup>();

      for (Map.Entry<String, MemcachedReplicaGroup> entry : allAlterGroups.entrySet()) {
        MemcachedReplicaGroup group = entry.getValue();
        if (group.isEmptyGroup()) {
          toDeleteGroup.add(group);
        }
      }

      for (MemcachedReplicaGroup group : toDeleteGroup) {
        getLogger().info("old memcached migration replica group removed %s", group.getGroupName());
        allAlterGroups.remove(group.getGroupName());
        allHashPoints.remove(group.getGroupName());
        if (mode == MigrationMode.Join) {
          updateMigrationHash(group, true);
        } else {
          updateHash(group, true);
        }
        MemcachedReplicaGroup mrg = allFailedAlterGroups.get(group.getGroupName());
        if (mrg == null) {
          allFailedAlterGroups.put(group.getGroupName(), group);
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } finally {
      lock.unlock();
    }
  }

  public void reflectMigratedHash(List<String> mgStateList) {
    /* migrations znode : 0^g0^110^g2^120, 0^g0^160^g2^148, 1^g0^2^g2^120 */
    int executeRound;
    int existMoveSlice;
    int alterMoveSlice;
    String existName;
    String alterName;
    // DEBUG code
    int count = 0;

    if (migrationMode == MigrationMode.Init) {
      return;
    }

    for (String migrations : mgStateList) {
      getLogger().debug("Reflect migrations znode[%d]: %s, mode = " + migrationMode, count++, migrations);
      String[] splitedResponse = migrations.split("\\^");
      assert splitedResponse.length == 5 : "migrations znode format must Rond^exName^compCnt^alName^hslice";

      executeRound = Integer.valueOf(splitedResponse[0]);
      existName = splitedResponse[1];
      existMoveSlice = Integer.valueOf(splitedResponse[2]);
      alterName = splitedResponse[3];
      alterMoveSlice = Integer.valueOf(splitedResponse[4]);

      lock.lock();
      if (executeRound > migrationExecutionRound) {
        /* migration re-execution */
        /* remove failed hash point */
        List<MemcachedReplicaGroup> toDeleteGroup = new ArrayList<MemcachedReplicaGroup>();
        for (Map.Entry<String, MemcachedReplicaGroup> entry : allFailedExistGroups.entrySet()) {
          toDeleteGroup.add(entry.getValue());
        }

        for (MemcachedReplicaGroup group : toDeleteGroup) {
          MemcachedReplicaGroup mrg = allExistGroups.get(group.getGroupName());
          if (mrg != null) {
            sortedExistNames.remove(group.getGroupName());
          }
        }

        /* reflect last alive exist node */
        reflectMigrationsZNode(sortedExistNames, sortedExistNames.get(sortedExistNames.size() - 1), 160, null, 0, migrationMode);

        for (MemcachedReplicaGroup group : toDeleteGroup) {
          MemcachedReplicaGroup mrg = allExistGroups.get(group.getGroupName());
          if (mrg != null) {
            mrg.deleteMemcachedNode(mrg.getMasterNode());
            mrg.deleteMemcachedNode(mrg.getSlaveNode());
            updateHash(mrg, true);
            allExistGroups.remove(group.getGroupName());
          }
        }
        migrationExecutionRound = executeRound;
        prevExistNodeName = null;
        prevExistHSliceIndex = -1;
      }

      if (executeRound == migrationExecutionRound) {
        /* mean of alterMoveSlice
         * alterMoveSlice -1 : completed round.
         * alterMoveSlice -2 : migration done.
         * alterMoveSlice -3 : new execution round.
         */
        if (alterMoveSlice != -3) {
          reflectMigrationsZNode(sortedExistNames, existName, existMoveSlice, alterName, alterMoveSlice, migrationMode);
        }
      }
      lock.unlock();
    }
  }

  private boolean reflectAlterHsliceMigrated(int existMoveSlice, int alterMoveSlice,
                                             List<Long> existHPointList, List<Long> alterHPointList, MigrationMode reflectMode) {
    /* hash slice migrated */
    Map.Entry<Long, MemcachedReplicaGroup> targ;

    if (reflectMode == MigrationMode.Join) {
      Long from = existHPointList.get(existMoveSlice);
      Long to = alterHPointList.get(alterMoveSlice);

      do {
        targ = ketamaGroups.lowerEntry(from);
        if (targ == null) {
          targ = ketamaGroups.lastEntry();
        }
        from = targ.getKey();
      } while (checkFoundExist(targ.getValue().getGroupName()));

      NavigableMap<Long, MemcachedReplicaGroup> sub;
      if (from < to) {
        sub = migrationKetamaGroups.subMap(from, false, to, true);
      } else {
        NavigableMap<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        sub = migrationKetamaGroups.subMap((long) 0, true, to, true);
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
          temp.put(entry.getKey(), entry.getValue());
        }
        if (from < migrationKetamaGroups.lastKey()) {
          sub = migrationKetamaGroups.subMap(from, true, migrationKetamaGroups.lastKey(), true);
          for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
            temp.put(entry.getKey(), entry.getValue());
          }
        }
        sub = temp;
      }

      if (sub != null && !sub.isEmpty()) {
        Map<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
          temp.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : temp.entrySet()) {
          ketamaGroups.put(entry.getKey(), entry.getValue());
          migrationKetamaGroups.remove(entry.getKey());
        }
      }
    } else {
      assert reflectMode == MigrationMode.Leave;

      Long from = alterHPointList.get(alterMoveSlice);
      Long to = existHPointList.get(existMoveSlice);

      NavigableMap<Long, MemcachedReplicaGroup> sub;
      if (from < to) {
        sub = ketamaGroups.subMap(from, true, to, false);
      } else {
        NavigableMap<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        sub = ketamaGroups.subMap((long) 0, true, to, false);
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
          temp.put(entry.getKey(), entry.getValue());
        }
        Long lastKey = ketamaGroups.lastKey();
        if (from == lastKey) {
          sub = ketamaGroups.subMap(from, true, from + 1, false);
        } else {
          assert from < lastKey;
          sub = ketamaGroups.subMap(from, true, lastKey, true);
        }
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
          temp.put(entry.getKey(), entry.getValue());
        }
        sub = temp;
      }

      if (sub != null && !sub.isEmpty()) {
        Map<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
          temp.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : temp.entrySet()) {
          ketamaGroups.remove(entry.getKey());
        }
      }
    }
    return existMoveSlice == prevExistHSliceIndex;
  }

  private boolean reflectExistHsliceMigrated(String nodeName, int existHSliceIndex, List<Long> existHPointList, MigrationMode reflectMode) {
    /* hash slice migrated */
    Map.Entry<Long, MemcachedReplicaGroup> targ;
    NavigableMap<Long, MemcachedReplicaGroup> sub = null;

    Long to = existHPointList.get(existHSliceIndex);
    Long from = to;
    if (reflectMode == MigrationMode.Join) {
      targ = ketamaGroups.lowerEntry(from);
      if (targ == null) {
        targ = ketamaGroups.lastEntry();
      }
      from = targ.getKey();

      if (from < to) {
        sub = migrationKetamaGroups.subMap(from, false, to, false);
      } else {
        NavigableMap<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        if (to != 0) {
          sub = migrationKetamaGroups.subMap((long) 0, true, to, false);
          for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
            temp.put(entry.getKey(), entry.getValue());
          }
        }
        long lastKey = migrationKetamaGroups.lastKey();
        if (from < lastKey) {
          sub = migrationKetamaGroups.subMap(from, false, lastKey, true);
          for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
            temp.put(entry.getKey(), entry.getValue());
          }
        }
        sub = temp;
      }

      if (sub != null && !sub.isEmpty()) {
        Map<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
          temp.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : temp.entrySet()) {
          ketamaGroups.put(entry.getKey(), entry.getValue());
          migrationKetamaGroups.remove(entry.getKey());
        }
      }
    } else {
      assert reflectMode == MigrationMode.Leave;

      do {
        targ = ketamaGroups.lowerEntry(from);
        if (targ == null) {
          targ = ketamaGroups.lastEntry();
        }
        from = targ.getKey();
      } while (checkFoundExist(targ.getValue().getGroupName()));

      if (from < to) {
        sub = ketamaGroups.subMap(from, false, to, false);
      } else {
        NavigableMap<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        if (to != 0) {
          sub = ketamaGroups.subMap((long) 0, true, to, false);
          for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
            temp.put(entry.getKey(), entry.getValue());
          }
        }
        long lastKey = ketamaGroups.lastKey();
        if (from != lastKey) {
          assert from < lastKey;
          sub = ketamaGroups.subMap(from, false, lastKey, true);
          for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
            temp.put(entry.getKey(), entry.getValue());
          }
        }
        sub = temp;
      }

      if (sub != null && !sub.isEmpty()) {
        Map<Long, MemcachedReplicaGroup> temp = new TreeMap<Long, MemcachedReplicaGroup>();
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : sub.entrySet()) {
          temp.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Long, MemcachedReplicaGroup> entry : temp.entrySet()) {
          ketamaGroups.remove(entry.getKey());
        }
      }
    }
    return (prevExistNodeName != null && prevExistNodeName.equals(nodeName) && prevExistHSliceIndex == existHSliceIndex);
  }

  private void reflectMigrationsZNode(List<String> existList, String existName, int existMoveSlice,
                                      String alterName, int alterMoveSlice, MigrationMode reflectMode) {
    /* prepare hash point list */
    List<Long> existHPointList;
    List<Long> alterHPointList;

    existHPointList = allHashPoints.get(existName);
    if (existHPointList == null) {
      /* the exist node was failed */
      return;
    }

    if (reflectMode == MigrationMode.Init) {
      return;
    } else if (reflectMode == MigrationMode.Join) {
      if (migrationKetamaGroups.isEmpty()) {
        return;
      }
    } else {
      assert reflectMode == MigrationMode.Leave;
      if (ketamaGroups.isEmpty()) {
        return;
      }
    }
    if (existMoveSlice < config.getNodeRepetitions()) {
      if (alterName != null && allAlterGroups.containsKey(alterName)) {
        alterHPointList = allHashPoints.get(alterName);
        if (alterHPointList != null) {
          if (reflectAlterHsliceMigrated(existMoveSlice, alterMoveSlice, existHPointList, alterHPointList, reflectMode)) {
            return;
          }
        } else {
          /* Skip reflecting the current hslice migration.
           * Go downward and reflect from the prev hslice.
           */
        }
      }
    }

    int existHSliceIndex = existMoveSlice;
    int existNodeIndex = existList.indexOf(existName);
    for (int i = existNodeIndex; i >= 0; i--) {
      String existNodeName = existList.get(i);
      if (i != existNodeIndex && allExistGroups.containsKey(existNodeName)) {
        continue;
      }
      if (allExistGroups.containsKey(existList.get(i))) {
        existHPointList = prepareHashPoint(allExistGroups.get(existList.get(i)));
      } else if (!allFailedExistGroups.isEmpty() && allFailedExistGroups.containsKey(existList.get(i))) {
        existHPointList = prepareHashPoint(allFailedExistGroups.get(existList.get(i)));
      }
      assert existHPointList != null;

      for (existHSliceIndex = existHSliceIndex - 1; existHSliceIndex >= 0; existHSliceIndex--) {
        if (reflectExistHsliceMigrated(existNodeName, existHSliceIndex, existHPointList, reflectMode)) {
          break; /* Reflection has completely done */
        }
      }
      if (existHSliceIndex >= 0) {
        break; /* Reflection has completely done */
      }

      /* the existMiveSlice of the prev existing node */
      existHSliceIndex = config.getNodeRepetitions();
    }
    if ((prevExistNodeName == null && prevExistHSliceIndex == -1) ||
            (existList.indexOf(prevExistNodeName) < existList.indexOf(existName)) ||
            (existList.indexOf(prevExistNodeName) == existList.indexOf(existName) && prevExistHSliceIndex < existMoveSlice)) {
      prevExistNodeName = existName;
      prevExistHSliceIndex = existMoveSlice;
    }
  }

  private boolean checkFoundExist(String groupName) {
    if (allFailedExistGroups.isEmpty()) {
      if (allExistGroups.containsKey(groupName)) {
        return false;
      }
    } else {
      if (allExistGroups.containsKey(groupName) || allFailedExistGroups.containsKey(groupName)) {
        return false;
      }
    }
    return true;
  }

  private List<Long> prepareHashPoint(MemcachedReplicaGroup group) {
    // Ketama does some special work with md5 where it reuses chunks.
    List<Long> result = new ArrayList<Long>();
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForGroup(group, i));

      for (int h = 0; h < 4; h++) {
        Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                | (digest[h * 4] & 0xFF);
        result.add(k);
      }
    }
    Collections.sort(result);
    return result;
  }
  /* ENABLE_MIGRATION end */

  private class ReplKetamaIterator implements Iterator<MemcachedNode> {
    final String key;
    long hashVal;
    int remainingTries;
    int numTries = 0;
    ReplicaPick pick = ReplicaPick.MASTER;

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
