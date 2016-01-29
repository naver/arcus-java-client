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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.ArcusReplKetamaNodeLocatorConfiguration;

public class ArcusReplKetamaNodeLocator extends SpyObject implements NodeLocator {

	private TreeMap<Long, MemcachedReplicaGroup> ketamaGroups;
	private HashMap<String, MemcachedReplicaGroup> allGroups;
	private Collection<MemcachedNode> allNodes;

	private HashAlgorithm hashAlg;
	private ArcusReplKetamaNodeLocatorConfiguration config;

	Lock lock = new ReentrantLock();

	public ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
		// This configuration class is aware that InetSocketAddress is really
		// ArcusReplNodeAddress.  Its getKeyForNode uses the group name, instead
		// of the socket address.
		this (nodes, alg, new ArcusReplKetamaNodeLocatorConfiguration());
	}

	private ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg,
			ArcusReplKetamaNodeLocatorConfiguration conf) {
		super();
		allNodes = nodes;
		hashAlg = alg;
		ketamaGroups = new TreeMap<Long, MemcachedReplicaGroup>();
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
			}
			else {
				mrg.setMemcachedNode(node);
			}
		}

		int numReps = config.getNodeRepetitions();
		for (MemcachedReplicaGroup group : allGroups.values()) {
			// Ketama does some special work with md5 where it reuses chunks.
			if (alg == HashAlgorithm.KETAMA_HASH) {
				updateHash(group, false);
			}
			else {
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
			rg = ketamaGroups.get(hash);
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
				new TreeMap<Long, MemcachedReplicaGroup> (ketamaGroups);
		HashMap<String, MemcachedReplicaGroup> ag = 
				new HashMap<String, MemcachedReplicaGroup> (allGroups.size());
		Collection<MemcachedNode> an = new ArrayList<MemcachedNode> (allNodes.size());

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
		} catch (RuntimeException e) {
			throw e;
		} finally {
			lock.unlock();
		}
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
				
				if (remove)
					ketamaGroups.remove(k);
				else
					ketamaGroups.put(k, group);
			}
		}
	}

	private class ReplKetamaIterator implements Iterator<MemcachedNode> {
		final String key;
		long hashVal;
		int remainingTries;
		int numTries = 0;
		ReplicaPick pick = ReplicaPick.MASTER;

		public ReplKetamaIterator(final String k,  ReplicaPick p, final int t) {
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
