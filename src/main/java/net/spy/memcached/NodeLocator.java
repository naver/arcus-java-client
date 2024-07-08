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
import java.util.Collection;
import java.util.Iterator;

/**
 * Interface for locating a node by hash value.
 */
public interface NodeLocator {

  /**
   * Get the primary location for the given key.
   *
   * @param k the object key
   * @return the QueueAttachment containing the primary storage for a key
   */
  MemcachedNode getPrimary(String k);

  /**
   * Get an iterator over the sequence of nodes that make up the backup
   * locations for a given key.
   *
   * @param k the object key
   * @return the sequence of backup nodes.
   */
  Iterator<MemcachedNode> getSequence(String k);

  /**
   * Get all memcached nodes.  This is useful for broadcasting messages.
   */
  Collection<MemcachedNode> getAll();

  /**
   * Create a read-only copy of this NodeLocator.
   */
  NodeLocator getReadonlyCopy();

  /**
   * Update all memcached nodes. Note that this feature is
   * only available in ArcusKetamaNodeLocator.
   */
  void update(Collection<MemcachedNode> toAttach, Collection<MemcachedNode> toDelete);

  /**
   * Get all memcached nodes that removed from ZK but has operation in queue.
   * Note that this feature is only available in ArcusKetamaNodeLocator.
   */
  Collection<MemcachedNode> getDelayedClosingNodes();

  /**
   * Update all memcached nodes that removed from ZK but has operation in queue.
   * Note that this feature is only available in ArcusKetamaNodeLocator.
   */
  void updateDelayedClosingNodes(Collection<MemcachedNode> closedNodes);

  /* ENABLE_MIGRATION if */
  /**
   * Get all alter memcached nodes.
   */
  Collection<MemcachedNode> getAlterAll();

  /**
   * Get an alter memcached node which contains the given socket address.
   */
  MemcachedNode getAlterNode(SocketAddress sa);

  /**
   * Get an owner node by owner name.
   * @return OwnerNode is a importer. Importer by migration type
   * JOIN : joining node, LEAVE : existing node.
   */
  MemcachedNode getOwnerNode(String owner, MigrationType mgType);

  /**
   * Remove the alter nodes which have failed down.
   */
  void updateAlter(Collection<MemcachedNode> toAttach,
                   Collection<MemcachedNode> toDelete);

  /**
   * Prepare migration with alter nodes and migration type.
   */
  void prepareMigration(Collection<MemcachedNode> toAlter, MigrationType type);

  /**
   * Update(or reflect) the migratoin range in ketama hash ring.
   */
  void updateMigration(Long spoint, Long epoint);
  /* ENABLE_MIGRATION end */
}
