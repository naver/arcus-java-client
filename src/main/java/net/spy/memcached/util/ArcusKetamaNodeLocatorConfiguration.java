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
package net.spy.memcached.util;

import java.util.Comparator;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MemcachedNodeROImpl;

public class ArcusKetamaNodeLocatorConfiguration extends
        DefaultKetamaNodeLocatorConfiguration {

  private boolean enableShardKey = false;

  /**
   * insert a node from the internal node-address map.
   *
   * @param node
   */
  public void insertNode(MemcachedNode node) {
    getSocketAddressForNode(node);
  }

  /**
   * Removes a node from the internal node-address map.
   *
   * @param node
   */
  public void removeNode(MemcachedNode node) {
    super.socketAddresses.remove(node);
  }
  /**
   * Returns whether the shard key feature is enabled.
   *
   * @return true if the shard key feature is enabled; false otherwise.
   */
  public boolean isShardKeyEnabled() {
    return enableShardKey;
  }

  /**
   * Sets the enable status of the shard key feature.
   *
   * @param useShardKey true to enable the shard key feature; false to disable.
   */
  public void enableShardKey(boolean useShardKey) {
    enableShardKey = useShardKey;
  }

  public class NodeNameComparator implements Comparator<MemcachedNode> {
    /**
     * compares lexicographically the two socket address string of MemcachedNode node1 and node2.
     * @param node1
     * @param node2
     * @return return the value 0 if the node2 is equal to node1;
     * a value less than 0 if node1 is lexicographically less than node2;
     * and a value greater than 0 if node1 is lexicographically greater than the node2.
     */
    @Override
    public int compare(MemcachedNode node1, MemcachedNode node2) {
      if (node1 instanceof MemcachedNodeROImpl) {
        node1 = ((MemcachedNodeROImpl) node1).getMemcachedNode();
      }
      if (node2 instanceof MemcachedNodeROImpl) {
        node2 = ((MemcachedNodeROImpl) node2).getMemcachedNode();
      }
      String name1 = socketAddresses.get(node1);
      String name2 = socketAddresses.get(node2);
      return name1.compareTo(name2);
    }
  }
}
