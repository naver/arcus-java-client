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
package net.spy.memcached.util;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ArcusReplNodeAddress;
import net.spy.memcached.MemcachedReplicaGroup;

import java.util.Comparator;

public class ArcusReplKetamaNodeLocatorConfiguration implements
        KetamaNodeLocatorConfiguration {

  private static final int NUM_REPS = 160;

  public String getKeyForNode(MemcachedNode node, int repetition) {
    ArcusReplNodeAddress addr = (ArcusReplNodeAddress) node.getSocketAddress();
    String key = addr.getGroupName() + "-" + repetition;
    return key;
  }

  public String getKeyForGroup(MemcachedReplicaGroup group, int repetition) {
    String key = group.getGroupName() + "-" + repetition;
    return key;
  }

  public int getNodeRepetitions() {
    return NUM_REPS;
  }

  public static class MemcachedReplicaGroupComparator implements Comparator<MemcachedReplicaGroup> {
    /**
     * compares lexicographically the two socket address string of
     * MemcachedReplicaGroup group1 and group2.
     * @param group1
     * @param group2
     * @return return the value 0 if the group2 is equal to group1;
     * a value less than 0 if group1 is lexicographically less than group2;
     * and a value greater than 0 if group1 is lexicographically greater than the group2.
     */
    @Override
    public int compare(MemcachedReplicaGroup group1, MemcachedReplicaGroup group2) {
      String name1 = group1.getGroupName();
      String name2 = group2.getGroupName();
      return name1.compareTo(name2);
    }
  }
}
/* ENABLE_REPLICATION end */
