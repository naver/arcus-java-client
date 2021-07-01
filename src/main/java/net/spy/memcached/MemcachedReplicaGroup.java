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

import net.spy.memcached.compat.SpyObject;

public abstract class MemcachedReplicaGroup extends SpyObject {
  protected final String group;
  protected MemcachedNode masterNode;
  protected MemcachedNode slaveNode;
  private boolean prevMasterPick;

  protected MemcachedReplicaGroup(final String groupName) {
    if (groupName == null) {
      throw new IllegalArgumentException("Memcached in Replica Group must have group name");
    }
    this.group = groupName;
  }

  public String toString() {
    return "[" + this.masterNode + ", " + this.slaveNode + "]";
  }

  public boolean isEmptyGroup() {
    return masterNode == null && slaveNode == null;
  }

  public abstract boolean setMemcachedNode(final MemcachedNode node);

  public abstract boolean deleteMemcachedNode(final MemcachedNode node);

  public String getGroupName() {
    return this.group;
  }

  public MemcachedNode getMasterNode() {
    return masterNode;
  }

  public MemcachedNode getSlaveNode() {
    return slaveNode;
  }

  public MemcachedNode getNodeByReplicaPick(ReplicaPick pick) {
    MemcachedNode node = null;

    switch (pick) {
      case MASTER:
        node = masterNode;
        break;
      case SLAVE:
        if (slaveNode != null && slaveNode.isActive()) {
          node = slaveNode;
        } else {
          node = masterNode;
        }
        break;
      case RR:
        if (prevMasterPick && slaveNode != null && slaveNode.isActive()) {
          node = slaveNode;
        } else {
          node = masterNode;
        }
        prevMasterPick = !prevMasterPick;
        break;
      default: // This case never exist.
        break;
    }
    return node;
  }

  public abstract boolean changeRole();

  public static String getGroupNameFromNode(final MemcachedNode node) {
    return ((ArcusReplNodeAddress) node.getSocketAddress()).getGroupName();
  }
}
/* ENABLE_REPLICATION end */
