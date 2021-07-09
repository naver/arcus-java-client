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

import java.util.ArrayList;
import java.util.List;

public abstract class MemcachedReplicaGroup extends SpyObject {
  protected final String group;
  protected MemcachedNode masterNode;
  protected List<MemcachedNode> slaveNodes = new ArrayList<MemcachedNode>(MAX_REPL_SLAVE_SIZE);
  private boolean prevMasterPick;
  private int nextSlaveIndex;
  private int nextRrIndex;
  protected MemcachedNode masterCandidate;
  private final StringBuilder sb = new StringBuilder();

  public static final int MAX_REPL_SLAVE_SIZE = 2;
  public static final int MAX_REPL_GROUP_SIZE = MAX_REPL_SLAVE_SIZE + 1;

  protected MemcachedReplicaGroup(final String groupName) {
    if (groupName == null) {
      throw new IllegalArgumentException("Memcached in Replica Group must have group name");
    }
    this.group = groupName;
  }

  public String toString() {
    sb.setLength(0);
    sb.append("[");
    sb.append(masterNode);
    for (MemcachedNode slaveNode : slaveNodes) {
      sb.append(", ");
      sb.append(slaveNode);
    }
    sb.append("]");
    return sb.toString();
  }

  public boolean isEmptyGroup() {
    return masterNode == null && slaveNodes.isEmpty();
  }

  public abstract boolean setMemcachedNode(final MemcachedNode node);

  public abstract boolean deleteMemcachedNode(final MemcachedNode node);

  public String getGroupName() {
    return this.group;
  }

  public MemcachedNode getMasterNode() {
    return masterNode;
  }

  public List<MemcachedNode> getSlaveNodes() {
    return slaveNodes;
  }

  public MemcachedNode getSlaveNode(int index) {
    return slaveNodes.get(index);
  }

  public MemcachedNode getMasterCandidate() {
    return masterCandidate;
  }

  public void setMasterCandidate(MemcachedNode masterCandidate) {
    this.masterCandidate = masterCandidate;
  }

  public MemcachedNode getNodeByReplicaPick(ReplicaPick pick) {
    MemcachedNode node = null;

    switch (pick) {
      case MASTER:
        node = masterNode;
        break;
      case SLAVE:
        if (!slaveNodes.isEmpty()) {
          node = getNextActiveSlaveNode(true);
        }
        if (node == null) {
          node = masterNode;
        }
        break;
      case RR:
        if (prevMasterPick) {
          node = getNextActiveSlaveNode(false);
        }
        if (node == null) {
          node = masterNode;
        }
        break;
      default: // This case never exist.
        break;
    }
    return node;
  }

  private MemcachedNode getNextActiveSlaveNode(boolean rotate) {
    MemcachedNode node;
    int index = rotate ? this.nextSlaveIndex : this.nextRrIndex;
    int firstIndex = index;

    do {
      node = slaveNodes.get(index);
      if (!node.isActive()) {
        node = null;
      }
      index = (index + 1) % slaveNodes.size();
    } while (node == null && index != firstIndex);

    if (rotate) {
      this.nextSlaveIndex = index;
    } else {
      this.nextRrIndex = index;
    }

    return node;
  }

  public abstract boolean changeRole();

  public static String getGroupNameFromNode(final MemcachedNode node) {
    return ((ArcusReplNodeAddress) node.getSocketAddress()).getGroupName();
  }
}
/* ENABLE_REPLICATION end */
