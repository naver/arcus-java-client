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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.spy.memcached.compat.SpyObject;

public abstract class MemcachedReplicaGroup extends SpyObject {
  protected final String group;
  protected MemcachedNode masterNode;
  protected List<MemcachedNode> slaveNodes = new ArrayList<>(MAX_REPL_SLAVE_SIZE);
  private int nextSlaveIndex = -1;
  protected MemcachedNode masterCandidate;
  private final StringBuilder sb = new StringBuilder();
  private boolean delayedSwitchover = false;

  public static final int MAX_REPL_SLAVE_SIZE = 2;
  public static final int MAX_REPL_GROUP_SIZE = MAX_REPL_SLAVE_SIZE + 1;

  protected MemcachedReplicaGroup(final String groupName) {
    if (groupName == null) {
      throw new IllegalArgumentException("Memcached in Replica Group must have group name");
    }
    this.group = groupName;
  }

  @Override
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
    return Collections.unmodifiableList(slaveNodes);
  }

  public MemcachedNode getMasterCandidate() {
    return masterCandidate;
  }

  public void setMasterCandidate() {
    if (!slaveNodes.isEmpty()) {
      this.masterCandidate = slaveNodes.get(0);
    }
  }

  public void clearMasterCandidate() {
    this.masterCandidate = null;
  }

  public void setMasterCandidateByAddr(String address) {
    for (MemcachedNode node : this.getSlaveNodes()) {
      if (address.equals(((ArcusReplNodeAddress) node.getSocketAddress()).getIPPort())) {
        this.masterCandidate = node;
        break;
      }
    }
  }

  public MemcachedNode getNodeByReplicaPick(ReplicaPick pick) {
    MemcachedNode node = null;

    switch (pick) {
      case MASTER:
        if (masterCandidate != null && !delayedSwitchover) {
          node = masterCandidate;
        } else {
          node = masterNode;
        }
        break;
      case SLAVE:
        if (!slaveNodes.isEmpty()) {
          node = getNextActiveSlaveNodeRotate();
        }
        if (node == null) {
          node = masterNode;
        }
        break;
      case RR:
        if (!slaveNodes.isEmpty()) {
          node = getNextActiveSlaveNodeNoRotate();
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

  public boolean isDelayedSwitchover() {
    return delayedSwitchover;
  }

  public void setDelayedSwitchover(boolean delayedSwitchover) {
    this.delayedSwitchover = delayedSwitchover;
  }

  private MemcachedNode getNextActiveSlaveNodeRotate() {
    MemcachedNode node = null;
    int firstIndex = -1;

    do {
      if (++nextSlaveIndex >= slaveNodes.size()) {
        nextSlaveIndex = 0;
      }
      if (nextSlaveIndex == firstIndex) {
        break;
      }
      node = slaveNodes.get(nextSlaveIndex);
      if (!node.isActive()) {
        node = null;
        if (firstIndex == -1) {
          firstIndex = nextSlaveIndex;
        }
      }
    } while (node == null);

    return node;
  }

  private MemcachedNode getNextActiveSlaveNodeNoRotate() {
    MemcachedNode node = null;

    do {
      if (++nextSlaveIndex >= slaveNodes.size()) {
        nextSlaveIndex = -1;
        break;
      }
      node = slaveNodes.get(nextSlaveIndex);
      if (!node.isActive()) {
        node = null;
      }
    } while (node == null);

    return node;
  }

  public abstract boolean changeRole();

  public static String getGroupNameFromNode(final MemcachedNode node) {
    return ((ArcusReplNodeAddress) node.getSocketAddress()).getGroupName();
  }
}
/* ENABLE_REPLICATION end */
