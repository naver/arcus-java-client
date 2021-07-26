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

public class MemcachedReplicaGroupImpl extends MemcachedReplicaGroup {

  public MemcachedReplicaGroupImpl(final MemcachedNode node) {
    super(getGroupNameFromNode(node));

    // Cannot make MemcachedReplicaGoup instance without group name and master/slave node
    if (node == null) {
      throw new IllegalArgumentException("Memcached Node must not be null");
    }

    setMemcachedNode(node);
  }

  public boolean setMemcachedNode(final MemcachedNode node) {
    if (node == null) {
      return false;
    }

    if (this.group.equals(getGroupNameFromNode(node))) {
      if (((ArcusReplNodeAddress) node.getSocketAddress()).isMaster()) {
        this.masterNode = node;
      } else {
        this.slaveNodes.add(node);
      }

      node.setReplicaGroup(this);
      return true;
    }
    return false;
  }

  public boolean deleteMemcachedNode(final MemcachedNode node) {
    if (node == null) {
      return false;
    }

    if (this.group.equals(getGroupNameFromNode(node))) {
      if (((ArcusReplNodeAddress) node.getSocketAddress()).isMaster()) {
        this.masterNode = null;
      } else {
        this.slaveNodes.remove(node);
      }
      return true;
    }
    return false;
  }

  public boolean changeRole() {
        /* role change */
    MemcachedNode tmpNode = this.masterNode;

    this.masterNode = this.masterCandidate;
    if (this.masterNode != null) { // previous slave node
      ((ArcusReplNodeAddress) this.masterNode.getSocketAddress()).setMaster(true);
      this.slaveNodes.remove(this.masterNode);
      this.setMasterCandidate(null);
    }

    if (tmpNode != null) { // previous master node
      ((ArcusReplNodeAddress) tmpNode.getSocketAddress()).setMaster(false);
      this.slaveNodes.add(tmpNode);
    }
    return true;
  }
}
/* ENABLE_REPLICATION end */
