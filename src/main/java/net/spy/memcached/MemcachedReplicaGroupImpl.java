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

public class MemcachedReplicaGroupImpl extends MemcachedReplicaGroup {

	public MemcachedReplicaGroupImpl(final MemcachedNode node) {
		super(getGroupNameForNode(node));

		// Cannot make MemcachedReplicaGoup instance without group name and master/slave node  
		if (node == null)
			throw new IllegalArgumentException("Memcached Node must not be null");		

		setMemcachedNode(node);
	}
	
	public boolean setMemcachedNode(final MemcachedNode node) {
		if (node == null)
			return false;		

		if (this.group.equals(getGroupNameForNode(node))) {
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			if (((ArcusReplNodeAddress)node.getSocketAddress()).master)
				this.masterNode = node;
			else
				this.slaveNode = node;

			node.setReplicaGroup(this);
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			if (((ArcusReplNodeAddress)node.getSocketAddress()).master)
				this.masterNode = node;
			else
				this.slaveNode = node;
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			return true;
		} else {
			return false;
		}
	}

	public boolean deleteMemcachedNode(final MemcachedNode node) {
		if (node == null)
			return false;		

		if (this.group.equals(getGroupNameForNode(node))) {
			if (((ArcusReplNodeAddress)node.getSocketAddress()).master)
				this.masterNode = null;
			else
				this.slaveNode = null;
			return true;
		} else {
			return false;
		}
	}

	public boolean changeRole() {
		/* role change */
		MemcachedNode tmpNode = this.masterNode;

		this.masterNode = this.slaveNode;
		if (this.masterNode != null) // previous slave node
			((ArcusReplNodeAddress)this.masterNode.getSocketAddress()).master = true;

		this.slaveNode = tmpNode; 
		if (this.slaveNode != null) // previous master node
			((ArcusReplNodeAddress)this.slaveNode.getSocketAddress()).master = false;

		return true;
	}
}
/* ENABLE_REPLICATION end */
