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
/* ENABLE_REPLICATION start */
package net.spy.memcached;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import net.spy.memcached.util.ArcusReplKetamaNodeLocatorConfiguration;

public class ArcusReplKetamaNodeLocator extends ArcusKetamaNodeLocator {
	boolean switchover;

	public ArcusReplKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
		// This configuration class is aware that InetSocketAddress is really
		// ArcusReplNodeAddress.  Its getKeyForNode uses the group name, instead
		// of the socket address.
		super(nodes, alg, new ArcusReplKetamaNodeLocatorConfiguration());
		// By default, Arcus replication client support switchover.
		// It just means that we do not shutdown the removed nodes right away.
		switchover = "true".equals(System.getProperty("arcus.switchover", "true"));
	}

	// This is same as super's update.  The only difference is that we
	// first remove nodes and then add new ones.  The order matters now because
	// old and new ones may have the same group name but different socket addresses.
	// For example, we remove the slave node and add the master node of group "g0".
	// If we add first and then remove, we end up with empty ketamaNodes.
	public void update(Collection<MemcachedNode> toAttach, Collection<MemcachedNode> toDelete) {
		lock.lock();
		try {
			// Remove memcached nodes.
			for (MemcachedNode node : toDelete) {
				allNodes.remove(node);
				updateHash(node, true);

				if (switchover && !node.isFake()) {
					// MemcachedConnection shuts down the node later on.
					continue;
				}
				// This is the base client behavior.  We shut down the node right away.
				// We stop processing ongoing requests, and the user will see
				// timeouts.
				try {
					node.getSk().attach(null);
					node.shutdown();
				} catch (IOException e) {
					getLogger().error("Failed to shutdown the node : " + node.toString());
					node.setSk(null);
				}
			}

			// Add memcached nodes.
			for (MemcachedNode node : toAttach) {
				allNodes.add(node);
				updateHash(node, false);
			}
		} catch (RuntimeException e) {
			throw e;
		} finally {
			lock.unlock();
		}
	}
}
/* ENABLE_REPLICATION end */
