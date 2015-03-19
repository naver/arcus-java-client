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

import java.util.List;

import net.spy.memcached.compat.SpyObject;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * CacheMonitor monitors the changes of the cache server list
 * in the ZooKeeper node(/arcus/cache_list/<service_code>).
 */
public class CacheMonitor extends SpyObject implements Watcher,
		ChildrenCallback {

	ZooKeeper zk;

	/* ENABLE_REPLICATION start */
	String cacheListPath;
	/* ENABLE_REPLICATION end */

	String serviceCode;

	volatile boolean dead;

	CacheMonitorListener listener;

	List<String> prevChildren;
	
	/**
	 * The locator class of the spymemcached has an assumption
	 * that it should have one cache node at least. 
	 * Thus, we add a fake server node in it
	 * if there's no cache servers for the given service code.
	 * This is just a work-around, but it works really. 
	 */
	public static final String FAKE_SERVER_NODE = "0.0.0.0:23456";

	/* ENABLE_REPLICATION start */
	/* We only use this for demo, to show more readable node names when the cache list changes. */
	boolean demoPrintClusterDiff = false;
	/* ENABLE_REPLICATION end */

	/**
	 * Constructor
	 * 
	 * @param zk
	 *            ZooKeeper connection
	 * @param serviceCode
	 *            service code (or cloud name) to identify each cloud
	 * @param listener
	 *            Callback listener
	 */
	/* ENABLE_REPLICATION start */
	public CacheMonitor(ZooKeeper zk, String cacheListPath, String serviceCode,
			CacheMonitorListener listener) {
		this.zk = zk;
		this.cacheListPath = cacheListPath;
		this.serviceCode = serviceCode;
		this.listener = listener;

		demoPrintClusterDiff = "true".equals(System.getProperty("arcus.demoPrintClusterDiff", "false"));

		getLogger().info("Initializing the CacheMonitor.");

		// Get the cache list from the Arcus admin asynchronously.
		// Returning list would be processed in processResult().
		asyncGetCacheList();
	}
	/* ENABLE_REPLICATION else */
	/*
	public CacheMonitor(ZooKeeper zk, String serviceCode,
			CacheMonitorListener listener) {
		this.zk = zk;
		this.serviceCode = serviceCode;
		this.listener = listener;

		getLogger().info("Initializing the CacheMonitor.");
		
		// Get the cache list from the Arcus admin asynchronously.
		// Returning list would be processed in processResult().
		asyncGetCacheList();
	}
	*/
	/* ENABLE_REPLICATION end */

	/**
	 * Other classes use the CacheMonitor by implementing this method
	 */
	public interface CacheMonitorListener {
		/**
		 * The existing children of the node has changed.
		 */
		void commandNodeChange(List<String> children);

		/**
		 * The ZooKeeper session is no longer valid.
		 */
		void closing();
	}

	/**
	 * Processes every events from the ZooKeeper.
	 */
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.None) {
			// Processes session events
			switch (event.getState()) {
			case SyncConnected:
				getLogger().warn("Reconnected to the Arcus admin. " + getInfo());
				return;
			case Disconnected:
				getLogger().warn("Disconnected from the Arcus admin. Trying to reconnect. " + getInfo());
				return;
			case Expired:
				// If the session was expired, just shutdown this client to be re-initiated.
				getLogger().warn("Session expired. Trying to reconnect to the Arcus admin." + getInfo());
				shutdown();
				return;
			}
		} else {
			// Set a new watch on the znode when there are any changes in it.
			if (event.getType() == Event.EventType.NodeChildrenChanged) {
				asyncGetCacheList();
			}
		}
	}

	/**
	 * A callback function to process the result of getChildren(watch=true).
	 */
	public void processResult(int rc, String path, Object ctx,
			List<String> children) {
		switch (Code.get(rc)) {
		case OK:
			commandNodeChange(children);
			return;
		case NONODE:
			getLogger().fatal("Cannot find your service code. Please contact Arcus support to solve this problem. " + getInfo());
			return;
		case SESSIONEXPIRED:
			getLogger().warn("Session expired. Trying to reconnect to the Arcus admin. " + getInfo());
			shutdown();
			return;
		case NOAUTH:
			getLogger().fatal("Authorization failed " + getInfo());
			shutdown();
			return;
		case CONNECTIONLOSS:
			getLogger().warn("Connection lost. Trying to reconnect to the Arcus admin." + getInfo());
			asyncGetCacheList();
			return;
		default:
			getLogger().warn("Ignoring an unexpected event from the Arcus admin. code=" + Code.get(rc) + ", " + getInfo());
			asyncGetCacheList();
			return;
		}
	}

	/**
	 * Get the cache list asynchronously from the Arcus admin.
	 */
	void asyncGetCacheList() {
		if (getLogger().isDebugEnabled()) {
			/* ENABLE_REPLICATION start */
			getLogger().debug("Set a new watch on " + (cacheListPath + serviceCode));
			/* ENABLE_REPLICATION else */
			/*
			getLogger().debug("Set a new watch on " + (CacheManager.CACHE_LIST_PATH + serviceCode));
			*/
			/* ENABLE_REPLICATION end */
		}
		
		/* ENABLE_REPLICATION start */
		zk.getChildren(cacheListPath + serviceCode, true, this, null);
		/* ENABLE_REPLICATION else */
		/*
		zk.getChildren(CacheManager.CACHE_LIST_PATH + serviceCode, true, this, null);
		*/
		/* ENABLE_REPLICATION end */
	}

	/**
	 * Let the CacheManager change the cache list. 
	 * If there's no children in the znode, make a fake server node.
	 * @param children
	 */
	void commandNodeChange(List<String> children) {
		// If there's no children, add a fake server node to the list.
		if (children.size() == 0) {
			getLogger().error("Cannot find any cache nodes for your service code. Please contact Arcus support to solve this problem. " + getInfo());
			children.add(FAKE_SERVER_NODE);
		}

		if (!children.equals(prevChildren)) {
			getLogger().warn("Cache list has been changed : From=" + prevChildren + ", To=" + children + ", " + getInfo());
			/* ENABLE_REPLICATION start */
			if (demoPrintClusterDiff) {
				// Assume 1.7 cluster
				System.out.println("\nCLUSTER CHANGE\n---PREVIOUS---");
				if (prevChildren == null) {
					System.out.println("NO NODES");
				}
				else {
					for (String s : prevChildren) {
						try {
							System.out.println(Arcus17NodeAddress.parseNodeName(s));
						} catch (Exception e) {
						}
					}
				}
				System.out.println("---CURRENT---");
				if (children == null) {
					System.out.println("NO NODES");
				}
				else {
					for (String s : children) {
						try {
							System.out.println(Arcus17NodeAddress.parseNodeName(s));
						} catch (Exception e) {
						}
					}
				}
				System.out.println("");
			}
			/* ENABLE_REPLICATION end */
		}
		
		// Store the current children.
		prevChildren = children;

		// Change the memcached node list.
		listener.commandNodeChange(children);
	}

	/**
	 * Shutdown the CacheMonitor.
	 */
	public void shutdown() {
		getLogger().info("Shutting down the CacheMonitor. " + getInfo());
		dead = true;
		listener.closing();
	}
	
	private String getInfo() {
		String zkSessionId = null;
		if (zk != null) {
			zkSessionId = "0x" + Long.toHexString(zk.getSessionId());
		}
		return "[serviceCode=" + serviceCode + ", adminSessionId=" + zkSessionId + "]";
	}
}
