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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArcusReplNodeAddress extends InetSocketAddress {
	boolean master;
	String group;
	String ip;
	int port;

	private ArcusReplNodeAddress(String group, boolean master, String ip, int port) {
		super(ip, port);
		this.group = group;
		this.master = master;
		this.ip = ip;
		this.port = port;
	}

	public String toString() {
		return "Group(" + group + ") Address(" + ip + ":" + port + ") " + (master ? "MASTER" : "SLAVE");
	}

	public String getIPPort() {
		return this.ip + ":" + this.port;
	}

	public String getGroupName() {
		return group;
	}

	static ArcusReplNodeAddress create(String group, boolean master, String ipport) {
		String[] temp = ipport.split(":");
		String ip = temp[0];
		int port = Integer.parseInt(temp[1]);
		return new ArcusReplNodeAddress(group, master, ip, port);
	}

	static ArcusReplNodeAddress createFake(String groupName) {
		return create(groupName == null ? "invalid" : groupName,
					  true, CacheManager.FAKE_SERVER_NODE);
	}

	private static List<InetSocketAddress> parseNodeNames(String s) throws Exception {
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();

		for (String node : s.split(",")) {
			ArcusReplNodeAddress a = null;
			if (node.equals(CacheManager.FAKE_SERVER_NODE)) {
				a = ArcusReplNodeAddress.createFake(null);
			} else {
				String[] temp = node.split("\\^");
				String group = temp[0];
				boolean master = temp[1].equals("M") ? true : false;
				String ipport = temp[2];
				// We may throw null pointer exception if the string has
				// an unexpected format.  Abort the whole method instead of
				// trying to ignore malformed strings.
				// Is this the right behavior?  FIXME
	
				a = ArcusReplNodeAddress.create(group, master, ipport);
			}
			addrs.add(a);
		}
		return addrs;
	}

	// Similar to AddrUtil.getAddresses.  This version parses replicaton znode names.
	// Znode names are group^{M,S}^ip:port-hostname
	static List<InetSocketAddress> getAddresses(String s) {
		List<InetSocketAddress> list = null;

		try {
			list = parseNodeNames(s);
		} catch (Exception e) {
			// May see an exception if nodes do not follow the replication naming convention
			ArcusClient.arcusLogger.error("Exception caught while parsing node" +
									" addresses. cache_list=" + s + "\n" + e);
			e.printStackTrace();
			list = null;
		}
		// Return at least one node in all cases.  Otherwise we may see unexpected null pointer
		// exceptions throughout this client library...
		if (list == null || list.size() == 0) {
			list = new ArrayList<InetSocketAddress>(1);
			list.add((InetSocketAddress)ArcusReplNodeAddress.createFake(null));
		}
		return list;
	}

	static Map<String, List<ArcusReplNodeAddress>> makeGroupAddrsList (
											List<InetSocketAddress> addrs) {

		Map<String, List<ArcusReplNodeAddress>> newAllGroups =
				new HashMap<String, List<ArcusReplNodeAddress>>();

		for (int i = 0; i < addrs.size(); i++) {
			ArcusReplNodeAddress a = (ArcusReplNodeAddress)addrs.get(i);
			String groupName = a.getGroupName();
			List<ArcusReplNodeAddress> gNodeList = newAllGroups.get(groupName);
			if (gNodeList == null) {
				gNodeList = new ArrayList<ArcusReplNodeAddress>();
				newAllGroups.put(groupName, gNodeList);
			}
			/* Make a master node the first element of node list. */
			if (a.master) /* shifts the element currently at that position */
				gNodeList.add(0, a);
			else /* Don't care the index, just add. */
				gNodeList.add(a);
		}

		for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllGroups.entrySet()) {
			/* If newGroupNodes is validate
			 * then newGroupNodes is sorted by master / slave
			 */
			List<ArcusReplNodeAddress> newGroupNodes = entry.getValue();

			if (newGroupNodes.size() >= 3) {
				ArcusClient.arcusLogger.error(entry.getKey()
						+ " group have too many node. " + newGroupNodes);
				entry.setValue(new ArrayList<ArcusReplNodeAddress>());
			}
			else if (newGroupNodes.size() == 2 &&
					newGroupNodes.get(0).getIPPort().equals(newGroupNodes.get(1).getIPPort())) {
				/*
				 * Two nodes have the same ip and port
				 */
				ArcusClient.arcusLogger.error("Group " + entry.getKey() + " have invalid state. "
						+ "Master and Slave nodes have the same ip and port. "
						+ newGroupNodes);
				entry.setValue(new ArrayList<ArcusReplNodeAddress>());
			}
			else if (!newGroupNodes.get(0).master) {
				/*
				 * Fake group (node) is always master...
				 * 
				 * Maybe! this group have slave and slave node
				 * or slave node only
				 */
			 	ArcusClient.arcusLogger.info("Group " + entry.getKey() + " have invalid state. "
						+ "This group don't have master node. "
						+ newGroupNodes);
				entry.setValue(new ArrayList<ArcusReplNodeAddress>());
			}
		}
		return newAllGroups;
	}
}
/* ENABLE_REPLICATION end */
