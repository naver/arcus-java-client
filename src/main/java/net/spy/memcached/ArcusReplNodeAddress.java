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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.ArrayList;

public class ArcusReplNodeAddress extends InetSocketAddress {
	boolean master;
	String group;
	String ip;
	int port;

	ArcusReplNodeAddress(String group, boolean master, String ip, int port) {
		super(ip, port);
		this.group = group;
		this.master = master;
		this.ip = ip;
		this.port = port;
	}

	public String toString() {
		return "Group(" + group + ") Address(" + ip + ":" + port + ") " + (master ? "MASTER" : "SLAVE");
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

	private static List<InetSocketAddress> parseNodeNames(String s) throws Exception {
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		for (String node : s.split(",")) {
			String[] temp = node.split("\\^");
			String group = temp[0];
			boolean master = temp[1].equals("M") ? true : false;
			String[] temp2 = temp[2].split("-");
			String ipport = temp2[0];
			// We may throw null pointer exception if the string has
			// an unexpected format.  Abort the whole method instead of
			// trying to ignore malformed strings.
			// Is this the right behavior?  FIXME

			// Turn slave servers into fake ones.  We still create MemcachedNode's
			// for groups without masters.  We don't want these to connect to actual
			// slave servers.
			if (!master)
				ipport = CacheMonitor.FAKE_SERVER_NODE;

			ArcusReplNodeAddress a = ArcusReplNodeAddress.create(group, master, ipport);

			// We want exactly one node per group in this version.
			// If a node exists and the new one is the master, replace the old one
			// with the new one.
			for (int i = 0; i < addrs.size(); i++) {
				if (((ArcusReplNodeAddress)addrs.get(i)).group.equals(group)) {
					if (master) {
						// The new node is the master.  Replace.
						addrs.set(i, a);
					}
					else {
						// The new node is the slave.  Do not add.
					}
					// In any case, we've found a previous node with
					// the same group name.  So do not append the new node.
					a = null;
					break;
				}
			}
			// We've never seen this group before.  Append it to the list.
			if (a != null) {
				addrs.add((InetSocketAddress)a);
			}
		}
		return addrs;
	}

	// Similar to AddrUtil.getAddresses.  This version parses replicaton znode names.
	// Znode names are group^{M,S}^ip:port-hostname
	static List<InetSocketAddress> getAddresses(String s) {
		List<InetSocketAddress> list = null;

		if (s.equals(CacheMonitor.FAKE_SERVER_NODE)) {
			// Special case the empty cache_list.
			// CacheMonitor adds one FAKE_SERVER_NODE to children before calling this method.
		}
		else {
			try {
				list = parseNodeNames(s);
			} catch (Exception e) {
				// May see an exception if nodes do not follow the replication naming convention
				ArcusClient.arcusLogger.error("Exception caught while parsing node" +
							      " addresses. cache_list=" + s +
							      "\n" + e);
				e.printStackTrace();
				list = null;
			}
		}
		// Return at least one node in all cases.  Otherwise we may see unexpected null pointer
		// exceptions throughout this client library...
		if (list == null || list.size() == 0) {
			list = new ArrayList<InetSocketAddress>(1);
			list.add((InetSocketAddress)
				 ArcusReplNodeAddress.create("invalid", false, CacheMonitor.FAKE_SERVER_NODE));
		}
		return list;
	}

	public static ArcusReplNodeAddress parseNodeName(String node) throws Exception {
		String[] temp = node.split("\\^");
		String group = temp[0];
		boolean master = temp[1].equals("M") ? true : false;
		String[] temp2 = temp[2].split("-");
		String ipport = temp2[0];

		return ArcusReplNodeAddress.create(group, master, ipport);
	}
}
/* ENABLE_REPLICATION end */
