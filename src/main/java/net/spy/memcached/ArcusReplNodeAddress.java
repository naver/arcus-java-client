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

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class ArcusReplNodeAddress extends InetSocketAddress {

  private static final long serialVersionUID = -1555690881482453720L;
  private static final Logger arcusLogger = LoggerFactory.getLogger(ArcusReplNodeAddress.class);
  private boolean master;
  private final String group;
  private final String ip;
  private final int port;

  private ArcusReplNodeAddress(String group, boolean master, String ip, int port) {
    super(ip, port);
    this.group = group;
    this.master = master;
    this.ip = ip;
    this.port = port;
  }

  public ArcusReplNodeAddress(ArcusReplNodeAddress addr) {
    this(addr.group, addr.master, addr.ip, addr.port);
  }

  public String toString() {
    return "{" + group + " " + (master ? "M" : "S") + " " + ip + ":" + port + "}";
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

  private static List<InetSocketAddress> parseNodeNames(String s) {
    List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();

    for (String node : s.split(",")) {
      String[] temp = node.split("\\^");
      String group = temp[0];
      boolean master = temp[1].equals("M");
      String ipport = temp[2];
      // We may throw null pointer exception if the string has
      // an unexpected format.  Abort the whole method instead of
      // trying to ignore malformed strings.
      // Is this the right behavior?  FIXME

      addrs.add(ArcusReplNodeAddress.create(group, master, ipport));
    }
    return addrs;
  }

  // Similar to AddrUtil.getAddresses.  This version parses replicaton znode names.
  // Znode names are group^{M,S}^ip:port-hostname
  static List<InetSocketAddress> getAddresses(String s) {
    List<InetSocketAddress> list = null;

    if (s != null && !s.isEmpty()) {
      try {
        list = parseNodeNames(s);
      } catch (Exception e) {
        // May see an exception if nodes do not follow the replication naming convention
        arcusLogger.error("Exception caught while parsing node" +
                " addresses. cache_list=" + s + "\n" + e);
        e.printStackTrace();
      }
    }

    if (list == null) {
      list = new ArrayList<InetSocketAddress>(0);
    }
    return list;
  }

  static Map<String, List<ArcusReplNodeAddress>> makeGroupAddrsList(
          List<InetSocketAddress> addrs) {

    Map<String, List<ArcusReplNodeAddress>> newAllGroups =
            new HashMap<String, List<ArcusReplNodeAddress>>();

    for (InetSocketAddress addr : addrs) {
      ArcusReplNodeAddress a = (ArcusReplNodeAddress) addr;
      String groupName = a.getGroupName();
      List<ArcusReplNodeAddress> gNodeList = newAllGroups.get(groupName);
      if (gNodeList == null) {
        gNodeList = new ArrayList<ArcusReplNodeAddress>();
        newAllGroups.put(groupName, gNodeList);
      }
      // Add the master node as the first element of node list.
      if (a.master) { // shifts the element currently at that position
        gNodeList.add(0, a);
      } else { // Don't care the index, just add it.
        gNodeList.add(a);
      }
    }

    for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllGroups.entrySet()) {
      // If newGroupNodes is valid, it is sorted by master and slave order.
      validateGroup(entry);
    }
    return newAllGroups;
  }

  private static void validateGroup(Map.Entry<String, List<ArcusReplNodeAddress>> group) {
    List<ArcusReplNodeAddress> newGroupNodes = group.getValue();
    int groupSize = newGroupNodes.size();

    if (groupSize > MemcachedReplicaGroup.MAX_REPL_GROUP_SIZE) {
      arcusLogger.error("Invalid group " + group.getKey() + " : "
              + " Too many nodes. " + newGroupNodes);
      group.setValue(new ArrayList<ArcusReplNodeAddress>());
    } else if (groupSize > 1 && hasDuplicatedAddress(newGroupNodes, groupSize)) {
      // Two or more nodes have the same ip and port.
      arcusLogger.error("Invalid group " + group.getKey() + " : "
              + "Two or more nodes have the same ip and port." + newGroupNodes);
      group.setValue(new ArrayList<ArcusReplNodeAddress>());
    } else if (groupSize > 1 && hasMultiMasters(newGroupNodes)) {
      // Two or more nodes are masters.
      arcusLogger.error("Invalid group " + group.getKey() + " : "
              + "Two or more master nodes exist. " + newGroupNodes);
      group.setValue(new ArrayList<ArcusReplNodeAddress>());
    } else if (!newGroupNodes.get(0).master) {
      /* This case can occur during the switchover or failover.
       * 1) In the switchover, it occurs after below the first phase.
       * - the old master is changed to the new slave node.
       * - the old slave is changed to the new master node.
       * 2) In the failover, it occurs after below the first phase.
       * - the old master is removed by abnormal shutdown.
       * - the old slave is changed to the new master node.
       */
      arcusLogger.info("Invalid group " + group.getKey() + " : "
              + "Master does not exist. " + newGroupNodes);
      group.setValue(new ArrayList<ArcusReplNodeAddress>());
    }
  }

  public boolean isSameAddress(ArcusReplNodeAddress addr) {
    return this.getIPPort().equals(addr.getIPPort());
  }

  public boolean isMaster() {
    return master;
  }

  public void setMaster(boolean master) {
    this.master = master;
  }

  private static boolean hasDuplicatedAddress(List<ArcusReplNodeAddress> groupNodes, int groupSize) {
    if (groupSize == 2) {
      return groupNodes.get(0).isSameAddress(groupNodes.get(1));
    }

    Set<String> addrSet = new HashSet<String>();
    for (ArcusReplNodeAddress nodeAddress : groupNodes) {
      addrSet.add(nodeAddress.getIPPort());
    }
    return groupSize != addrSet.size();
  }

  private static boolean hasMultiMasters(List<ArcusReplNodeAddress> newGroupNodes) {
    int masterCount = 0;
    for (ArcusReplNodeAddress nodeAddress : newGroupNodes) {
      if (nodeAddress.isMaster()) {
        if (++masterCount > 1) {
          return true;
        }
      }
    }
    return false;
  }
}
/* ENABLE_REPLICATION end */
