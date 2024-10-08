package net.spy.memcached;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class MemcachedReplicaGroupTest {

  @Test
  void findChangedGroupsTest() {
    List<ArcusReplNodeAddress> g0 = createReplList("g0", "192.168.0.1");
    List<ArcusReplNodeAddress> g1 = createReplList("g1", "192.168.0.2");
    List<MemcachedNode> old = new ArrayList<>();
    setReplGroup(g0, old);
    setReplGroup(g1, old);

    List<InetSocketAddress> update = new ArrayList<>(g0);

    Set<String> changedGroups = MemcachedReplicaGroup.findChangedGroups(update, old);
    Assertions.assertEquals(1, changedGroups.size());
    Assertions.assertTrue(changedGroups.contains("g1"));
  }

  @Test
  void findAddrsOfChangedGroupsTest() {
    List<ArcusReplNodeAddress> g0 = createReplList("g0", "192.168.0.1");
    List<ArcusReplNodeAddress> g1 = createReplList("g1", "192.168.0.2");
    List<MemcachedNode> old = new ArrayList<>();
    setReplGroup(g0, old);
    setReplGroup(g1, old);

    List<InetSocketAddress> update = new ArrayList<>();
    update.addAll(g0.subList(0, 2));
    update.addAll(g1.subList(0, 2));

    Set<String> changedGroups = MemcachedReplicaGroup.findChangedGroups(update, old);
    List<InetSocketAddress> result
            = MemcachedReplicaGroup.findAddrsOfChangedGroups(update, changedGroups);

    Assertions.assertEquals(4, result.size());
    Assertions.assertTrue(result.contains(g0.get(0)));
    Assertions.assertTrue(result.contains(g0.get(1)));
    Assertions.assertTrue(result.contains(g1.get(0)));
    Assertions.assertTrue(result.contains(g1.get(1)));
  }

  private void setReplGroup(List<ArcusReplNodeAddress> group, List<MemcachedNode> old) {
    List<MockMemcachedNode> collect = group.stream()
            .map(MockMemcachedNode::new)
            .collect(Collectors.toList());
    MemcachedReplicaGroupImpl impl = null;
    for (MockMemcachedNode node : collect) {
      if (impl == null) {
        impl = new MemcachedReplicaGroupImpl(node);
      } else {
        node.setReplicaGroup(impl);
      }
    }
    old.addAll(collect);
  }

  private List<ArcusReplNodeAddress> createReplList(String group, String ip) {
    List<ArcusReplNodeAddress> replList = new ArrayList<>();
    replList.add(ArcusReplNodeAddress.create(group, true, ip + ":" + 11211));
    replList.add(ArcusReplNodeAddress.create(group, false, ip + ":" + (11211 + 1)));
    replList.add(ArcusReplNodeAddress.create(group, false, ip + ":" + (11211 + 2)));
    return replList;
  }
}
