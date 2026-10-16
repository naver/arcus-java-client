package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import net.spy.memcached.util.ArcusReplKetamaNodeLocatorConfiguration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArcusReplKetamaHashingTest {

  // This group repeats this hash point at (repetition, chunk) (4, 3) and (14, 1).
  private static final String DUPLICATE_GROUP = "g652";
  private static final long DUPLICATE_HASH_POINT = 2_005_388_527L;

  @Test
  void testRemoveGroupWithDuplicateHashPointInSelf() {
    // given
    List<MemcachedNode> nodes = createNodes(Arrays.asList("g0", "g1"));
    List<MemcachedNode> dupNodes = createGroup(DUPLICATE_GROUP, 11213);

    ArcusReplKetamaNodeLocator locator =
        new ArcusReplKetamaNodeLocator(nodes, new ArcusReplKetamaNodeLocatorConfiguration());
    locator.update(dupNodes, Collections.emptyList());
    assertTrue(locator.getAll().containsAll(dupNodes));

    // when
    locator.update(Collections.emptyList(), dupNodes);

    // then
    for (MemcachedNode node : dupNodes) {
      assertFalse(locator.getAll().contains(node));
    }
    assertFalse(locator.getAllGroups().containsKey(DUPLICATE_GROUP));
    assertNoHashPointOf(locator, DUPLICATE_GROUP);
  }

  @Test
  void testRemoveJoiningAlterGroupWithDuplicateHashPointInSelf() {
    // given
    List<MemcachedNode> nodes = createNodes(Arrays.asList("g0", "g1"));
    List<MemcachedNode> dupNodes = createGroup(DUPLICATE_GROUP, 11213);

    ArcusReplKetamaNodeLocator locator = new ArcusReplKetamaNodeLocator(
        nodes, new ArcusReplKetamaNodeLocatorConfiguration());
    locator.prepareMigration(dupNodes, MigrationType.JOIN);
    assertTrue(locator.getAlterAll().containsAll(dupNodes));
    locator.updateMigration(0L, DUPLICATE_HASH_POINT);

    // when
    locator.updateAlter(Collections.emptyList(), dupNodes);

    // then
    assertTrue(locator.getAlterAll().isEmpty());
    assertNoHashPointOf(locator, DUPLICATE_GROUP);
  }

  @Test
  void testRemoveLeavingAlterGroupWithDuplicateHashPointInSelf() {
    // given
    List<MemcachedNode> nodes = createNodes(Arrays.asList("g0", "g1"));
    List<MemcachedNode> dupNodes = createGroup(DUPLICATE_GROUP, 11213);

    ArcusReplKetamaNodeLocator locator = new ArcusReplKetamaNodeLocator(
        nodes, new ArcusReplKetamaNodeLocatorConfiguration());
    locator.update(dupNodes, Collections.emptyList());
    locator.prepareMigration(dupNodes, MigrationType.LEAVE);
    assertTrue(locator.getAlterAll().containsAll(dupNodes));
    locator.updateMigration(0L, DUPLICATE_HASH_POINT);

    // when
    locator.updateAlter(Collections.emptyList(), dupNodes);

    // then
    assertTrue(locator.getAlterAll().isEmpty());
    assertNoHashPointOf(locator, DUPLICATE_GROUP);
  }

  private void assertNoHashPointOf(ArcusReplKetamaNodeLocator locator, String groupName) {
    for (SortedSet<MemcachedReplicaGroup> groupSet : locator.getKetamaGroups().values()) {
      for (MemcachedReplicaGroup group : groupSet) {
        assertNotEquals(groupName, group.getGroupName());
      }
    }
  }

  private List<MemcachedNode> createNodes(List<String> groupNames) {
    List<MemcachedNode> nodes = new ArrayList<>(groupNames.size() * 2);
    for (int i = 0; i < groupNames.size(); i++) {
      nodes.addAll(createGroup(groupNames.get(i), 11211 + i));
    }
    return nodes;
  }

  private List<MemcachedNode> createGroup(String groupName, int port) {
    MemcachedNode master = new MockMemcachedNode(
        ArcusReplNodeAddress.create(groupName, true, "127.0.0.1:" + port));
    MemcachedNode slave = new MockMemcachedNode(
        ArcusReplNodeAddress.create(groupName, false, "127.0.0.2:" + port));
    return Arrays.asList(master, slave);
  }
}
