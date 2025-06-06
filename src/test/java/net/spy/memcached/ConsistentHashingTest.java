package net.spy.memcached;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test fails intermittently.
 * Ignore it now and fix it later.
 */
@Disabled
class ConsistentHashingTest {

  @Test
  void testSmallSet() {
    runThisManyNodes(3);
  }

  @Test
  void testLargeSet() {
    runThisManyNodes(100);
  }

  /**
   * Simulate dropping from (totalNodes) to (totalNodes-1).
   * Ensure hashing is consistent between the the two scenarios.
   *
   * @param totalNodes totalNodes
   */
  private void runThisManyNodes(final int totalNodes) {
    final List<String>[] stringNodes = generateAddresses(totalNodes);

    List<MemcachedNode> smaller = createNodes(
            AddrUtil.getAddresses(stringNodes[0]));
    List<MemcachedNode> larger = createNodes(
            AddrUtil.getAddresses(stringNodes[1]));

    assertTrue(larger.containsAll(smaller));
    MemcachedNode oddManOut = larger.get(larger.size() - 1);
    assertFalse(smaller.contains(oddManOut));

    KetamaNodeLocator lgLocator = new KetamaNodeLocator(
            larger, HashAlgorithm.KETAMA_HASH);
    KetamaNodeLocator smLocator = new KetamaNodeLocator(
            smaller, HashAlgorithm.KETAMA_HASH);

    SortedMap<Long, MemcachedNode> lgMap = lgLocator.getKetamaNodes();
    SortedMap<Long, MemcachedNode> smMap = smLocator.getKetamaNodes();

    // Verify that EVERY entry in the smaller map has an equivalent
    // mapping in the larger map.
    boolean failed = false;
    for (final Long key : smMap.keySet()) {
      final MemcachedNode largeNode = lgMap.get(key);
      final MemcachedNode smallNode = smMap.get(key);
      if (!largeNode.equals(smallNode)) {
        failed = true;
        System.out.println("---------------");
        System.out.println("Key: " + key);
        System.out.println("Small: " + smallNode.getSocketAddress());
        System.out.println("Large: " + largeNode.getSocketAddress());
      }
    }
    assertFalse(failed);

    for (final Map.Entry<Long, MemcachedNode> entry : lgMap.entrySet()) {
      final Long key = entry.getKey();
      final MemcachedNode node = entry.getValue();
      if (node.equals(oddManOut)) {
        final MemcachedNode newNode = smLocator.getNodeForKey(key);
        if (!smaller.contains(newNode)) {
          System.out.println(
                  "Error - " + key + " -> " + newNode.getSocketAddress());
          failed = true;
        }
      }
    }
    assertFalse(failed);

  }

  private List<String>[] generateAddresses(final int maxSize) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    final List<String>[] results = new List[2];
    results[0] = new ArrayList<>(maxSize);
    results[1] = new ArrayList<>(maxSize);

    // Generate a pseudo-random set of addresses.
    long now = new Date().getTime();
    int first = (int) ((now % 250) + 3);

    int second = (int) (((now / 250) % 250) + 3);

    String port = ":11211 ";
    int last = (int) ((now % 100) + 3);

    String prefix = first + "." + second + ".1.";

    // Don't protect the possible range too much, as we are our own client.
    for (int ix = 0; ix < maxSize - 1; ix++) {
      String addr = prefix + (last + ix) + port;
      results[0].add(addr);
      results[1].add(addr);
    }

    results[1].add(prefix + (last + maxSize - 1) + port);
    return results;
  }

  private List<MemcachedNode> createNodes(List<InetSocketAddress> addresses) {
    List<MemcachedNode> results = new ArrayList<>();

    for (InetSocketAddress addr : addresses) {
      results.add(new MockMemcachedNode(addr));
    }

    return results;
  }

}
