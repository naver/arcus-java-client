package net.spy.memcached;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArcusKetamaHashingTest {

  @Test
  void testSmallSet() {
    List<String>[] stringNodes = generateAddresses(3);
    runThisManyNodes(stringNodes[0], stringNodes[1]);
  }

  @Test
  void testLargeSet() {
    List<String>[] stringNodes = generateAddresses(100);
    runThisManyNodes(stringNodes[0], stringNodes[1]);
  }

  @Test
  void testHashCollision() {
    int maxIdx = 103;
    List<String> sb = new ArrayList<>(maxIdx + 6);
    for (int i = 0; i <= maxIdx; i++) {
      sb.add("28.67.1." + i + ":11211 ");
    }

    /* Both stringNode1 and stringNode2 do not cause hash collision */
    List<String> stringNode1 = new ArrayList<>(sb);
    sb.add("28.67.1." + (++maxIdx) + ":11211 ");
    List<String> stringNode2 = sb;
    runThisManyNodes(stringNode1, stringNode2);

    /* stringNode1 does not cause hash collision
     * stringNode2 causes hash collision
     */
    sb.add("28.67.1." + (++maxIdx) + ":11211 ");
    stringNode1 = new ArrayList<>(sb);
    sb.add("28.67.1." + (++maxIdx) + ":11211 ");
    stringNode2 = new ArrayList<>(sb);
    runThisManyNodes(stringNode1, stringNode2);

    /* Both stringNode1 and stringNode2 cause hash collision */
    sb.add("28.67.1." + (++maxIdx) + ":11211 ");
    stringNode1 = new ArrayList<>(sb);
    sb.add("28.67.1." + (++maxIdx) + ":11211 ");
    stringNode2 = new ArrayList<>(sb);
    runThisManyNodes(stringNode1, stringNode2);
  }

  /**
   * Simulate dropping from stringNode1 to stringNode2.
   * Ensure hashing is consistent between the the two scenarios.
   *
   * @param stringNode1
   * @param stringNode2
   */
  private void runThisManyNodes(List<String> stringNode1, List<String> stringNode2) {
    List<MemcachedNode> smaller = createNodes(
            AddrUtil.getAddresses(stringNode1));
    List<MemcachedNode> larger = createNodes(
            AddrUtil.getAddresses(stringNode2));

    assertTrue(larger.containsAll(smaller));
    MemcachedNode oddManOut = larger.get(larger.size() - 1);
    assertFalse(smaller.contains(oddManOut));

    ArcusKetamaNodeLocator lgLocator = new ArcusKetamaNodeLocator(larger);
    ArcusKetamaNodeLocator smLocator = new ArcusKetamaNodeLocator(smaller);

    SortedMap<Long, SortedSet<MemcachedNode>> lgMap = lgLocator.getKetamaNodes();
    SortedMap<Long, SortedSet<MemcachedNode>> smMap = smLocator.getKetamaNodes();

    // Verify that EVERY entry in the smaller map has an equivalent
    // mapping in the larger map.
    boolean failed = false;
    for (final Long key : smMap.keySet()) {
      final SortedSet<MemcachedNode> largeSet = lgMap.get(key);
      final SortedSet<MemcachedNode> smallSet = smMap.get(key);

      if (!largeSet.containsAll(smallSet)) {
        failed = true;
        System.out.println("---------------");
        System.out.println("Key: " + key);
        System.out.print("small:");
        for (MemcachedNode node : smallSet) {
          System.out.print(" " + node.getSocketAddress());
        }
        System.out.println();
        System.out.print("large:");
        for (MemcachedNode node : largeSet) {
          System.out.print(" " + node.getSocketAddress());
        }
        System.out.println();
      }
    }
    assertFalse(failed);

    for (final Map.Entry<Long, SortedSet<MemcachedNode>> entry : lgMap.entrySet()) {
      final Long key = entry.getKey();
      final SortedSet<MemcachedNode> node = entry.getValue();
      if (node.contains(oddManOut)) {
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
