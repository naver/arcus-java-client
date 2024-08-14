package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Small test program that does a bunch of sets in a tight loop.
 */
public final class DoLotsOfSets {

  private DoLotsOfSets() {
  }

  public static void main(String[] args) throws Exception {
    // Create a client with a queue big enough to hold the 300,000 items
    // we're going to add.
    MemcachedClient client = new MemcachedClient(
            new DefaultConnectionFactory(350000, 32768),
            AddrUtil.getAddresses("localhost:11211"));
    int count = 300000;
    long start = System.currentTimeMillis();
    byte[] toStore = new byte[26];
    Arrays.fill(toStore, (byte) 'a');
    List<Future<Boolean>> futures = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      futures.add(client.set("k" + i, 3600, toStore));
    }
    long added = System.currentTimeMillis();
    System.err.printf("Finished queuing in %sms%n", added - start);
    for (int i = 0; i < count; i++) {
      Future<Boolean> future = futures.get(i);
      assertTrue("k" + i + " is not stored.", future.get(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
    }
    long end = System.currentTimeMillis();
    System.err.printf("Completed everything in %sms (%sms to flush)%n",
            end - start, end - added);
    Map<String, Object> m = client.getBulk(Arrays.asList("k1", "k2", "k3", "k4", "k5",
            "k299999", "k299998", "k299997", "k299996"));
    assertEquals("Expected 9 results, got " + m, m.size(), 9);
    client.shutdown();
  }
}
