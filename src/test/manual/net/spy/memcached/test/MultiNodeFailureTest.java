package net.spy.memcached.test;

import java.util.Arrays;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

/**
 * This is an attempt to reproduce a problem where a server fails during a
 * series of gets.
 */
public final class MultiNodeFailureTest {

  private MultiNodeFailureTest() {
  }

  public static void main(String args[]) throws Exception {
    MemcachedClient c = new MemcachedClient(
            AddrUtil.getAddresses("localhost:11200 localhost:11201"));
    while (true) {
      for (int i = 0; i < 1000; i++) {
        try {
          c.getBulk(Arrays.asList("blah1", "blah2", "blah3", "blah4", "blah5"));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      System.out.println("Did a thousand.");
    }
  }

}
