package net.spy.memcached.test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;

/**
 * This expects a server on port 11212 that's somewhat unstable so it can report
 * and what-not.
 */
final class ObserverToy {

  private ObserverToy() {
  }

  public static void main(String[] args) throws Exception {
    final ConnectionObserver obs = new ConnectionObserver() {
      public void connectionEstablished(MemcachedNode node,
                                        int reconnectCount) {
        System.out.println("*** Established:  " + node + " count="
                + reconnectCount);
      }

      public void connectionLost(MemcachedNode node) {
        System.out.println("*** Lost connection:  " + node);
      }

    };

    MemcachedClient client = new MemcachedClient(new DefaultConnectionFactory() {

      @Override
      public Collection<ConnectionObserver> getInitialObservers() {
        return Collections.singleton(obs);
      }

      @Override
      public boolean isDaemon() {
        return false;
      }

    }, AddrUtil.getAddresses(Collections.singletonList("localhost:11212")));

    while (true) {
      try {
        client.asyncGet("ObserverToy").get(1, TimeUnit.SECONDS);
        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
