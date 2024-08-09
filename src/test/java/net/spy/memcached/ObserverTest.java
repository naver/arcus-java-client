package net.spy.memcached;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.compat.SpyObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test observer hooks.
 */
public class ObserverTest extends ClientBaseCase {

  @BeforeEach
  public void setup() throws Exception {
    super.setUp();
  }

  @AfterEach
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testConnectionObserver() throws Exception {
    ConnectionObserver obs = new LoggingObserver();
    assertTrue(client.addObserver(obs), "Didn't add observer.");
    assertTrue(client.removeObserver(obs), "Didn't remove observer.");
    assertFalse(client.removeObserver(obs), "Removed observer more than once.");
  }

  @Test
  public void testInitialObservers() throws Exception {
    assumeTrue(!USE_ZK);
    assertTrue(client.shutdown(5, TimeUnit.SECONDS),
            "Couldn't shut down within five seconds");

    final CountDownLatch latch = new CountDownLatch(1);
    final ConnectionObserver obs = new ConnectionObserver() {

      public void connectionEstablished(SocketAddress sa,
                                        int reconnectCount) {
        latch.countDown();
      }

      public void connectionLost(SocketAddress sa) {
        assert false : "Should not see this.";
      }

    };

    // Get a new client
    initClient(new DefaultConnectionFactory() {
      @Override
      public Collection<ConnectionObserver> getInitialObservers() {
        return Collections.singleton(obs);
      }
    });

    assertTrue(latch.await(2, TimeUnit.SECONDS),
            "Didn't detect connection");
    assertTrue(client.removeObserver(obs), "Did not install observer.");
    assertFalse(client.removeObserver(obs), "Didn't clean up observer.");
  }

  static class LoggingObserver extends SpyObject
          implements ConnectionObserver {
    public void connectionEstablished(SocketAddress sa,
                                      int reconnectCount) {
      getLogger().info("Connection established to %s (%s)",
              sa, reconnectCount);
    }

    public void connectionLost(SocketAddress sa) {
      getLogger().info("Connection lost from %s", sa);
    }

  }

}
