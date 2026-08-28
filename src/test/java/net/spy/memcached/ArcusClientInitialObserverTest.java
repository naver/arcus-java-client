package net.spy.memcached;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArcusClientInitialObserverTest {

  private ArcusClient client;

  @AfterEach
  void tearDown() {
    if (client != null) {
      client.shutdown();
    }
  }

  @Test
  void test() {
    // given
    CountDownLatch latch = new CountDownLatch(1);

    ConnectionObserver observer = new ConnectionObserver() {
      @Override
      public void connectionEstablished(MemcachedNode node, int reconnectCount) {
        latch.countDown();
      }

      @Override
      public void connectionLost(MemcachedNode node) {
        // do-nothing.
      }
    };

    // when
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder()
        .setInitialObservers(Collections.singletonList(observer));

    client = ArcusClient.createArcusClient(
        "127.0.0.1:2181",
        "test",
        cfb
    );

    Collection<ConnectionObserver> configuredObservers = cfb.build().getInitialObservers();

    // then
    assertAll(
        () -> assertTrue(latch.await(700, TimeUnit.MILLISECONDS)),
        () -> assertTrue(client.removeObserver(observer)),
        () -> assertEquals(1, configuredObservers.size()),
        () -> assertTrue(configuredObservers.contains(observer))
    );
  }
}
