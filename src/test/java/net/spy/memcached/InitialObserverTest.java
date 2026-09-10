package net.spy.memcached;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class InitialObserverTest {

  private ArcusClient client;

  @AfterEach
  void tearDown() {
    if (client != null) {
      client.shutdown();
    }
  }

  @Test
  void shouldNotifyNextObserverWhenConnectionEstablishedThrows()
      throws InterruptedException {
    // given
    CountDownLatch latch = new CountDownLatch(1);
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();

    cfb.addInitialObserver(new ConnectionObserver() {
      @Override
      public void connectionEstablished(MemcachedNode node, int reconnectCount) {
        throw new RuntimeException("Test exception in connectionEstablished");
      }

      @Override
      public void connectionLost(MemcachedNode node) {
        // do nothing
      }
    });

    cfb.addInitialObserver(new ConnectionObserver() {
      @Override
      public void connectionEstablished(MemcachedNode node, int reconnectCount) {
        latch.countDown();
      }

      @Override
      public void connectionLost(MemcachedNode node) {
        // do nothing
      }
    });

    // when
    client = ArcusClient.createArcusClient(
        "127.0.0.1:2181", "test", cfb
    );

    // then
    assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test
  void shouldNotifyNextObserverWhenConnectionLostThrows()
      throws IOException, InterruptedException {
    // given
    CountDownLatch latch = new CountDownLatch(1);
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();

    cfb.addInitialObserver(new ConnectionObserver() {
      @Override
      public void connectionEstablished(MemcachedNode node, int reconnectCount) {
        // do nothing
      }

      @Override
      public void connectionLost(MemcachedNode node) {
        throw new RuntimeException("Test exception in connectionLost");
      }
    });

    cfb.addInitialObserver(new ConnectionObserver() {
      @Override
      public void connectionEstablished(MemcachedNode node, int reconnectCount) {
        // do nothing
      }

      @Override
      public void connectionLost(MemcachedNode node) {
        latch.countDown();
      }
    });

    client = ArcusClient.createArcusClient("127.0.0.1:2181", "test", cfb);

    // when
    for (MemcachedNode node : client.getAllNodes()) {
      assertTrue(node.isConnected());
      node.getChannel().socket().shutdownInput();
    }
    client.asyncGet("observer-exception-test");

    // then
    assertTrue(latch.await(1, TimeUnit.SECONDS));
  }
}
