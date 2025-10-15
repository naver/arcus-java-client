package net.spy.memcached;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.internal.OperationFuture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FutureListenerTest extends ClientBaseCase {
  @Test
  void testListener() throws Exception {
    final String key = "listener-test-key";
    final String value = "listener-test-value";
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Future<?>> futureInListener = new AtomicReference<>();
    OperationFuture<Boolean> future = client.set(key, 60, value);
    future.addListener(f -> {
      futureInListener.set(f);
      latch.countDown();
    });

    assertTrue(latch.await(1, TimeUnit.SECONDS));
    Future<?> completedFuture = futureInListener.get();
    assertNotNull(completedFuture);
    assertTrue(completedFuture.isDone());
    assertEquals(value, client.get(key));
  }
}
