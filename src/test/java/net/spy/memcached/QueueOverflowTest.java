package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test queue overflow.
 */

/**
 * The code has been modified so that no overflow occurs.
 * So this test is no longer necessary.
 */
@Disabled
class QueueOverflowTest extends ClientBaseCase {

  @Override
  protected void initClient() throws Exception {

    // We're creating artificially constrained queues with the explicit
    // goal of overrunning them to verify the client will still be
    // functional after such conditions occur.
    int opQueueLen = 5;
    initClient(new ConnectionFactoryBuilder()
            .setOpQueueFactory(() -> new ArrayBlockingQueue<>(opQueueLen))
            .setReadOpQueueFactory(() -> new ArrayBlockingQueue<>((int) (1.1 * opQueueLen)))
            .setWriteOpQueueFactory(() -> new ArrayBlockingQueue<>(opQueueLen))
            .setOpTimeout(1000)
            .setShouldOptimize(false)
            .setOpQueueMaxBlockTime(0)
            .setReadBufferSize(1024));
  }

  private void runOverflowTest(byte b[]) throws Exception {
    Collection<Future<Boolean>> c = new ArrayList<>();
    try {
      for (int i = 0; i < 1000; i++) {
        c.add(client.set("k" + i, 0, b));
      }
      fail("Didn't catch an illegal state exception");
    } catch (IllegalStateException e) {
      // expected
    }
    try {
      Thread.sleep(50);
      for (Future<Boolean> f : c) {
        f.get(1, TimeUnit.SECONDS);
      }
    } catch (TimeoutException e) {
      // OK, at least we got one back.
    } catch (ExecutionException e) {
      // OK, at least we got one back.
    }
    Thread.sleep(500);
    assertTrue(client.set("kx", 0, "woo").get(10, TimeUnit.SECONDS),
            "Was not able to set a key after failure.");
  }

  @Test
  void testOverflowingInputQueue() throws Exception {
    runOverflowTest(new byte[]{1});
  }

  @Test
  void testOverflowingWriteQueue() throws Exception {
    byte[] b = new byte[8192];
    Random r = new Random();
    r.nextBytes(b);
    runOverflowTest(b);
  }

  @Test
  void testOverflowingReadQueue() throws Exception {
    byte[] b = new byte[8192];
    Random r = new Random();
    r.nextBytes(b);
    assertTrue(client.set("x", 0, b).get());

    Collection<Future<Object>> c = new ArrayList<>();
    try {
      for (int i = 0; i < 1000; i++) {
        c.add(client.asyncGet("x"));
      }
      fail("Didn't catch an illegal state exception");
    } catch (IllegalStateException e) {
      // expected
    }
    try {
      Thread.sleep(50);
      for (Future<Object> f : c) {
        assertTrue(Arrays.equals(b,
                (byte[]) f.get(5, TimeUnit.SECONDS)));
      }
    } catch (TimeoutException e) {
      // OK, just want to make sure the client doesn't crash
    } catch (ExecutionException e) {
      // OK, at least we got one back.
    }
    Thread.sleep(500);
    assertTrue(client.set("kx", 0, "woo").get(5, TimeUnit.SECONDS));
  }
}
