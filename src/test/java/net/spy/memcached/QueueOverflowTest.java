package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.ops.Operation;

import org.junit.Ignore;

/**
 * Test queue overflow.
 */

/**
 * The code has been modified so that no overflow occurs.
 * So this test is no longer necessary.
 */
@Ignore
public class QueueOverflowTest extends ClientBaseCase {

  @Override
  protected void initClient() throws Exception {

    // We're creating artificially constrained queues with the explicit
    // goal of overrunning them to verify the client will still be
    // functional after such conditions occur.
    initClient(new DefaultConnectionFactory(5, 1024) {
      @Override
      public long getOperationTimeout() {
        return 1000;
      }

      @Override
      public BlockingQueue<Operation> createOperationQueue() {
        return new ArrayBlockingQueue<>(getOpQueueLen());
      }

      @Override
      public BlockingQueue<Operation> createReadOperationQueue() {
        return new ArrayBlockingQueue<>(
                (int) (getOpQueueLen() * 1.1));
      }

      @Override
      public BlockingQueue<Operation> createWriteOperationQueue() {
        return createOperationQueue();
      }

      @Override
      public boolean shouldOptimize() {
        return false;
      }

      @Override
      public long getOpQueueMaxBlockTime() {
        return 0;
      }

    });
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
    assertTrue("Was not able to set a key after failure.",
            client.set("kx", 0, "woo").get(10, TimeUnit.SECONDS));
  }

  public void testOverflowingInputQueue() throws Exception {
    runOverflowTest(new byte[]{1});
  }

  public void testOverflowingWriteQueue() throws Exception {
    byte[] b = new byte[8192];
    Random r = new Random();
    r.nextBytes(b);
    runOverflowTest(b);
  }

  public void testOverflowingReadQueue() throws Exception {
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
