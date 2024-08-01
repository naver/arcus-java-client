package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

/**
 * Base class for cancellation tests.
 */
public abstract class CancellationBaseCase extends TestCase {
  private static final String serverList = "127.0.0.1:64213";
  private MemcachedClient client = null;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    initClient();
  }

  @Override
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
    super.tearDown();
  }

  protected void initClient() throws Exception {
    initClient(new DefaultConnectionFactory() {
      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Retry;
      }
    });
  }

  protected void initClient(ConnectionFactory cf) throws Exception {
    client = new MemcachedClient(cf, AddrUtil.getAddresses(serverList));
  }

  private void tryCancellation(Future<?> f) throws Exception {
    f.cancel(true);
    assertTrue(f.isCancelled());
    assertTrue(f.isDone());
    try {
      Object o = f.get();
      fail("Expected cancellation, got " + o);
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("Cancelled"));
    }
  }

  public void testAvailableServers() throws Exception {
    tryTimeout(client.asyncGet("x"));
    assertEquals(Collections.emptyList(), client.getAvailableServers());
  }

  public void testUnavailableServers() throws Exception {
    tryTimeout(client.asyncGet("x"));
    assertEquals(new ArrayList<>(
                    Collections.singleton("/127.0.0.1:64213")),
            ClientBaseCase.stringify(client.getUnavailableServers()));
  }

  private void tryTimeout(Future<?> f) throws Exception {
    try {
      Object o = f.get(10, TimeUnit.MILLISECONDS);
      fail("Expected timeout, got " + o);
    } catch (TimeoutException e) {
      // expected
    }
  }

  protected void tryTestSequence(Future<?> f) throws Exception {
    tryTimeout(f);
    tryCancellation(f);
  }

  public void testAsyncGetCancellation() throws Exception {
    tryTestSequence(client.asyncGet("k"));
  }

  public void testAsyncGetsCancellation() throws Exception {
    tryTestSequence(client.asyncGets("k"));
  }

  public void testAsyncGetBulkCancellationCollection() throws Exception {
    tryTestSequence(client.asyncGetBulk(Arrays.asList("k", "k2")));
  }

  public void testDeleteCancellation() throws Exception {
    tryTestSequence(client.delete("x"));
  }

  public void testflushCancellation() throws Exception {
    tryTestSequence(client.flush());
  }

  public void testDelayedflushCancellation() throws Exception {
    tryTestSequence(client.flush(3));
  }

  public void testReplaceCancellation() throws Exception {
    tryTestSequence(client.replace("x", 3, "y"));
  }

  public void testAddCancellation() throws Exception {
    tryTestSequence(client.add("x", 3, "y"));
  }

  public void testSetCancellation() throws Exception {
    tryTestSequence(client.set("x", 3, "y"));
  }

  public void testCASCancellation() throws Exception {
    tryTestSequence(client.asyncCAS("x", 3, "y"));
  }
}
