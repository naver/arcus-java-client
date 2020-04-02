package net.spy.memcached;

import junit.framework.TestCase;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CancelFailureModeTest extends TestCase {
  private String serverList= "127.0.0.1:11311";
  private MemcachedClient client = null;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    client = new MemcachedClient(new DefaultConnectionFactory() {
      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Cancel;
      }
    }, AddrUtil.getAddresses(serverList));
  }

  @Override
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
    super.tearDown();
  }

  public void testQueueingToDownServer() throws Exception {
    Future<Boolean> f = client.add("someKey", 0, "some object");
    try {
      boolean b = f.get();
      fail("Should've thrown an exception, returned " + b);
    } catch (ExecutionException e) {
      // probably OK
    }
    assertTrue(f.isCancelled());
  }
}
