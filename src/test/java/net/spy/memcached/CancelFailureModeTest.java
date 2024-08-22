package net.spy.memcached;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CancelFailureModeTest {
  private String serverList = "127.0.0.1:11311";
  private MemcachedClient client = null;

  @BeforeEach
  protected void setUp() throws Exception {
    client = new MemcachedClient(new DefaultConnectionFactory() {
      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Cancel;
      }
    }, AddrUtil.getAddresses(serverList));
  }

  @AfterEach
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
  }

  @Test
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
