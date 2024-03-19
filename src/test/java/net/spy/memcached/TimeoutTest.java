package net.spy.memcached;

import junit.framework.TestCase;

import java.util.Arrays;

public class TimeoutTest extends TestCase {
  private MemcachedClient client = null;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    client = new MemcachedClient(new DefaultConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 1;
      }

      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Retry;
      }
    },
            AddrUtil.getAddresses("127.0.0.1:64213"));
  }

  @Override
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
    super.tearDown();
  }

  private void tryTimeout(String name, Runnable r) {
    try {
      r.run();
      fail("Expected timeout in " + name);
    } catch (OperationTimeoutException e) {
      // pass
    }
  }

  public void testCasTimeout() {
    tryTimeout("cas", new Runnable() {
      public void run() {
        client.cas("k", 1, "blah");
      }
    });
  }

  public void testGetsTimeout() {
    tryTimeout("gets", new Runnable() {
      public void run() {
        client.gets("k");
      }
    });
  }

  public void testGetTimeout() {
    tryTimeout("get", new Runnable() {
      public void run() {
        client.get("k");
      }
    });
  }

  public void testGetBulkTimeout() {
    tryTimeout("getbulk", new Runnable() {
      public void run() {
        client.getBulk(Arrays.asList("k", "k2"));
      }
    });
  }

  public void testIncrTimeout() {
    tryTimeout("incr", new Runnable() {
      public void run() {
        client.incr("k", 1);
      }
    });
  }

  public void testIncrWithDefTimeout() {
    tryTimeout("incrWithDef", new Runnable() {
      public void run() {
        client.incr("k", 1, 5);
      }
    });
  }

}
