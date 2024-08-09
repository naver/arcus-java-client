package net.spy.memcached;

import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

public class TimeoutTest {
  private MemcachedClient client = null;

  @BeforeEach
  protected void setUp() throws Exception {
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

  @AfterEach
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
  }

  private void tryTimeout(String name, Runnable r) {
    try {
      r.run();
      fail("Expected timeout in " + name);
    } catch (OperationTimeoutException e) {
      // pass
    }
  }

  @Test
  public void testCasTimeout() {
    tryTimeout("cas", new Runnable() {
      public void run() {
        client.cas("k", 1, "blah");
      }
    });
  }

  @Test
  public void testGetsTimeout() {
    tryTimeout("gets", new Runnable() {
      public void run() {
        client.gets("k");
      }
    });
  }

  @Test
  public void testGetTimeout() {
    tryTimeout("get", new Runnable() {
      public void run() {
        client.get("k");
      }
    });
  }

  @Test
  public void testGetBulkTimeout() {
    tryTimeout("getbulk", new Runnable() {
      public void run() {
        client.getBulk(Arrays.asList("k", "k2"));
      }
    });
  }

  @Test
  public void testIncrTimeout() {
    tryTimeout("incr", new Runnable() {
      public void run() {
        client.incr("k", 1);
      }
    });
  }

  @Test
  public void testIncrWithDefTimeout() {
    tryTimeout("incrWithDef", new Runnable() {
      public void run() {
        client.incr("k", 1, 5);
      }
    });
  }

}
