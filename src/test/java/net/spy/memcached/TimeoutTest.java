package net.spy.memcached;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class TimeoutTest {
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
            AddrUtil.getAddresses(Collections.singletonList("127.0.0.1:64213")));
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
  void testCasTimeout() {
    tryTimeout("cas", () -> client.cas("k", 1, "blah"));
  }

  @Test
  void testGetsTimeout() {
    tryTimeout("gets", () -> client.gets("k"));
  }

  @Test
  void testGetTimeout() {
    tryTimeout("get", () -> client.get("k"));
  }

  @Test
  void testGetBulkTimeout() {
    tryTimeout("getbulk", () -> client.getBulk(Arrays.asList("k", "k2")));
  }

  @Test
  void testIncrTimeout() {
    tryTimeout("incr", () -> client.incr("k", 1));
  }

  @Test
  void testIncrWithDefTimeout() {
    tryTimeout("incrWithDef", () -> client.incr("k", 1, 5));
  }

}
