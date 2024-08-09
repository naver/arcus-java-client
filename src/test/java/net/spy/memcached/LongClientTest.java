package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import net.spy.memcached.compat.SyncThread;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Longer running test case.
 */
public class LongClientTest extends ClientBaseCase {

  @Test
  public void testParallelGet() throws Throwable {
    // Get a connection with the get optimization disabled.
    client.shutdown();
    initClient(new DefaultConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 15000;
      }

      @Override
      public boolean shouldOptimize() {
        return false;
      }
    });

    // Throw in some seed data.
    byte data[] = new byte[32768];
    Random r = new Random();
    r.nextBytes(data);
    final int hashcode = Arrays.hashCode(data);
    final List<String> keys = new ArrayList<>(50);
    for (int i = 0; i < 50; i++) {
      keys.add("k" + i);
      assertTrue(client.set(keys.get(i), 3600, data).get());
    }

    int cnt = SyncThread.getDistinctResultCount(25, new Callable<Integer>() {
      public Integer call() throws Exception {
        for (int i = 0; i < 25; i++) {
          Map<String, Object> m = client.getBulk(keys);
          for (String s : keys) {
            byte b[] = (byte[]) m.get(s);
            assert Arrays.hashCode(b) == hashcode
                    : "Expected " + hashcode + " was "
                    + Arrays.hashCode(b);
          }
        }
        return hashcode;
      }
    });
    assertEquals(cnt, 25);
  }
}
