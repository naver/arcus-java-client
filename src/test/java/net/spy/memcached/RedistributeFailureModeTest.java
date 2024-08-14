package net.spy.memcached;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RedistributeFailureModeTest extends ClientBaseCase {

  private String serverList;

  @Override
  protected void setUp() throws Exception {
    serverList = ARCUS_HOST + " 127.0.0.1:11311";
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    serverList = ARCUS_HOST;
    super.tearDown();
  }

  @Override
  protected void initClient(ConnectionFactory cf) throws Exception {
    client = new MemcachedClient(cf, AddrUtil.getAddresses(serverList));
  }

  @Override
  protected void initClient() throws Exception {
    initClient(new DefaultConnectionFactory() {
      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Redistribute;
      }
    });
  }

  @Override
  protected void flushPause() throws InterruptedException {
    Thread.sleep(100);
  }

  // Just to make sure the sequence is being handled correctly
  public void testMixedSetsAndUpdates() throws Exception {
    int keySize = 100;
    List<String> keys = new ArrayList<>(keySize);
    List<Future<Boolean>> setFutures = new ArrayList<>(keySize);
    List<Future<Boolean>> addFutures = new ArrayList<>(keySize);
    Thread.sleep(100);

    for (int i = 0; i < keySize; i++) {
      String key = "k" + i;
      setFutures.add(client.set(key, 60, key));
      addFutures.add(client.add(key, 60, "a" + i));
      keys.add(key);
    }
    for (int i = 0; i < keySize; i++) {
      assertTrue(setFutures.get(i).get(10, TimeUnit.MILLISECONDS));
      assertFalse(addFutures.get(i).get(10, TimeUnit.MILLISECONDS));
    }

    Map<String, Object> m = client.getBulk(keys);
    assertEquals(keySize, m.size());
    for (Map.Entry<String, Object> me : m.entrySet()) {
      assertEquals(me.getKey(), me.getValue());
    }
    System.err.println(getName() + " complete.");
  }
}
