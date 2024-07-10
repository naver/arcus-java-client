package net.spy.memcached;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

public class ArcusClientPoolCreateTest {

  private static final String SERVICE_CODE = "test";
  private static final int POOL_SIZE = 3;
  private static final int CACHE_NODE_SIZE = 3;

  @Before
  public void setUp() throws Exception {
    // This test assumes we use ZK
    assumeTrue(BaseIntegrationTest.USE_ZK);
  }

  @Test
  public void testCreateClientPool() throws NoSuchFieldException, IllegalAccessException {
    //Create first ArcusClientPool instance.
    ArcusClientPool clientPool = ArcusClient.createArcusClientPool(SERVICE_CODE,
            new ConnectionFactoryBuilder(), POOL_SIZE);

    ArcusClient[] allClients = clientPool.getAllClients();

    Assert.assertEquals(allClients.length, POOL_SIZE);
    for (int i = 0 ; i < POOL_SIZE ; i++) {
      Assert.assertEquals(allClients[i].getMemcachedConnection().getLocator().getAll().size(),
              CACHE_NODE_SIZE);
      Collection<MemcachedNode> nodes =
              allClients[i].getMemcachedConnection().getLocator().getAll();
      for (MemcachedNode mc : nodes) {
        Pattern compile =
                Pattern.compile("ArcusClient-(\\d+)\\(" + (i + 1) + "-" + POOL_SIZE + "\\)");
        Matcher matcher = compile.matcher(mc.getNodeName().split(" ")[0]);
        Assert.assertTrue(matcher.matches());
      }
    }

    int firstPoolId = getPoolId();

    //Create second ArcusClientPool instance.
    clientPool = ArcusClient.createArcusClientPool(SERVICE_CODE,
            new ConnectionFactoryBuilder(), POOL_SIZE);

    allClients = clientPool.getAllClients();

    Assert.assertEquals(allClients.length, POOL_SIZE);
    for (int i = 0 ; i < POOL_SIZE ; i++) {
      Assert.assertEquals(allClients[i].getMemcachedConnection().getLocator().getAll().size(),
              CACHE_NODE_SIZE);
      Collection<MemcachedNode> nodes =
              allClients[i].getMemcachedConnection().getLocator().getAll();
      for (MemcachedNode mc : nodes) {
        Pattern compile =
                Pattern.compile("ArcusClient-(\\d+)\\(" + (i + 1) + "-" + POOL_SIZE + "\\)");
        Matcher matcher = compile.matcher(mc.getNodeName().split(" ")[0]);
        Assert.assertTrue(matcher.matches());
      }
    }

    int secondPoolId = getPoolId();
    Assert.assertTrue(secondPoolId > firstPoolId);
  }

  private int getPoolId() throws NoSuchFieldException, IllegalAccessException {
    Class<ElasticCacheManager> clazz = ElasticCacheManager.class;
    Field poolId = clazz.getDeclaredField("POOL_ID");
    poolId.setAccessible(true);
    AtomicInteger atomicInteger = (AtomicInteger) poolId.get(null);
    return atomicInteger.get();
  }
}
