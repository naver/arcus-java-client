package net.spy.memcached;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ArcusClientPoolCreateTest {

  private static final int POOL_SIZE = 3;
  private static final int CACHE_NODE_SIZE = 3;

  @BeforeEach
  public void setUp() throws Exception {
    // This test assumes we use ZK
    assumeTrue(BaseIntegrationTest.USE_ZK);
  }

  @Test
  public void testCreateClientPool() throws NoSuchFieldException, IllegalAccessException {
    //Create first ArcusClientPool instance.
    ArcusClientPool clientPool = ArcusClient.createArcusClientPool(BaseIntegrationTest.ZK_ADDRESS,
            BaseIntegrationTest.SERVICE_CODE, POOL_SIZE);

    ArcusClient[] allClients = clientPool.getAllClients();

    Assertions.assertEquals(allClients.length, POOL_SIZE);
    for (int i = 0 ; i < POOL_SIZE ; i++) {
      Assertions.assertEquals(allClients[i].getMemcachedConnection().getLocator().getAll().size(),
              CACHE_NODE_SIZE);
      Collection<MemcachedNode> nodes =
              allClients[i].getMemcachedConnection().getLocator().getAll();
      for (MemcachedNode mc : nodes) {
        Pattern compile =
                Pattern.compile("ArcusClient-(\\d+)\\(" + (i + 1) + "-" + POOL_SIZE + "\\)");
        Matcher matcher = compile.matcher(mc.getNodeName().split(" ")[0]);
        Assertions.assertTrue(matcher.matches());
      }
    }

    int firstPoolId = getPoolId();

    //Create second ArcusClientPool instance.
    clientPool = ArcusClient.createArcusClientPool(BaseIntegrationTest.ZK_ADDRESS,
            BaseIntegrationTest.SERVICE_CODE, new ConnectionFactoryBuilder(), POOL_SIZE);

    allClients = clientPool.getAllClients();

    Assertions.assertEquals(allClients.length, POOL_SIZE);
    for (int i = 0 ; i < POOL_SIZE ; i++) {
      Assertions.assertEquals(allClients[i].getMemcachedConnection().getLocator().getAll().size(),
              CACHE_NODE_SIZE);
      Collection<MemcachedNode> nodes =
              allClients[i].getMemcachedConnection().getLocator().getAll();
      for (MemcachedNode mc : nodes) {
        Pattern compile =
                Pattern.compile("ArcusClient-(\\d+)\\(" + (i + 1) + "-" + POOL_SIZE + "\\)");
        Matcher matcher = compile.matcher(mc.getNodeName().split(" ")[0]);
        Assertions.assertTrue(matcher.matches());
      }
    }

    int secondPoolId = getPoolId();
    Assertions.assertTrue(secondPoolId > firstPoolId);
  }

  private int getPoolId() throws NoSuchFieldException, IllegalAccessException {
    Class<CacheManager> clazz = CacheManager.class;
    Field poolId = clazz.getDeclaredField("POOL_ID");
    poolId.setAccessible(true);
    AtomicInteger atomicInteger = (AtomicInteger) poolId.get(null);
    return atomicInteger.get();
  }
}
