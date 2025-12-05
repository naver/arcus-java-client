package net.spy.memcached;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.APIType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ArcusShardKeyTest extends BaseIntegrationTest {

  @BeforeEach
  protected void setUp() throws Exception {
    assumeTrue(BaseIntegrationTest.USE_ZK);
    super.setUp();

    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    cfb.setArcusReplEnabled(true);
    cfb.enableShardKey(true);
    mc = ArcusClient.createArcusClient(ZK_ADDRESS, SERVICE_CODE, cfb);
  }

  @Test
  void testShardKey_SetAndGet() throws Exception {
    String key1 = "user:{groupA}:test1";
    String key2 = "user:{groupA}:test2";
    String key3 = "order:{groupA}:test3";
    String value1 = "test1";
    String value2 = "test2";
    String value3 = "test3";

    MemcachedNode node1 = mc.getMemcachedConnection().getPrimaryNode(key1, APIType.SET);
    MemcachedNode node2 = mc.getMemcachedConnection().getPrimaryNode(key2, APIType.SET);
    MemcachedNode node3 = mc.getMemcachedConnection().getPrimaryNode(key3, APIType.SET);

    assertSame(node1, node2);
    assertSame(node1, node3);

    assertTrue(mc.set(key1, 60, value1).get());
    assertTrue(mc.set(key2, 60, value2).get());
    assertTrue(mc.set(key3, 60, value3).get());

    assertEquals(value1, mc.get(key1));
    assertEquals(value2, mc.get(key2));
    assertEquals(value3, mc.get(key3));
  }
}
