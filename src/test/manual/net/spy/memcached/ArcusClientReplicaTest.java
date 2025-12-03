package net.spy.memcached;

import java.util.Map;
import java.util.concurrent.Future;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.OperationStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ArcusClientReplicaTest {

  private ArcusClientPool arcusClientPool;
  private static final int POOL_SIZE = 4;
  private static final String KEY = "test:replica:key";
  private static final String VALUE = "replica-value";
  private static final int REPLICA_COUNT = 3;
  private static final int EXPIRE_TIME = 60;

  @BeforeEach
  protected void setUp() throws Exception {
    // This test assumes we use ZK
    assumeTrue(BaseIntegrationTest.USE_ZK);

    arcusClientPool = ArcusClient.createArcusClientPool(
            BaseIntegrationTest.ZK_ADDRESS,
            BaseIntegrationTest.SERVICE_CODE,
            POOL_SIZE
    );

    arcusClientPool.flush().get();
  }

  @Test
  void testReplicaSetAndGet_Sync() throws Exception {

    Future<Map<String, OperationStatus>> future =
            arcusClientPool.setReplicas(KEY, REPLICA_COUNT, EXPIRE_TIME, VALUE);
    Map<String, OperationStatus> result = future.get();

    // result.size(): fail request count
    assertEquals(0, result.size());

    Object gotValue = arcusClientPool.getFromReplica(KEY, REPLICA_COUNT);
    assertNotNull(gotValue);
    assertEquals(VALUE, gotValue);

    for (int i = 0; i < REPLICA_COUNT; i++) {
      Object rValue = arcusClientPool.get(KEY + "#" + i);
      assertNotNull(rValue);
      assertEquals(VALUE, rValue);
    }
  }

  @Test
  void testReplicaSetAndGet_Async() throws Exception {
    Future<Map<String, OperationStatus>> future =
            arcusClientPool.setReplicas(KEY, REPLICA_COUNT, EXPIRE_TIME, VALUE);
    Map<String, OperationStatus> result = future.get();

    assertEquals(0, result.size());

    Object asyncGotValue = arcusClientPool.asyncGetFromReplica(KEY, REPLICA_COUNT).get();

    assertNotNull(asyncGotValue);
    assertEquals(VALUE, asyncGotValue);
  }

  @Test
  void testGetFromReplica_Miss() {
    Object value = arcusClientPool.getFromReplica(KEY, REPLICA_COUNT);
    assertNull(value);
  }

  @Test
  void testGetFromReplica_PartialHit() throws Exception {
    arcusClientPool.set(KEY + "#1", EXPIRE_TIME, VALUE).get();
    Object gotValue = arcusClientPool.getFromReplica(KEY, REPLICA_COUNT);

    assertNotNull(gotValue);
    assertEquals(VALUE, gotValue);
  }

  @Test
  void testReplicaUpdate() throws Exception {
    arcusClientPool.setReplicas(KEY, REPLICA_COUNT, EXPIRE_TIME, "initial-value").get();

    String newValue = "updated-value";
    arcusClientPool.setReplicas(KEY, REPLICA_COUNT, EXPIRE_TIME, newValue).get();

    Object gotValue = arcusClientPool.getFromReplica(KEY, REPLICA_COUNT);
    assertEquals(newValue, gotValue);

    for (int i = 0; i < REPLICA_COUNT; i++) {
      assertEquals(newValue, arcusClientPool.get(KEY + "#" + i));
    }
  }
}
