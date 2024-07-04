package net.spy.memcached.collection.map;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.Assert;

public class MopUpsertTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String MKEY = "upsertTestMkey";

  private final String VALUE = "VALUE";
  private final String NEW_VALUE = "NEWVALUE";

  private final CollectionAttributes collectionAttributes = new CollectionAttributes();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assert.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  public void testUpsertIfKeyExists() {
    try {
      // given
      Assert.assertTrue(mc.asyncMopInsert(KEY, MKEY, VALUE, collectionAttributes).get());

      // when
      CollectionFuture<Boolean> future =
              mc.asyncMopUpsert(KEY, MKEY, NEW_VALUE, collectionAttributes);
      Boolean result = future.get();
      CollectionOperationStatus operationStatus = future.getOperationStatus();

      // then
      assertTrue(result);
      assertEquals(CollectionResponse.REPLACED, operationStatus.getResponse());
      Map<String, Object> map = mc.asyncMopGet(KEY, false, false)
              .get(1000, TimeUnit.MILLISECONDS);
      assertEquals(NEW_VALUE, map.get(MKEY));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testUpsertIfKeyDoesNotExist() {
    try {
      // given & when
      CollectionFuture<Boolean> future =
              mc.asyncMopUpsert(KEY, MKEY, "VALUE", collectionAttributes);
      Boolean result = future.get();
      CollectionOperationStatus operationStatus = future.getOperationStatus();

      // then
      Assert.assertTrue("Upsert failed", result);
      assertEquals(CollectionResponse.CREATED_STORED, operationStatus.getResponse());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
