package net.spy.memcached.bulkoperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.Assert;

public class BopPipeUpsertTest extends BaseIntegrationTest {
  private static final String KEY = BopPipeUpsertTest.class.getSimpleName();
  private static final String WRONG_KEY = "WRONG_KEY";

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

  public void testAsyncBopPipedUpsertBulkSuccess() throws Exception {
    //given
    Map<Integer, CollectionOperationStatus> result;
    int elementCount = 1000;
    Map<Long, Object> insertElem = new TreeMap<Long, Object>();
    Map<Long, Object> upsertElem = new TreeMap<Long, Object>();
    for (long i = 0; i < elementCount; i++) {
      if (i % 2 == 0) {
        insertElem.put(i, "value" + i);
      }
      upsertElem.put(i, "upsertValue" + i);
    }
    CollectionAttributes attr = new CollectionAttributes();
    mc.asyncBopPipedInsertBulk(KEY, insertElem, attr).get();

    //when
    result = mc.asyncBopPipedUpsertBulk(KEY, upsertElem, attr).get();

    //then
    Assert.assertTrue(result.isEmpty());
  }

  public void testWrongKeyFail() throws Exception {
    //given
    Map<Integer, CollectionOperationStatus> result;
    int elementCount = 100;
    Map<Long, Object> insertElem = new TreeMap<Long, Object>();
    Map<Long, Object> upsertElem = new TreeMap<Long, Object>();
    for (long i = 0; i < elementCount; i++) {
      if (i % 2 == 0) {
        insertElem.put(i, "value" + i);
      }
      upsertElem.put(i, "upsertValue" + i);
    }
    mc.asyncBopPipedInsertBulk(KEY, insertElem, new CollectionAttributes()).get();

    //when
    result = mc.asyncBopPipedUpsertBulk(WRONG_KEY, upsertElem, null).get();

    //then
    Assert.assertFalse(result.isEmpty());
    for (CollectionOperationStatus status : result.values()) {
      Assert.assertFalse(status.isSuccess());
      Assert.assertEquals(status.getMessage(), "NOT_FOUND");
    }
  }

  public void testWrongTypeFail() throws Exception {
    //given
    Map<Integer, CollectionOperationStatus> result;
    int elementCount = 100;
    Map<String, Object> insertElem = new TreeMap<String, Object>();
    Map<Long, Object> upsertElem = new TreeMap<Long, Object>();
    for (long i = 0; i < elementCount; i++) {
      if (i % 2 == 0) {
        insertElem.put("MKey" + i, i);
      }
      upsertElem.put(i, "value" + i);
    }

    CollectionAttributes collectionAttributes = new CollectionAttributes();
    mc.asyncMopPipedInsertBulk(KEY, insertElem, collectionAttributes).get();

    //when
    result = mc.asyncBopPipedUpsertBulk(KEY, upsertElem, collectionAttributes).get();

    //then
    Assert.assertFalse(result.isEmpty());
    for (CollectionOperationStatus status : result.values()) {
      Assert.assertFalse(status.isSuccess());
      Assert.assertEquals(status.getMessage(), "TYPE_MISMATCH");
    }
  }

  public void testWrongBKeyTypeFail() throws Exception {
    //given
    Map<Integer, CollectionOperationStatus> result;
    int elementCount = 100;
    List<Element<Object>> insertElem = new ArrayList<Element<Object>>();
    List<Element<Object>> upsertElem = new ArrayList<Element<Object>>();
    for (int i = 0; i < elementCount; i++) {
      if (i % 2 == 0) {
        insertElem.add(new Element<Object>(new byte[]{(byte) i},
                "value" + i,
                new byte[]{(byte) 1}));
      }
      upsertElem.add(new Element<Object>(i, "upsertValue" + i, new byte[]{(byte) 1}));
    }

    CollectionAttributes collectionAttributes = new CollectionAttributes();
    mc.asyncBopPipedUpsertBulk(KEY, insertElem, collectionAttributes).get();

    //when
    result = mc.asyncBopPipedUpsertBulk(KEY, upsertElem, collectionAttributes).get();

    //then
    Assert.assertFalse(result.isEmpty());
    for (CollectionOperationStatus status : result.values()) {
      Assert.assertFalse(status.isSuccess());
      Assert.assertEquals(status.getMessage(), "BKEY_MISMATCH");
    }
  }


  public void testBopGetByList() throws Exception {
    //given
    int elementCount = 1000;
    List<Element<Object>> insertElem = new ArrayList<Element<Object>>();
    List<Element<Object>> upsertElem = new ArrayList<Element<Object>>();
    for (int i = 0; i < elementCount; i++) {
      if (i % 2 == 0) {
        insertElem.add(new Element<Object>(i, "value" + i, new byte[]{(byte) 1}));
      }
      upsertElem.add(new Element<Object>(i, "upsertValue" + i, new byte[]{(byte) 1}));
    }
    CollectionAttributes attr = new CollectionAttributes();
    mc.asyncBopPipedInsertBulk(KEY, insertElem, attr).get();

    //when
    mc.asyncBopPipedUpsertBulk(KEY, upsertElem, attr).get();

    //then
    Map<Long, Element<Object>> upsertResult = mc.asyncBopGet(
                    KEY, 0, elementCount,
                    ElementFlagFilter.DO_NOT_FILTER,
                    0, 0, false, false)
            .get();
    Assert.assertEquals(elementCount, upsertResult.size());
    int idx = 0;
    for (Element<Object> elem : upsertResult.values()) {
      Assert.assertEquals(elem.getLongBkey(), idx);
      Assert.assertEquals(elem.getValue(), "upsertValue" + idx);
      idx++;
    }
  }


  public void testAllInsertByMap() throws Exception {
    //given
    int elementCount = 4000;
    Map<Long, Object> elements = new TreeMap<Long, Object>();
    for (long i = 0; i < elementCount; i++) {
      elements.put(i, "value" + i);
    }
    CollectionAttributes attr = new CollectionAttributes();

    //when
    mc.asyncBopPipedUpsertBulk(KEY, elements, attr).get();

    //then
    Map<Long, Element<Object>> resultMap
            = mc.asyncBopGet(KEY, 0, 5000,
            ElementFlagFilter.DO_NOT_FILTER, 0,
            0, false, false).get();
    Long idx = 0L;
    for (Map.Entry<Long, Element<Object>> entry : resultMap.entrySet()) {
      Assert.assertEquals(idx, entry.getKey());
      Assert.assertEquals("value" + idx, entry.getValue().getValue());
      idx++;
    }
  }

  public void testAllInsertByList() throws Exception {
    //given
    int elementCount = 4000;
    List<Element<Object>> elements = new ArrayList<Element<Object>>();
    for (int i = 0; i < elementCount; i++) {
      elements.add(new Element<Object>(i, "value" + i, new byte[]{(byte) 1}));
    }
    CollectionAttributes attr = new CollectionAttributes();

    //when
    mc.asyncBopPipedUpsertBulk(KEY, elements, attr).get();

    //then
    Map<Long, Element<Object>> resultMap
            = mc.asyncBopGet(KEY, 0, 5000,
            ElementFlagFilter.DO_NOT_FILTER, 0,
            0, false, false).get();
    Long idx = 0L;
    for (Map.Entry<Long, Element<Object>> entry : resultMap.entrySet()) {
      Assert.assertEquals(idx, entry.getKey());
      Assert.assertEquals("value" + idx, entry.getValue().getValue());
      idx++;
    }
  }

  public void testAllUpdateByMap() throws Exception {
    //given
    int elementCount = 500;
    Map<Long, Object> insertElem = new TreeMap<Long, Object>();
    Map<Long, Object> upsertElem = new TreeMap<Long, Object>();

    for (long i = 0; i < elementCount; i++) {
      insertElem.put(i, "value" + i);
      upsertElem.put(i, "upsertValue" + i);
    }

    CollectionAttributes attr = new CollectionAttributes();
    mc.asyncBopPipedInsertBulk(KEY, insertElem, attr).get(5000L, TimeUnit.MILLISECONDS);

    //when
    mc.asyncBopPipedUpsertBulk(KEY, upsertElem, attr);
    Map<Long, Element<Object>> resultMap
            = mc.asyncBopGet(KEY, 0, 5000,
                    ElementFlagFilter.DO_NOT_FILTER, 0,
                    0, false, false)
            .get();
    int idx = 0;
    for (Element<Object> elem : resultMap.values()) {
      Assert.assertEquals(elem.getLongBkey(), idx);
      Assert.assertEquals(elem.getValue(), "upsertValue" + idx);
      idx++;
    }
  }

  public void testAllUpdateByList() throws Exception {
    int elementCount = 500;
    List<Element<Object>> insertElem = new ArrayList<Element<Object>>();
    List<Element<Object>> upsertElem = new ArrayList<Element<Object>>();

    for (int i = 0; i < elementCount; i++) {
      insertElem.add(new Element<Object>(i, "value" + i, new byte[]{(byte) 1}));
      upsertElem.add(new Element<Object>(i, "upsertValue" + i, new byte[]{(byte) 1}));
    }

    CollectionAttributes attr = new CollectionAttributes();
    mc.asyncBopPipedInsertBulk(KEY, insertElem, attr).get(5000L, TimeUnit.MILLISECONDS);

    //when
    mc.asyncBopPipedUpsertBulk(KEY, upsertElem, attr);
    Map<Long, Element<Object>> resultMap
            = mc.asyncBopGet(KEY, 0, 5000,
            ElementFlagFilter.DO_NOT_FILTER, 0,
                    0, false, false)
            .get();
    int idx = 0;
    for (Element<Object> elem : resultMap.values()) {
      Assert.assertEquals(elem.getLongBkey(), idx);
      Assert.assertEquals(elem.getValue(), "upsertValue" + idx);
      idx++;
    }
  }
}
