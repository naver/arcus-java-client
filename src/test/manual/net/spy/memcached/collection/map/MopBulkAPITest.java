package net.spy.memcached.collection.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import net.spy.memcached.collection.*;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

public class MopBulkAPITest extends BaseIntegrationTest {

    private String key = "MopBulkAPITest33";
    Map<Object, Object> elements = new HashMap<Object, Object>();
    List<MapField<Object>> updateMap = new ArrayList<MapField<Object>>();


    private int getValueCount() {
        return mc.getMaxPipedItemCount();
    }

    protected void setUp() throws Exception {
        super.setUp();
        for (long i = 0; i < getValueCount(); i++) {
            elements.put("field" + String.valueOf(i),
                        "value" + String.valueOf(i));
            MapField<Object> temp =
                    new MapField<Object>("field" + String.valueOf(i),
                                    "newvalue" + String.valueOf(i));
            updateMap.add(temp);
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testBulk() throws Exception {
        for (int i = 0; i < 10; i++) {
            mc.asyncMopDelete(key, 0, true).get(1000,
                    TimeUnit.MILLISECONDS);
            bulk();
        }
    }

    public void bulk() {
        try {
            Future<Map<Integer, CollectionOperationStatus>> future = mc
                    .asyncMopPipedInsertBulk(key, elements,
                            new CollectionAttributes());

            Map<Integer, CollectionOperationStatus> map = future.get(10000,
                    TimeUnit.MILLISECONDS);

            if (!map.isEmpty()) { // (4)
                System.out.println("일부 item이 insert 실패 하였음.");

                for (Map.Entry<Integer, CollectionOperationStatus> entry : map.entrySet()) {
                    System.out.print("실패한 아이템=" + elements.get(entry.getKey()));
                    System.out.println(", 실패원인=" + entry.getValue().getResponse());
                }
            } else {
                System.out.println("모두 insert 성공함.");
            }

            Map<String, Object> rmap = mc.asyncMopGet(key, 0, false,
                    false).get();
            assertEquals(getValueCount(), rmap.size());
            assertEquals(0, map.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    public void testBulkFailed() {
        try {
            mc.asyncMopDelete(key, 0, true).get(1000,
                    TimeUnit.MILLISECONDS);

            mc.asyncMopInsert(key, "field1", "value1", new CollectionAttributes())
                    .get();

            mc.asyncSetAttr(key, new CollectionAttributes(0, 1L, CollectionOverflowAction.error)).get();

            CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
                    .asyncMopPipedInsertBulk(key, elements,
                            new CollectionAttributes());

            Map<Integer, CollectionOperationStatus> map = future.get(10000,
                    TimeUnit.MILLISECONDS);

            assertEquals(getValueCount(), map.size());
            assertFalse(future.getOperationStatus().isSuccess());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    public void testBulkEmptyElements() {
        try {
            CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
                    .asyncMopPipedInsertBulk(key, new HashMap<Object, Object>(),
                            new CollectionAttributes());

            future.get(10000, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            return;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        Assert.fail();
    }

    public void testUpdateBulk() {
        try {
            mc.asyncMopDelete(key, 0, true).get(1000,
                    TimeUnit.MILLISECONDS);

            Future<Map<Integer, CollectionOperationStatus>> future = mc
                    .asyncMopPipedInsertBulk(key, elements,
                            new CollectionAttributes());

            Map<Integer, CollectionOperationStatus> map = future.get(10000,
                    TimeUnit.MILLISECONDS);

            CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 = mc
                    .asyncMopPipedUpdateBulk(key, updateMap);

            Map<Integer, CollectionOperationStatus> map2 = future2.get(10000,
                    TimeUnit.MILLISECONDS);

            Map<String, Object> rmap = mc.asyncMopGet(key, 0, false, false).get();
            assertEquals(getValueCount(), rmap.size());
            assertEquals(0, map.size());
            assertEquals(0, map2.size());

            assertEquals("newvalue1", rmap.get("field1"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
