package net.spy.memcached.collection.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.OperationStatus;

public class MopServerMessageTest extends BaseIntegrationTest {

    private String key = "MopServerMessageTest";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mc.delete(key).get();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testNotFound() throws Exception {
        CollectionFuture<Map<String, Object>> future = (CollectionFuture<Map<String, Object>>) mc
                .asyncMopGet(key, 0, false, false);
        assertNull(future.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future.getOperationStatus();
        assertNotNull(status);
        assertEquals("NOT_FOUND", status.getMessage());
    }

    public void testCreatedStored() throws Exception {
        CollectionFuture<Boolean> future = (CollectionFuture<Boolean>) mc
                .asyncMopInsert(key, 0, 0, new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future.getOperationStatus();
        assertNotNull(status);
        assertEquals("CREATED_STORED", status.getMessage());
    }

    public void testStored() throws Exception {
        CollectionFuture<Boolean> future = (CollectionFuture<Boolean>) mc
                .asyncMopInsert(key, 0, 0, new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        future = (CollectionFuture<Boolean>) mc.asyncMopInsert(key, 1, 1,
                new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future.getOperationStatus();
        assertNotNull(status);
        assertEquals("STORED", status.getMessage());
    }

    public void testOverflowed() throws Exception {
        CollectionFuture<Boolean> future = (CollectionFuture<Boolean>) mc
                .asyncMopInsert(key, 0, 0, new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        assertTrue(mc.asyncSetAttr(key,
                new CollectionAttributes(null, 2L, CollectionOverflowAction.error))
                .get(1000, TimeUnit.MILLISECONDS));

        future = (CollectionFuture<Boolean>) mc.asyncMopInsert(key, 1, 1,
                new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        future = (CollectionFuture<Boolean>) mc.asyncMopInsert(key, 2, 1,
                new CollectionAttributes());
        assertFalse(future.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future.getOperationStatus();
        assertNotNull(status);
        assertEquals("OVERFLOWED", status.getMessage());
    }

    public void testDeletedDropped() throws Exception {
        // create
        CollectionFuture<Boolean> future = (CollectionFuture<Boolean>) mc
                .asyncMopInsert(key, 0, "aaa", new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        // delete
        future = (CollectionFuture<Boolean>) mc.asyncMopDelete(key, 0, true);
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future.getOperationStatus();
        assertNotNull(status);
        assertEquals("DELETED_DROPPED", status.getMessage());
    }

    public void testDeleted() throws Exception {
        // create
        CollectionFuture<Boolean> future = (CollectionFuture<Boolean>) mc
                .asyncMopInsert(key, 0, "aaa", new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        // insert
        future = (CollectionFuture<Boolean>) mc.asyncMopInsert(key, 1, "bbb",
                new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        // delete
        List<Object> fieldList = new ArrayList<Object>();
        fieldList.add(0);
        future = (CollectionFuture<Boolean>) mc.asyncMopDelete(key, fieldList, false);
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future.getOperationStatus();
        assertNotNull(status);
        assertEquals("DELETED", status.getMessage());
    }

    public void testDeletedDroppedAfterRetrieval() throws Exception {
        // create
        CollectionFuture<Boolean> future = (CollectionFuture<Boolean>) mc
                .asyncMopInsert(key, 0, "aaa", new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        // get
        CollectionFuture<Map<String, Object>> future2 = (CollectionFuture<Map<String, Object>>) mc
                .asyncMopGet(key, 0, true, true);
        assertNotNull(future2.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future2.getOperationStatus();
        assertNotNull(status);
        assertEquals("DELETED_DROPPED", status.getMessage());
    }

    public void testDeletedAfterRetrieval() throws Exception {
        // create
        CollectionFuture<Boolean> future = (CollectionFuture<Boolean>) mc
                .asyncMopInsert(key, 0, "aaa", new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        // insert
        future = (CollectionFuture<Boolean>) mc.asyncMopInsert(key, 1, "bbb",
                new CollectionAttributes());
        assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

        // get
        CollectionFuture<Map<String, Object>> future2 = (CollectionFuture<Map<String, Object>>) mc
                .asyncMopGet(key, 0, true, false);
        assertNotNull(future2.get(1000, TimeUnit.MILLISECONDS));

        OperationStatus status = future2.getOperationStatus();
        assertNotNull(status);
        assertEquals("DELETED", status.getMessage());
    }
}
