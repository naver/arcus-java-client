package net.spy.memcached.collection.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.transcoders.LongTranscoder;

public class MopDeleteTest extends BaseIntegrationTest {

    private String key = "MopDeleteTest";

    private Long[] items9 = { 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L };

    protected void setUp() throws Exception {
        super.setUp();

        deleteMap(key);
        addToMap(key, items9);

        CollectionAttributes attrs = new CollectionAttributes();
        attrs.setMaxCount(10);
        assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));
    }

    protected void tearDown() throws Exception {
        try {
            deleteMap(key);
            super.tearDown();
        } catch (Exception e) {
        }
    }

    public void testMopDelete_NoKey() throws Exception {
        assertFalse(mc.asyncMopDelete("no_key", 0, false).get(1000, TimeUnit.MILLISECONDS));
    }

    public void testMopDelete_OutOfRange() throws Exception {
        List<Object> fieldList = new ArrayList<Object>();
        fieldList.add(11);
        assertFalse(mc.asyncMopDelete(key, fieldList, false).get(1000, TimeUnit.MILLISECONDS));
    }

    public void testMopDelete_DeleteByBestEffort() throws Exception {
        // Delete items(2..11) in the map
        List<Object> fieldList = new ArrayList<Object>();
        for (int i=2; i<12; i++) {
            fieldList.add(i);
        }

        assertTrue(mc.asyncMopDelete(key, fieldList, false).get(1000,
                TimeUnit.MILLISECONDS));


        // Check that item is inserted
        Map<String, Long> rmap = mc.asyncMopGet(key, 0, false, false,
                new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

        // By rule of 'best effort',
        // items(2..9) should be deleted
        assertEquals(2, rmap.size());
        assertEquals((Long) 0L, rmap.get("0"));
        assertEquals((Long) 1L, rmap.get("1"));
    }

    public void testMopDelete_DeletedDropped() throws Exception {
        // Delete all items in the list
        assertTrue(mc.asyncMopDelete(key, 0, true).get(1000, TimeUnit.MILLISECONDS));

        CollectionAttributes attrs = mc.asyncGetAttr(key).get(1000,
                TimeUnit.MILLISECONDS);
        assertNull(attrs);
    }
}
