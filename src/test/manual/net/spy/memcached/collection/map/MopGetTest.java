package net.spy.memcached.collection.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

public class MopGetTest extends BaseIntegrationTest {

    private String key = "MopGetTest";

    private Long[] items9 = { 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L };

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

    public void testMopGet_NoKey() throws Exception {
        Map<String, Object> rmap = mc.asyncMopGet("no_key", 0, false, false).get(
                1000, TimeUnit.MILLISECONDS);

        // We've got an empty list
        assertNull(rmap);
    }

    public void testMopGet_NoField() throws Exception {
        List<Object> fieldList = new ArrayList<Object>();
        fieldList.add(20);
        Map<String, Object> map = mc.asyncMopGet(key, fieldList, false, false).get(1000,
                TimeUnit.MILLISECONDS);
        assertNotNull(map);
        assertTrue(map.isEmpty());
    }

    public void testLopGet_GetByBestEffort() throws Exception {
        // Retrieve items(2..11) in the list
        List<Object> fieldList = new ArrayList<Object>();
        for (int i = 2; i < 12; i++) {
            fieldList.add(i);
        }
        Map<String, Object> rmap = mc.asyncMopGet(key, fieldList, false, false).get(1000,
                TimeUnit.MILLISECONDS);

        // By rule of 'best effort',
        // items(2..9) should be retrieved
        assertEquals(7, rmap.size());
        for (int i = 0; i < rmap.size(); i++) {
            assertEquals(items9[i + 2], rmap.get(String.valueOf(i + 2)));
        }
    }

    public void testLopGet_GetWithDeletion() throws Exception {
        CollectionAttributes attrs = null;
        Map<String, Object> rmap = null;
        List<Object> fieldList = new ArrayList<Object>();

        // Retrieve items(0..5) in the list with delete option
        for (int i = 0; i < 6; i++) {
            fieldList.add(i);
        }
        rmap = mc.asyncMopGet(key, fieldList, true, false).get(1000,
                TimeUnit.MILLISECONDS);

        assertEquals(6, rmap.size());
        fieldList.clear();

        // Check the remaining item count in the list
        attrs = mc.asyncGetAttr(key).get(1000, TimeUnit.MILLISECONDS);
        assertEquals(3, attrs.getCount().intValue());

        // Retrieve items(6..8) in the list with delete option
        for (int i = 6; i < 9; i++) {
            fieldList.add(i);
        }
        rmap = mc.asyncMopGet(key, fieldList, true, true).get(1000,
                TimeUnit.MILLISECONDS);

        assertEquals(3, rmap.size());

        // Now our list has no items and would be deleted
        rmap = mc.asyncMopGet(key, 0, true, false).get(1000,
                TimeUnit.MILLISECONDS);
        assertNull(rmap);
    }
}