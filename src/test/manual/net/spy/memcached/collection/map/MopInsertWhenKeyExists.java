package net.spy.memcached.collection.map;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.transcoders.LongTranscoder;

public class MopInsertWhenKeyExists extends BaseIntegrationTest {

    private String key = "MopInsertWhenKeyExists";

    private Long[] items9 = { 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L };

    protected void tearDown() {
        try {
            mc.asyncMopDelete(key, 0, true).get(1000, TimeUnit.MILLISECONDS);
            mc.delete(key).get();
            super.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testMopInsert_Normal() throws Exception {
        // Create a list and add it 9 items
        addToMap(key, items9);

        // Set maxcount to 10
        CollectionAttributes attrs = new CollectionAttributes();
        attrs.setMaxCount(10);
        assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

        // Insert one item
        assertTrue(mc.asyncMopInsert(key, 10, 10L,
                new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS));

        // Check inserted item
        Map<String, Long> rmap = mc.asyncMopGet(key, 0, false, false,
                new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
        assertEquals(10, rmap.size());

        Long comp = rmap.get("10");
        assertEquals((Long) 10L, comp);

        // Check list attributes
        CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
                TimeUnit.MILLISECONDS);
        assertEquals(10, rattrs.getCount().intValue());
    }

    public void testMopInsert_SameItem() throws Exception {
        // Create a list and add it 9 items
        addToMap(key, items9);

        // Set maxcount to 10
        CollectionAttributes attrs = new CollectionAttributes();
        attrs.setMaxCount(10);
        assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

        // Insert an item same to the last item
        mc.asyncMopInsert(key, 10, 9L, new CollectionAttributes()).get(
                1000, TimeUnit.MILLISECONDS);

        // Check that item is inserted
        Map<String, Long> rmap = mc.asyncMopGet(key, 0, false, false,
                new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
        assertEquals(10, rmap.size());
    }

    public void testMopInsert_SameField() throws Exception {
        // Create a list and add it 9 items
        addToMap(key, items9);

        // Set maxcount to 10
        CollectionAttributes attrs = new CollectionAttributes();
        attrs.setMaxCount(10);
        assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

        // Insert an item same to the last item
        mc.asyncMopInsert(key, 8, 10L, new CollectionAttributes()).get(
                1000, TimeUnit.MILLISECONDS);

        // Check that item is inserted
        Map<String, Long> rmap = mc.asyncMopGet(key, 0, false, false,
                new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

        assertEquals(9, rmap.size());
    }
}
