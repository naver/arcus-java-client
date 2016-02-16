package net.spy.memcached.collection.map;

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

public class MopInsertWhenKeyNotExist extends BaseIntegrationTest {

    private String key = "MopInsertWhenKeyNotExist";

    private String[] items9 = { "value0", "value1", "value2", "value3",
            "value4", "value5", "value6", "value7", "value8", };

    protected void tearDown() {
        try {
            deleteMap(key);
            super.tearDown();
        } catch (Exception e) {
        }
    }

    /**
     * <pre>
     * CREATE	FIXED	VALUE
     * true	false	null
     * </pre>
     */
    public void testMopInsert_nokey_01() throws Exception {
        insertToFail(key, true, null);
    }

    /**
     * <pre>
     * CREATE	FIXED	VALUE
     * false	true	not null
     * </pre>
     */
    public void testMopInsert_nokey_02() throws Exception {
        assertFalse(insertToSucceed(key, false, items9[0]));
    }

    /**
     * <pre>
     * CREATE	FIXED	VALUE
     * false	false	not null
     * </pre>
     */
    public void testMopInsert_nokey_04() throws Exception {
        assertFalse(insertToSucceed(key, false, items9[0]));
    }

    /**
     * <pre>
     * CREATE	FIXED	VALUE
     * true	true	not null
     * </pre>
     */
    public void testMopInsert_nokey_05() throws Exception {
        assertTrue(insertToSucceed(key, true, items9[0]));
    }

    boolean insertToFail(String key, boolean createKeyIfNotExists, Object value) {
        boolean result = false;
        try {
            result = mc
                    .asyncMopInsert(
                            key,
                            0,
                            value,
                            ((createKeyIfNotExists) ? new CollectionAttributes()
                                    : null)).get(1000, TimeUnit.MILLISECONDS);
            fail("should be failed");
        } catch (Exception e) {
        }
        return result;
    }

    boolean insertToSucceed(String key, boolean createKeyIfNotExists,
                            Object value) {
        boolean result = false;
        try {
            result = mc
                    .asyncMopInsert(
                            key,
                            0,
                            value,
                            ((createKeyIfNotExists) ? new CollectionAttributes()
                                    : null)).get(1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            fail("should not be failed");
        }
        return result;
    }

}