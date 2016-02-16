package net.spy.memcached.collection.map;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

public class MopUpdateTest extends BaseIntegrationTest {

    private final String KEY = this.getClass().getSimpleName();

    private final String field = "updateTestField";

    private final String VALUE = "VALUE";
    private final String NEW_VALUE = "NEWVALUE";

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

    public void testUpdateNotExistsKey() {
        try {
            // update value
            Assert.assertFalse(mc.asyncMopUpdate(KEY, field, VALUE).get());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    public void testExistsKey() {
        try {
            // insert one
            Assert.assertTrue(mc.asyncMopInsert(KEY, field, VALUE,
                    new CollectionAttributes()).get());

            // update value only
            Map<String, Object> rmap = mc.asyncMopGet(KEY, 0, false, false)
                                        .get(1000, TimeUnit.MILLISECONDS);

            Assert.assertEquals(VALUE, rmap.get(field));

            Assert.assertTrue(mc.asyncMopUpdate(KEY, field, NEW_VALUE).get());

            Map<String, Object> urmap = mc.asyncMopGet(KEY, 0, false, false)
                                        .get(1000, TimeUnit.MILLISECONDS);
            Assert.assertEquals(NEW_VALUE, urmap.get(field));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
