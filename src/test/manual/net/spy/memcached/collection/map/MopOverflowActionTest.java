package net.spy.memcached.collection.map;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;

public class MopOverflowActionTest extends BaseIntegrationTest {

	private String key = "MopOverflowActionTest";

	protected void setUp() {
		try {
			super.setUp();
			mc.delete(key).get();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void testMopGet_Maxcount() throws Exception {
		// Test
		for (int maxcount = 100; maxcount <= 200; maxcount += 100) {
			// Create a map
			mc.asyncMopInsert(key, 0, "item0", new CollectionAttributes());

			// Set maxcount
			CollectionAttributes attrs = new CollectionAttributes();
			attrs.setMaxCount(maxcount);
			assertTrue(mc.asyncSetAttr(key, attrs).get(1000,
					TimeUnit.MILLISECONDS));

			for (int i = 1; i < maxcount; i++) {
				assertTrue(mc.asyncMopInsert(key, i, "item" + i, null).get(
						1000, TimeUnit.MILLISECONDS));
			}

			Map<String, Object> result = mc.asyncMopGet(key, 0, false,
					false).get(10000, TimeUnit.MILLISECONDS);
			assertEquals(maxcount, result.size());
			assertTrue(mc.asyncMopDelete(key, 0, true).get(1000,
					TimeUnit.MILLISECONDS));
		}
	}

	public void testMopGet_Overflow() throws Exception {
		// Create a map
		mc.asyncMopInsert(key, 0, "item0", new CollectionAttributes());

		int maxcount = 100;

		// Set maxcount to 100
		CollectionAttributes attrs = new CollectionAttributes();
		attrs.setMaxCount(maxcount);
		attrs.setOverflowAction(CollectionOverflowAction.error);
		assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

		// Insert more than maxcount
		for (int i = 1; i <= maxcount + 10; i++) {
			mc.asyncMopInsert(key, i, "item" + i, null).get(1000,
					TimeUnit.MILLISECONDS);

		}

		Map<String, Object> result = mc.asyncMopGet(key, 0, false,
				false).get(10000, TimeUnit.MILLISECONDS);

		// result size should be maxsize(10000)
		assertEquals(maxcount, result.size());
		assertEquals("item0", result.get("0"));
		assertEquals("item99", result.get(String.valueOf(result.size() - 1)));
		assertTrue(mc.asyncMopDelete(key, 0, true).get(1000,
				TimeUnit.MILLISECONDS));
	}

	public void testLopGet_AvailableOverflowAction() throws Exception {
		// Create a set
		mc.asyncMopInsert(key, 0, "item0", new CollectionAttributes());

		// Set OverflowAction
		// error
		assertTrue(mc.asyncSetAttr(key,
				new CollectionAttributes(null, null, CollectionOverflowAction.error))
				.get(1000, TimeUnit.MILLISECONDS));

		// head_trim
		assertFalse(mc.asyncSetAttr(key,
				new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
				TimeUnit.MILLISECONDS));

		// tail_trim
		assertFalse(mc.asyncSetAttr(key,
				new CollectionAttributes(null, null, CollectionOverflowAction.tail_trim)).get(1000,
				TimeUnit.MILLISECONDS));

		// smallest_trim
		assertFalse(mc.asyncSetAttr(key,
				new CollectionAttributes(null, null, CollectionOverflowAction.smallest_trim)).get(1000,
				TimeUnit.MILLISECONDS));

		// largest_trim
		assertFalse(mc.asyncSetAttr(key,
				new CollectionAttributes(null, null, CollectionOverflowAction.largest_trim)).get(1000,
				TimeUnit.MILLISECONDS));

		mc.asyncMopDelete(key, 0, true).get(1000, TimeUnit.MILLISECONDS);
	}

}
