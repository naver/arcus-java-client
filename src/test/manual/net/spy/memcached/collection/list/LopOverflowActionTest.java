/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.collection.list;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.ElementValueType;

public class LopOverflowActionTest extends BaseIntegrationTest {

	private String key = "LopOverflowActionTest";
	private List<String> keyList = new ArrayList<String>();

	protected void setUp() {
		keyList.add(key);
		try {
			super.setUp();
			mc.delete(key).get();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void testLopGet_Maxcount() throws Exception {
		// Test
		for (int maxcount = 100; maxcount <= 200; maxcount += 100) {
			// Create a list
			mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes());

			// Set maxcount
			CollectionAttributes attrs = new CollectionAttributes();
			attrs.setMaxCount(maxcount);
			assertTrue(mc.asyncSetAttr(key, attrs).get(1000,
					TimeUnit.MILLISECONDS));

			for (int i = 1; i < maxcount; i++) {
				assertTrue(mc.asyncLopInsert(key, i, "item" + i, null).get(
						1000, TimeUnit.MILLISECONDS));
			}

			List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
					false).get(10000, TimeUnit.MILLISECONDS);
			assertEquals(maxcount, result.size());
			assertTrue(mc.asyncLopDelete(key, 0, 20000, true).get(1000,
					TimeUnit.MILLISECONDS));
		}
	}

	public void testLopGet_Overflow() throws Exception {
		// Create a List
		mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes());

		int maxcount = 100;

		// Set maxcount to 100
		CollectionAttributes attrs = new CollectionAttributes();
		attrs.setMaxCount(maxcount);
		attrs.setOverflowAction(CollectionOverflowAction.error);
		assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

		// Insert more than maxcount
		for (int i = 1; i <= maxcount + 10; i++) {
			mc.asyncLopInsert(key, -1, "item" + i, null).get(1000,
					TimeUnit.MILLISECONDS);

		}

		List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
				false).get(10000, TimeUnit.MILLISECONDS);

		// result size should be maxsize(10000)
		assertEquals(maxcount, result.size());
		assertEquals("item0", result.get(0));
		assertEquals("item99", result.get(result.size() - 1));
		assertTrue(mc.asyncLopDelete(key, 0, 20000, true).get(1000,
				TimeUnit.MILLISECONDS));
	}

	public void testLopGet_HeadTrim() throws Exception {
		// Create a List
		mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes());

		int maxcount = 100;

		// Set maxcount to 10000
		CollectionAttributes attrs = new CollectionAttributes();
		attrs.setMaxCount(maxcount);
		attrs.setOverflowAction(CollectionOverflowAction.head_trim);
		assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

		// Insert more than maxcount
		for (int i = 1; i <= maxcount + 10; i++) {
			assertTrue(mc.asyncLopInsert(key, -1, "item" + i, null).get(1000,
					TimeUnit.MILLISECONDS));
		}

		List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
				false).get(10000, TimeUnit.MILLISECONDS);

		// result size should be maxsize(10000)
		assertEquals(maxcount, result.size());
		assertEquals("item11", result.get(0));
		assertEquals("item110", result.get(result.size() - 1));
		assertTrue(mc.asyncLopDelete(key, 0, 20000, true).get(1000,
				TimeUnit.MILLISECONDS));
	}

	public void testLopGet_TailTrim() throws Exception {
		// Create a List
		mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes());

		int maxcount = 100;

		// Set maxcount to 10000
		CollectionAttributes attrs = new CollectionAttributes();
		attrs.setMaxCount(maxcount);
		attrs.setOverflowAction(CollectionOverflowAction.tail_trim);
		assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

		// Insert more than maxcount
		for (int i = 1; i <= maxcount + 10; i++) {
			assertTrue(mc.asyncLopInsert(key, 0, "item" + i, null).get(1000,
					TimeUnit.MILLISECONDS));
		}

		List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
				false).get(10000, TimeUnit.MILLISECONDS);

		// result size should be maxsize(10000)
		assertEquals(maxcount, result.size());
		assertEquals("item110", result.get(0));
		assertEquals("item11", result.get(result.size() - 1));
		assertTrue(mc.asyncLopDelete(key, 0, 20000, false).get(1000,
				TimeUnit.MILLISECONDS));
	}

	public void testLopGet_HeadTrim_OutOfRange() throws Exception {
		// Create a set
		mc.asyncLopInsert(key, 1, "item1", new CollectionAttributes());

		// head_trim
		assertFalse(mc.asyncSetAttr(key,
				new CollectionAttributes(null, 1L, CollectionOverflowAction.head_trim)).get(1000,
				TimeUnit.MILLISECONDS));

		// test
		assertFalse(mc.asyncLopInsert(key, 0, "item0", null).get(1000,
				TimeUnit.MILLISECONDS));

		mc.asyncLopDelete(key, 0, 10, false).get(1000, TimeUnit.MILLISECONDS);
	}

	public void testLopGet_TailTrim_OutOfRange() throws Exception {
		// Create a set
		mc.asyncLopInsert(key, 1, "item1", new CollectionAttributes());

		// tail_trim
		assertFalse(mc.asyncSetAttr(key,
				new CollectionAttributes(null, 1L, CollectionOverflowAction.tail_trim)).get(1000,
				TimeUnit.MILLISECONDS));

		// test
		assertFalse(mc.asyncLopInsert(key, 2, "item2", null).get(1000,
				TimeUnit.MILLISECONDS));

		mc.asyncLopDelete(key, 0, 10, false).get(1000, TimeUnit.MILLISECONDS);
	}

	public void testLopGet_AvailableOverflowAction() throws Exception {
		// Create a set
		mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes());

		// Set OverflowAction
		// error
		assertTrue(mc.asyncSetAttr(key,
				new CollectionAttributes(null, null, CollectionOverflowAction.error))
				.get(1000, TimeUnit.MILLISECONDS));

		// head_trim
		assertTrue(mc.asyncSetAttr(key,
				new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
				TimeUnit.MILLISECONDS));

		// tail_trim
		assertTrue(mc.asyncSetAttr(key,
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

		mc.asyncLopDelete(key, 0, true).get(1000, TimeUnit.MILLISECONDS);
	}

	public void testLopGet_notAvailableOverflowAction() {
		CollectionAttributes attributesForCreate = new CollectionAttributes();

		// create
		try {
			attributesForCreate.setOverflowAction(CollectionOverflowAction.smallest_trim);
			mc.asyncLopCreate(key, ElementValueType.STRING, attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// insert
		try {
			mc.asyncLopInsert(key, 0, "0", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// pipe insert
		try {
			mc.asyncLopPipedInsertBulk(key, 0, new ArrayList<Object>(), attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// bulk insert
		try {
			mc.asyncLopInsertBulk(keyList, 0, "0", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// create
		try {
			attributesForCreate.setOverflowAction(CollectionOverflowAction.smallest_silent_trim);
			mc.asyncLopCreate(key, ElementValueType.STRING, attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// insert
		try {
			mc.asyncLopInsert(key, 0, "1", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// pipe insert
		try {
			mc.asyncLopPipedInsertBulk(key, 0, new ArrayList<Object>(), attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// bulk insert
		try {
			mc.asyncLopInsertBulk(keyList, 0, "0", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// create
		try {
			attributesForCreate.setOverflowAction(CollectionOverflowAction.largest_trim);
			mc.asyncLopCreate(key, ElementValueType.STRING, attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// insert
		try {
			mc.asyncLopInsert(key, 0, "1", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// pipe insert
		try {
			mc.asyncLopPipedInsertBulk(key, 0, new ArrayList<Object>(), attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// bulk insert
		try {
			mc.asyncLopInsertBulk(keyList, 0, "0", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// create
		try {
			attributesForCreate.setOverflowAction(CollectionOverflowAction.largest_silent_trim);
			mc.asyncLopCreate(key, ElementValueType.STRING, attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// insert
		try {
			mc.asyncLopInsert(key, 0, "1", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// pipe insert
		try {
			mc.asyncLopPipedInsertBulk(key, 0, new ArrayList<Object>(), attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		// bulk insert
		try {
			mc.asyncLopInsertBulk(keyList, 0, "0", attributesForCreate).get();
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// test success
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
}
