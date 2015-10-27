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
package net.spy.memcached.collection.btree;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.Element;
import net.spy.memcached.internal.CollectionFuture;

public class BopGetByPositionTest extends BaseIntegrationTest {

	private String key = "BopGetByPositionTest";
	private String invalidKey = "InvalidBopGetByPositionTest";
	private String kvKey = "KvBopGetByPositionTest";

	/* bkey values larger than maximum value of 4 bytes integer */
	private long[] longBkeys = { 9000000000L,
			9000000001L, 9000000002L, 9000000003L,
			9000000004L, 9000000005L, 9000000006L,
			9000000007L, 9000000008L, 9000000009L };
	private byte[][] byteArrayBkeys = { new byte[] { 10 }, new byte[] { 11 },
			new byte[] { 12 }, new byte[] { 13 }, new byte[] { 14 },
			new byte[] { 15 }, new byte[] { 16 }, new byte[] { 17 },
			new byte[] { 18 }, new byte[] { 19 } };

    private byte[] eflag = new byte[] {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
            31 };

	protected void setUp() throws Exception {
		super.setUp();
		mc.delete(key).get(1000, TimeUnit.MILLISECONDS);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testLongBKeySingle() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (long each : longBkeys) {
			mc.asyncBopInsert(key, each, eflag, "val", attrs).get();
		}

		// bop gbp
		int pos = 5;
		CollectionFuture<Map<Integer, Element<Object>>> f = mc
				.asyncBopGetByPosition(key, BTreeOrder.ASC, pos);
		Map<Integer, Element<Object>> result = f.get(1000,
				TimeUnit.MILLISECONDS);

		assertEquals(1, result.size());
		assertEquals(CollectionResponse.END, f.getOperationStatus()
				.getResponse());

		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			// System.out.println(String.format("index:%d, bkey:%d, value:%s",
			// each.getKey(), each.getValue().getLongBkey(), each
			// .getValue().getValue()));
			assertEquals("invalid index", pos, each.getKey().intValue());
			assertEquals("invalid bkey", longBkeys[pos], each.getValue()
					.getLongBkey());
			assertEquals("invalid value", "val", each.getValue().getValue());
		}
	}

    public void testLongBKeySingleWithoutEflag() throws Exception {
        // insert
        CollectionAttributes attrs = new CollectionAttributes();
        for (long each : longBkeys) {
            mc.asyncBopInsert(key, each, null, "val", attrs).get();
        }

        // bop gbp
        int pos = 5;
        CollectionFuture<Map<Integer, Element<Object>>> f = mc
                .asyncBopGetByPosition(key, BTreeOrder.ASC, pos);
        Map<Integer, Element<Object>> result = f.get(1000,
                TimeUnit.MILLISECONDS);

        assertEquals(1, result.size());
        assertEquals(CollectionResponse.END, f.getOperationStatus()
                .getResponse());

        for (Entry<Integer, Element<Object>> each : result.entrySet()) {
            // System.out.println(String.format("index:%d, bkey:%d, value:%s",
            // each.getKey(), each.getValue().getLongBkey(), each
            // .getValue().getValue()));
            assertEquals("invalid index", pos, each.getKey().intValue());
            assertEquals("invalid bkey", longBkeys[pos], each.getValue()
                    .getLongBkey());
            assertEquals("invalid value", "val", each.getValue().getValue());
        }
    }

	public void testLongBKeyMultiple() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (long each : longBkeys) {
			mc.asyncBopInsert(key, each, eflag, "val", attrs).get();
		}

		// bop gbp
		int posFrom = 5;
		int posTo = 8;
		CollectionFuture<Map<Integer, Element<Object>>> f = mc
				.asyncBopGetByPosition(key, BTreeOrder.ASC, posFrom, posTo);
		Map<Integer, Element<Object>> result = f.get(1000,
				TimeUnit.MILLISECONDS);

		assertEquals(4, result.size());
		assertEquals(CollectionResponse.END, f.getOperationStatus()
				.getResponse());

		int count = 0;
		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			// System.out.println(String.format("index:%d, bkey:%d, value:%s",
			// each.getKey(), each.getValue().getLongBkey(), each
			// .getValue().getValue()));
			int currPos = posFrom + count++;
			assertEquals("invalid index", currPos, each.getKey().intValue());
			assertEquals("invalid bkey", longBkeys[currPos], each.getValue()
					.getLongBkey());
			assertEquals("invalid value", "val", each.getValue().getValue());
		}
	}

	public void testLongBKeyMultipleReversed() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (long each : longBkeys) {
			mc.asyncBopInsert(key, each, eflag, "val", attrs).get();
		}

		// bop gbp
		int posFrom = 8;
		int posTo = 5;
		CollectionFuture<Map<Integer, Element<Object>>> f = mc
				.asyncBopGetByPosition(key, BTreeOrder.ASC, posFrom, posTo);
		Map<Integer, Element<Object>> result = f.get(1000,
				TimeUnit.MILLISECONDS);

		assertEquals(4, result.size());
		assertEquals(CollectionResponse.END, f.getOperationStatus()
				.getResponse());

		int count = 0;
		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			// System.out.println(String.format("index:%d, bkey:%d, value:%s",
			// each.getKey(), each.getValue().getLongBkey(), each
			// .getValue().getValue()));
			int currPos = posFrom - count++;
			assertEquals("invalid index", currPos, each.getKey().intValue());
			assertEquals("invalid bkey", longBkeys[currPos], each.getValue()
					.getLongBkey());
			assertEquals("invalid value", "val", each.getValue().getValue());
		}
	}

	public void testByteArrayBKeySingle() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (byte[] each : byteArrayBkeys) {
			mc.asyncBopInsert(key, each, eflag, "val", attrs).get();
		}

		// bop gbp
		int pos = 5;
		CollectionFuture<Map<Integer, Element<Object>>> f = mc
				.asyncBopGetByPosition(key, BTreeOrder.ASC, pos);
		Map<Integer, Element<Object>> result = f.get(1000,
				TimeUnit.MILLISECONDS);

		assertEquals(1, result.size());
		assertEquals(CollectionResponse.END, f.getOperationStatus()
				.getResponse());

		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			// System.out.println(String.format("index:%d, bkey:%s, value:%s",
			// each.getKey(), each.getValue().getBkeyByHex(), each
			// .getValue().getValue()));
			assertEquals("invalid index", pos, each.getKey().intValue());
			assertTrue("invalid bkey", Arrays.equals(byteArrayBkeys[pos], each
					.getValue().getByteArrayBkey()));
			assertEquals("invalid value", "val", each.getValue().getValue());
		}
	}

    public void testByteArrayBKeySingleWithoutEflag() throws Exception {
        // insert
        CollectionAttributes attrs = new CollectionAttributes();
        for (byte[] each : byteArrayBkeys) {
            mc.asyncBopInsert(key, each, null, "val", attrs).get();
        }

        // bop gbp
        int pos = 5;
        CollectionFuture<Map<Integer, Element<Object>>> f = mc
                .asyncBopGetByPosition(key, BTreeOrder.ASC, pos);
        Map<Integer, Element<Object>> result = f.get(1000,
                TimeUnit.MILLISECONDS);

        assertEquals(1, result.size());
        assertEquals(CollectionResponse.END, f.getOperationStatus()
                .getResponse());

        for (Entry<Integer, Element<Object>> each : result.entrySet()) {
            // System.out.println(String.format("index:%d, bkey:%s, value:%s",
            // each.getKey(), each.getValue().getBkeyByHex(), each
            // .getValue().getValue()));
            assertEquals("invalid index", pos, each.getKey().intValue());
            assertTrue("invalid bkey", Arrays.equals(byteArrayBkeys[pos], each
                    .getValue().getByteArrayBkey()));
            assertEquals("invalid value", "val", each.getValue().getValue());
        }
    }

	public void testByteArrayBKeyMultiple() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (byte[] each : byteArrayBkeys) {
			mc.asyncBopInsert(key, each, eflag, "val", attrs).get();
		}

		// bop gbp
		int posFrom = 5;
		int posTo = 8;
		CollectionFuture<Map<Integer, Element<Object>>> f = mc
				.asyncBopGetByPosition(key, BTreeOrder.ASC, posFrom, posTo);
		Map<Integer, Element<Object>> result = f.get(1000,
				TimeUnit.MILLISECONDS);

		assertEquals(4, result.size());
		assertEquals(CollectionResponse.END, f.getOperationStatus()
				.getResponse());

		int count = 0;
		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			// System.out.println(String.format("index:%d, bkey:%s, value:%s",
			// each.getKey(), each.getValue().getBkeyByHex(), each
			// .getValue().getValue()));
			int currPos = posFrom + count++;
			assertEquals("invalid index", currPos, each.getKey().intValue());
			assertTrue("invalid bkey", Arrays.equals(byteArrayBkeys[currPos],
					each.getValue().getByteArrayBkey()));
			assertEquals("invalid value", "val", each.getValue().getValue());
		}
	}

	public void testByteArrayBKeyMultipleReversed() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (byte[] each : byteArrayBkeys) {
			mc.asyncBopInsert(key, each, eflag, "val", attrs).get();
		}

		// bop gbp
		int posFrom = 8;
		int posTo = 5;
		CollectionFuture<Map<Integer, Element<Object>>> f = mc
				.asyncBopGetByPosition(key, BTreeOrder.ASC, posFrom, posTo);
		Map<Integer, Element<Object>> result = f.get(1000,
				TimeUnit.MILLISECONDS);

		assertEquals(4, result.size());
		assertEquals(CollectionResponse.END, f.getOperationStatus()
				.getResponse());

		int count = 0;
		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			// System.out.println(String.format("index:%d, bkey:%s, value:%s",
			// each.getKey(), each.getValue().getBkeyByHex(), each
			// .getValue().getValue()));
			int currPos = posFrom - count++;
			assertEquals("invalid index", currPos, each.getKey().intValue());
			assertTrue("invalid bkey", Arrays.equals(byteArrayBkeys[currPos],
					each.getValue().getByteArrayBkey()));
			assertEquals("invalid value", "val", each.getValue().getValue());
		}
	}

	public void testUnsuccessfulResponses() throws Exception {
		mc.delete(invalidKey).get();
		mc.delete(kvKey).get();

		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		attrs.setReadable(false);
		for (byte[] each : byteArrayBkeys) {
			mc.asyncBopInsert(key, each, null, "val", attrs).get();
		}

		// set a test key
		mc.set(kvKey, 0, "value").get();

		CollectionFuture<Map<Integer, Element<Object>>> f = null;
		Map<Integer, Element<Object>> result = null;

		// NOT_FOUND
		f = mc.asyncBopGetByPosition(invalidKey, BTreeOrder.ASC, 0);
		result = f.get();
		assertNull(result);
		assertEquals(CollectionResponse.NOT_FOUND, f.getOperationStatus()
				.getResponse());

		// UNREADABLE
		f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0);
		result = f.get();
		assertNull(result);
		assertEquals(CollectionResponse.UNREADABLE, f.getOperationStatus()
				.getResponse());

		attrs.setReadable(true);
		mc.asyncSetAttr(key, attrs).get();

		// TYPE_MISMATCH
		f = mc.asyncBopGetByPosition(kvKey, BTreeOrder.ASC, 0);
		result = f.get();
		assertNull(result);
		assertEquals(CollectionResponse.TYPE_MISMATCH, f.getOperationStatus()
				.getResponse());

		// NOT_FOUND_ELEMENT
		f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 2000);
		result = f.get();
		assertNotNull(result);
		assertEquals(0, result.size());
		assertEquals(CollectionResponse.NOT_FOUND_ELEMENT, f
				.getOperationStatus().getResponse());

		f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 1000, 2000);
		result = f.get();
		assertNotNull(result);
		assertEquals(0, result.size());
		assertEquals(CollectionResponse.NOT_FOUND_ELEMENT, f
				.getOperationStatus().getResponse());
	}

	public void testAscDesc() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (long each : longBkeys) {
			mc.asyncBopInsert(key, each, null, "val", attrs).get();
		}

		CollectionFuture<Map<Integer, Element<Object>>> f = null;
		Map<Integer, Element<Object>> result = null;

		// 1. ASC 5 20 5 6 7 8 9
		// 2. ASC 20 5 9 8 7 6 5
		// 3. DESC 5 20 5 6 7 8 9
		// 4. DESC 20 5 9 8 7 6 5
		int prevPos = 0;

		// case 1.
		prevPos = Integer.MIN_VALUE;
		f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 5, 20);
		result = f.get();
		assertNotNull(result);
		assertEquals(5, result.size());
		System.out.println(result.keySet());

		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			int currPos = each.getKey();
			assertTrue("positions are not in ascending order",
					currPos > prevPos);
			prevPos = currPos;

			Element<Object> e = each.getValue();
			// assertEquals(longBkeys[currPos], e.getLongBkey());
			System.out.println(currPos + " : " + e.getLongBkey());
		}

		// case 2.
		f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 20, 5);
		result = f.get();
		assertNotNull(result);
		assertEquals(5, result.size());
		System.out.println(result.keySet());

		prevPos = Integer.MAX_VALUE;
		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			int currPos = each.getKey();
			assertTrue("positions are not in descending order",
					currPos < prevPos);
			prevPos = currPos;

			Element<Object> e = each.getValue();
			assertEquals(longBkeys[currPos], e.getLongBkey());
		}

		// case 3.
		f = mc.asyncBopGetByPosition(key, BTreeOrder.DESC, 5, 20);
		result = f.get();
		assertNotNull(result);
		assertEquals(5, result.size());

		prevPos = Integer.MAX_VALUE;
		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			int currPos = longBkeys.length - each.getKey() - 1;
			assertTrue("positions are not in ascending order (reversed)",
					currPos < prevPos);
			prevPos = currPos;

			Element<Object> e = each.getValue();
			assertEquals(longBkeys[currPos], e.getLongBkey());
		}

		// case 4.
		f = mc.asyncBopGetByPosition(key, BTreeOrder.DESC, 20, 5);
		result = f.get();
		assertNotNull(result);
		assertEquals(5, result.size());
		System.out.println(result.keySet());

		prevPos = Integer.MIN_VALUE;
		for (Entry<Integer, Element<Object>> each : result.entrySet()) {
			int currPos = longBkeys.length - each.getKey() - 1;
			assertTrue("positions are not in descending order (reversed)",
					currPos > prevPos);
			prevPos = currPos;

			Element<Object> e = each.getValue();
			assertEquals(longBkeys[currPos], e.getLongBkey());
		}
	}

	public void testInvalidArgumentException() throws Exception {
		// insert
		CollectionAttributes attrs = new CollectionAttributes();
		for (long each : longBkeys) {
			mc.asyncBopInsert(key, each, null, "val", attrs).get();
		}

		CollectionFuture<Map<Integer, Element<Object>>> f = null;

		// BTreeOrder == null
		try {
			f = mc.asyncBopGetByPosition(key, null, 5, 20);
			f.get();
			fail("This should be an exception");
		} catch (IllegalArgumentException e) {
			assertEquals("BTreeOrder must not be null.", e.getMessage());
		}

		// Position < 0
		try {
			f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, -1);
			fail("This should be an exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Position must be 0 or positive integer.",
					e.getMessage());
		}

		try {
			f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, -1, 20);
			fail("This should be an exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Position must be 0 or positive integer.",
					e.getMessage());
		}

		try {
			f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0, -1);
			fail("This should be an exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Position must be 0 or positive integer.",
					e.getMessage());
		}

		try {
			f = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, -1, -1);
			fail("This should be an exception");
		} catch (IllegalArgumentException e) {
			assertEquals("Position must be 0 or positive integer.",
					e.getMessage());
		}
	}

}
