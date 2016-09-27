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
package net.spy.memcached.bulkoperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

public class PipeInsertTest extends BaseIntegrationTest {

	private static final String KEY = PipeInsertTest.class.getSimpleName();

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

	public void testBopPipeInsert() {
		int elementCount = 5000;

		List<Element<Object>> elements = new ArrayList<Element<Object>>();

		for (int i = 0; i < elementCount; i++) {
			elements.add(new Element<Object>(i, "value" + i,
					new byte[] { (byte) 1 }));
		}

		try {
			CollectionAttributes attr = new CollectionAttributes();

			CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncBopPipedInsertBulk(KEY, elements, attr);

			Map<Integer, CollectionOperationStatus> map = future.get(5000L,
					TimeUnit.MILLISECONDS);

			Assert.assertTrue(map.isEmpty());

			Map<Long, Element<Object>> map3 = mc.asyncBopGet(KEY, 0, 9999,
					ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get();

			Assert.assertEquals(4000, map3.size());

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	public void testBopPipeInsert2() {
		int elementCount = 5000;
		Map<Long, Object> elements = new TreeMap<Long, Object>();
		for (long i = 0; i < elementCount; i++) {
			elements.put(i, "value" + i);
		}

		try {
			long start = System.currentTimeMillis();

			CollectionAttributes attr = new CollectionAttributes();

			CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncBopPipedInsertBulk(KEY, elements, attr);

			Map<Integer, CollectionOperationStatus> map = future.get(5000L,
					TimeUnit.MILLISECONDS);

			// System.out.println(System.currentTimeMillis() - start + "ms");

			Assert.assertTrue(map.isEmpty());

			Map<Long, Element<Object>> map3 = mc.asyncBopGet(KEY, 0, 9999,
					ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get();

			Assert.assertEquals(4000, map3.size());

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	public void testLopPipeInsert() {
		int elementCount = 5000;

		List<Object> elements = new ArrayList<Object>(elementCount);

		for (int i = 0; i < elementCount; i++) {
			elements.add("value" + i);
		}

		try {
			long start = System.currentTimeMillis();

			CollectionAttributes attr = new CollectionAttributes();

			CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncLopPipedInsertBulk(KEY, -1, elements, attr);

			Map<Integer, CollectionOperationStatus> map = future.get(5000L,
					TimeUnit.MILLISECONDS);

			// System.out.println(System.currentTimeMillis() - start + "ms");

			Assert.assertTrue(map.isEmpty());

			List<Object> list = mc.asyncLopGet(KEY, 0, 9999, false, false)
					.get();

			Assert.assertEquals(4000, list.size());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	public void testMopPipeInsert() {
		int elementCount = 5000;

		Map<String, Object> elements = new TreeMap<String, Object>();

		for (int i = 0; i < elementCount; i++) {
			elements.put(String.valueOf(i), "value" + i);
		}

		try {
			CollectionAttributes attr = new CollectionAttributes();

			CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncMopPipedInsertBulk(KEY, elements, attr);

			Map<Integer, CollectionOperationStatus> map = future.get(5000L,
					TimeUnit.MILLISECONDS);

			Assert.assertEquals(1000, map.size());

			Map<String, Object> rmap = mc.asyncMopGet(KEY, false, false)
					.get();

			Assert.assertEquals(4000, rmap.size());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

}
