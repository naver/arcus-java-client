/*
 * arcus-java-client : Arcus Java client
 * Copyright 2016 JaM2in Co., Ltd.
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

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

public class MopInsertBulkMultipleTest extends BaseIntegrationTest {

	public void testInsertAndGet() {
		String key = "MyMopKey32";
		String value = "MyValue";

		int mkeySize = 500;
		Map<String, Object> mkeys = new TreeMap<String, Object>();
		for (int i = 0; i < mkeySize; i++) {
			mkeys.put(String.valueOf(i), value);
		}

		try {
			// REMOVE
			mc.asyncMopDelete(key, true);

			// SET
			Future<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncMopPipedInsertBulk(key, mkeys,
							new CollectionAttributes());
			try {
				Map<Integer, CollectionOperationStatus> errorList = future.get(
						20000L, TimeUnit.MILLISECONDS);

				Assert.assertTrue("Error list is not empty.",
						errorList.isEmpty());
			} catch (TimeoutException e) {
				future.cancel(true);
				e.printStackTrace();
			}

			// GET
			int errorCount = 0;
			for (Entry<String, Object> entry : mkeys.entrySet()) {
				Future<Map<String, Object>> f = mc.asyncMopGet(key,
						entry.getKey(), false, false);
				Map<String, Object> map = null;
				try {
					map = f.get();
				} catch (Exception e) {
					f.cancel(true);
					e.printStackTrace();
				}
				Object value2 = map.entrySet().iterator().next().getValue();
				if (!value.equals(value2)) {
					errorCount++;
				}
			}
			Assert.assertEquals("Error count is greater than 0.", 0, errorCount);

			// REMOVE
			for (Entry<String, Object> entry : mkeys.entrySet()) {
				mc.asyncMopDelete(key, entry.getKey(), true).get();
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testTimeout() {
		String key = "MyMopKey";
		String value = "MyValue";

		int mkeySize = mc.getMaxPipedItemCount();
		Map<String, Object> mkeys = new TreeMap<String, Object>();
		for (int i = 0; i < mkeySize; i++) {
			mkeys.put(String.valueOf(i), value);
		}

		try {
			// SET
			Future<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncMopPipedInsertBulk(key, mkeys,
							new CollectionAttributes());
			try {
				Map<Integer, CollectionOperationStatus> errorList = future.get(
						1L, TimeUnit.NANOSECONDS);

				Assert.assertTrue("Error list is not empty." + errorList,
						errorList.isEmpty());
			} catch (TimeoutException e) {
				future.cancel(true);
				return;
			} catch (Exception e) {
				future.cancel(true);
				Assert.fail();
			}
			Assert.fail();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testInsertAndGetUsingSingleClient() {
		String key = "MyMopKey333";
		String value = "MyValue";

		int mkeySize = 500;
		Map<String, Object> mkeys = new TreeMap<String, Object>();
		for (int i = 0; i < mkeySize; i++) {
			mkeys.put(String.valueOf(i), value);
		}

		try {
			// REMOVE
			mc.asyncMopDelete(key, true);

			// SET
			Future<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncMopPipedInsertBulk(key, mkeys,
							new CollectionAttributes());
			try {
				Map<Integer, CollectionOperationStatus> errorList = future.get(
						20000L, TimeUnit.MILLISECONDS);

				Assert.assertTrue("Error list is not empty.",
						errorList.isEmpty());
			} catch (TimeoutException e) {
				future.cancel(true);
				e.printStackTrace();
			}

			// GET
			int errorCount = 0;
			for (Entry<String, Object> entry : mkeys.entrySet()) {
				Future<Map<String, Object>> f = mc.asyncMopGet(key,
						entry.getKey(), false, false);
				Map<String, Object> map = null;
				try {
					map = f.get();
				} catch (Exception e) {
					f.cancel(true);
					e.printStackTrace();
				}
				Object value2 = map.entrySet().iterator().next().getValue();
				if (!value.equals(value2)) {
					errorCount++;
				}
			}
			Assert.assertEquals("Error count is greater than 0.", 0, errorCount);

			// REMOVE
			for (Entry<String, Object> entry : mkeys.entrySet()) {
				mc.asyncMopDelete(key, entry.getKey(), true).get();
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testTimeoutUsingSingleClient() {
		String key = "MyMopKey";
		String value = "MyValue";

		int mkeySize = mc.getMaxPipedItemCount();
		Map<String, Object> mkeys = new TreeMap<String, Object>();
		for (int i = 0; i < mkeySize; i++) {
			mkeys.put(String.valueOf(i), value);
		}

		try {
			// SET
			Future<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncMopPipedInsertBulk(key, mkeys,
							new CollectionAttributes());
			try {
				Map<Integer, CollectionOperationStatus> errorList = future.get(
						1L, TimeUnit.NANOSECONDS);

				Assert.assertTrue("Error list is not empty." + errorList,
						errorList.isEmpty());
			} catch (TimeoutException e) {
				future.cancel(true);
				return;
			} catch (Exception e) {
				future.cancel(true);
				Assert.fail();
			}
			Assert.fail();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testErrorCount() {
		String key = "MyMopKeyErrorCount";
		String value = "MyValue";

		int mkeySize = 1200;
		Map<String, Object> mkeys = new TreeMap<String, Object>();
		for (int i = 0; i < mkeySize; i++) {
			mkeys.put(String.valueOf(i), value);
		}

		try {
			mc.delete(key).get();

			// SET
			Future<Map<Integer, CollectionOperationStatus>> future = mc
					.asyncMopPipedInsertBulk(key, mkeys, null);

			Map<Integer, CollectionOperationStatus> map = future.get(2000L,
					TimeUnit.MILLISECONDS);
			assertEquals(mkeySize, map.size());

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

}
