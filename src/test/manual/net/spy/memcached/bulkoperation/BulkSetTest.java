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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.CollectionOperationStatus;

public class BulkSetTest extends BaseIntegrationTest {

	public void testInsertAndGet2() {
		int TEST_COUNT = 3;

		try {
			for (int keySize = 0; keySize < TEST_COUNT; keySize++) {

				// generate key
				Map<String, Object> o = new HashMap<String, Object>();

				for (int i = 0; i < 600; i++) {
					o.put("MyKey" + i, "MyValue" + i);
				}

				List<String> keys = new ArrayList<String>(o.keySet());

				// REMOVE
				for (String key : keys) {
					mc.delete(key).get();
				}

				// SET
				Future<Map<String, CollectionOperationStatus>> future = mc
						.asyncSetBulk(o, 60);

				Map<String, CollectionOperationStatus> errorList;
				try {
					errorList = future.get(20000L, TimeUnit.MILLISECONDS);
					Assert.assertTrue("Error list is not empty.",
							errorList.isEmpty());
				} catch (TimeoutException e) {
					future.cancel(true);
					e.printStackTrace();
				}

				// GET
				int errorCount = 0;
				String k, v;
				for (int i = 0; i < keys.size(); i++) {
					k = keys.get(i);
					v = (String) mc.asyncGet(k).get();

					if (!v.equals(o.get(k))) {
						errorCount++;
					}

					mc.delete(k).get();
				}

				Assert.assertEquals("Error count is greater than 0.", 0,
						errorCount);

			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testInsertAndGet() {
		String value = "MyValue";

		int TEST_COUNT = 64;

		try {
			// SET null key
			try {
				mc.asyncSetBulk(null, 60, value);
			} catch (NullPointerException e) {

			} catch (Exception e) {
				Assert.fail();
			}

			for (int keySize = 0; keySize < TEST_COUNT; keySize++) {
				// generate key
				String[] keys = new String[keySize];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = "MyKey" + i;
				}

				// REMOVE
				for (String key : keys) {
					mc.delete(key);
				}

				// SET
				Future<Map<String, CollectionOperationStatus>> future = mc
						.asyncSetBulk(Arrays.asList(keys), 60, value);

				Map<String, CollectionOperationStatus> errorList;
				try {
					errorList = future.get(20000L, TimeUnit.MILLISECONDS);
					Assert.assertTrue("Error list is not empty.",
							errorList.isEmpty());
				} catch (TimeoutException e) {
					future.cancel(true);
					e.printStackTrace();
				}

				// GET
				int errorCount = 0;
				for (String key : keys) {
					String v = (String) mc.get(key);
					if (!value.equals(v)) {
						errorCount++;
					}
				}

				Assert.assertEquals("Error count is greater than 0.", 0,
						errorCount);

				// REMOVE
				for (String key : keys) {
					mc.delete(key);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testTimeout() {
		int keySize = 100000;

		String[] keys = new String[keySize];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = "MyKey" + i;
		}

		String value = "MyValue";

		try {
			Future<Map<String, CollectionOperationStatus>> future = mc
					.asyncSetBulk(Arrays.asList(keys), 60, value);

			try {
				future.get(1000L, TimeUnit.MILLISECONDS);
				Assert.fail("There is no timeout.");
			} catch (TimeoutException e) {
				future.cancel(true);
				return;
			} catch (Exception e) {
				future.cancel(true);
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	public void testInsertAndGetUsingSingleClient() {
		String value = "MyValue";

		int TEST_COUNT = 64;

		try {
			// SET null key
			try {
				mc.asyncSetBulk(null, 60, value);
			} catch (NullPointerException e) {

			} catch (Exception e) {
				Assert.fail();
			}

			for (int keySize = 0; keySize < TEST_COUNT; keySize++) {
				// generate key
				String[] keys = new String[keySize];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = "MyKey" + i;
				}

				// REMOVE
				for (String key : keys) {
					mc.delete(key);
				}

				// SET
				Future<Map<String, CollectionOperationStatus>> future = mc
						.asyncSetBulk(Arrays.asList(keys), 60, value);

				Map<String, CollectionOperationStatus> errorList;
				try {
					errorList = future.get(20000L, TimeUnit.MILLISECONDS);
					Assert.assertTrue("Error list is not empty.",
							errorList.isEmpty());
				} catch (TimeoutException e) {
					future.cancel(true);
					e.printStackTrace();
				}

				// GET
				int errorCount = 0;
				for (String key : keys) {
					String v = (String) mc.get(key);
					if (!value.equals(v)) {
						errorCount++;
					}
				}

				Assert.assertEquals("Error count is greater than 0.", 0,
						errorCount);

				// REMOVE
				for (String key : keys) {
					mc.delete(key);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testTimeoutUsingSingleClient() {
		int keySize = 100000;

		String[] keys = new String[keySize];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = "MyKey" + i;
		}

		String value = "MyValue";

		try {
			Future<Map<String, CollectionOperationStatus>> future = mc
					.asyncSetBulk(Arrays.asList(keys), 60, value);

			try {
				future.get(1000L, TimeUnit.MILLISECONDS);
				Assert.fail("There is no timeout.");
			} catch (TimeoutException e) {
				future.cancel(true);
				return;
			} catch (Exception e) {
				future.cancel(true);
				Assert.fail();
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
}
