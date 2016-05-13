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

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.CollectionOperationStatus;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BulkDeleteTest extends BaseIntegrationTest {

	public void testInsertAndDelete() {
		String value = "MyValue";

		int TEST_COUNT = 64;

		try {
			// DELETE null key
			try {
				List<String> keys = null;
				mc.asyncDeleteBulk(keys);
			} catch (Exception e) {
				assertEquals("Key list is null.", e.getMessage());
			}

			try {
				String[] keys = null;
				mc.asyncDeleteBulk(keys);
			} catch (Exception e) {
				assertEquals("Key list is null.", e.getMessage());
			}

			for (int keySize = 0; keySize < TEST_COUNT; keySize++) {
				// generate key
				String[] keys = new String[keySize];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = "MyKey" + i;
				}

				// SET
				for (String key : keys) {
					mc.set(key, 60, value);
				}

				// Bulk delete
				Future<Map<String, CollectionOperationStatus>> future = mc.
						asyncDeleteBulk(Arrays.asList(keys));

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
					if (v != null) {
						errorCount++;
					}
				}

				Assert.assertEquals("Error count is greater than 0.", 0,
						errorCount);

			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	public void testDeleteNotFoundKey() {
		int TEST_COUNT = 64;

		try {
			for (int keySize = 0; keySize < TEST_COUNT; keySize++) {
				// generate key
				String[] keys = new String[keySize];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = "MyKey" + i;
				}

				// Bulk delete
				Future<Map<String, CollectionOperationStatus>> future = mc.
						asyncDeleteBulk(Arrays.asList(keys));

				Map<String, CollectionOperationStatus> errorList;

				try {
					errorList = future.get(20000L, TimeUnit.MILLISECONDS);
					Assert.assertEquals("Error count is less than input.", keys.length,
							errorList.size());
				} catch (TimeoutException e) {
					future.cancel(true);
					e.printStackTrace();
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

		// SET
		for (String key : keys) {
			mc.set(key, 60, value);
		}

		try {
			Future<Map<String, CollectionOperationStatus>> future = mc
					.asyncDeleteBulk(Arrays.asList(keys));

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

	public void testInsertAndDeleteUsingSingleClient() {
		String value = "MyValue";

		int TEST_COUNT = 64;

		try {
			for (int keySize = 0; keySize < TEST_COUNT; keySize++) {
				// generate key
				String[] keys = new String[keySize];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = "MyKey" + i;
				}

				// SET
				for (String key : keys) {
					mc.set(key, 60, value);
				}

				// Bulk Delete
				Future<Map<String, CollectionOperationStatus>> future = mc.
						asyncDeleteBulk(Arrays.asList(keys));

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
					if (v != null) {
						errorCount++;
					}
				}

				Assert.assertEquals("Error count is greater than 0.", 0,
						errorCount);

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

		// SET
		for (String key : keys) {
			mc.set(key, 60, value);
		}

		try {
			Future<Map<String, CollectionOperationStatus>> future = mc
					.asyncDeleteBulk(Arrays.asList(keys));

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
