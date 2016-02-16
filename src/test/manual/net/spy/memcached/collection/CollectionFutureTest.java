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
package net.spy.memcached.collection;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.OperationStatus;

public class CollectionFutureTest extends BaseIntegrationTest {

	private String key = "CollectionFutureTest";

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		mc.asyncBopDelete(key, 0, 100, ElementFlagFilter.DO_NOT_FILTER, 0, true);
		mc.asyncMopDelete(key, 0, true);
	}

	public void testAfterSuccess() throws Exception {
		CollectionFuture<Boolean> future;
		OperationStatus status;

		future = (CollectionFuture<Boolean>) mc.asyncBopInsert(key, 0, null,
				"hello", new CollectionAttributes());

		// OperationStatus should be null before operation completion
		// status = future.getOperationStatus();
		// assertNull(status);

		// After operation completion (SUCCESS)
		Boolean success = future.get(1000, TimeUnit.MILLISECONDS);
		status = future.getOperationStatus();

		assertTrue(success);
		assertNotNull(status);
		assertTrue(status.isSuccess());
		assertEquals("CREATED_STORED", status.getMessage());
	}

	public void testAfterSuccessForMap() throws Exception {
		/* Map future test */
		CollectionFuture<Boolean> future;
		OperationStatus status;

		future = (CollectionFuture<Boolean>) mc.asyncMopInsert(key, "field", "hello", new CollectionAttributes());
		Boolean success = future.get(1000, TimeUnit.MILLISECONDS);
		status = future.getOperationStatus();

		assertTrue(success);
		assertNotNull(status);
		assertTrue(status.isSuccess());
		assertEquals("CREATED_STORED", status.getMessage());
	}

	public void testAfterFailure() throws Exception {
		CollectionFuture<Map<Long, Element<Object>>> future;
		OperationStatus status;

		future = (CollectionFuture<Map<Long, Element<Object>>>) mc.asyncBopGet(
				key, 0, ElementFlagFilter.DO_NOT_FILTER, false, false);

		// OperationStatus should be null before operation completion
		// status = future.getOperationStatus();
		// assertNull(status);

		// After operation completion (FAILURE)
		Map<Long, Element<Object>> result = future.get(1000,
				TimeUnit.MILLISECONDS);
		status = future.getOperationStatus();

		assertNull(result);
		assertNotNull(status);
		assertFalse(status.isSuccess());
		assertEquals("NOT_FOUND", status.getMessage());
	}

	public void testAfterFailureForMap() throws Exception {
		CollectionFuture<Map<String, Object>> future;
		OperationStatus status;

		future = (CollectionFuture<Map<String, Object>>) mc.asyncMopGet(
				key, 0, false, false);

		// OperationStatus should be null before operation completion
		// status = future.getOperationStatus();
		// assertNull(status);

		// After operation completion (FAILURE)
		Map<String, Object> result = future.get(1000,
				TimeUnit.MILLISECONDS);
		status = future.getOperationStatus();

		assertNull(result);
		assertNotNull(status);
		assertFalse(status.isSuccess());
		assertEquals("NOT_FOUND", status.getMessage());
	}

	public void testTimeout() throws Exception {
		CollectionFuture<Boolean> future;
		OperationStatus status;

		future = (CollectionFuture<Boolean>) mc.asyncBopInsert(key, 0, null,
				"hello", new CollectionAttributes());

		try {
			future.get(1, TimeUnit.NANOSECONDS);
		} catch (Exception e) {
			future.cancel(true);
		}

		status = future.getOperationStatus();

		// assertNull(status);
	}

	public void testTimeoutForMap() throws Exception {
		CollectionFuture<Boolean> future;
		OperationStatus status;

		future = (CollectionFuture<Boolean>) mc.asyncMopInsert(key, "field", "hello", new CollectionAttributes());

		try {
			future.get(1, TimeUnit.NANOSECONDS);
		} catch (Exception e) {
			future.cancel(true);
		}

		status = future.getOperationStatus();

		// assertNull(status);
	}
}
