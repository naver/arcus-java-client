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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.CollectionOperationStatus;

@Ignore
public class SetBulkTimeoutTest extends BaseIntegrationTest {

	public void testTimeoutUsingSingleClient() {
		String value = "MyValue";

		int keySize = 250000;
		String[] keys = new String[keySize];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = "MyBopKey" + i;
		}

		try {
			// SET
			Future<Map<String, CollectionOperationStatus>> future = mc
					.asyncSetBulk(Arrays.asList(keys), 60, value);
			try {
				future.get(10000L, TimeUnit.MILLISECONDS);
				Assert.fail("Timeout is not simulated.");
			} catch (TimeoutException e) {
				future.cancel(true);
				return;
			} catch (Exception e) {
				future.cancel(true);
				Assert.fail();
			}
			Assert.fail();
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

}