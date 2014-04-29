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

import java.util.concurrent.TimeUnit;

import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.transcoders.CollectionTranscoder;

public class CollectionMaxElementSize extends BaseIntegrationTest {

	private String key = "CollectionMaxElementSize";

	public void testExceed() throws Exception {
		CollectionFuture<Boolean> future;
		future = mc.asyncLopInsert(key, -1, "test", new CollectionAttributes());
		assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < CollectionTranscoder.MAX_SIZE + 1; i++) {
			sb.append(i % 9);
		}

		String tooLargeValue = sb.toString();
		assertEquals(CollectionTranscoder.MAX_SIZE + 1, tooLargeValue.length());

		try {
			future = mc.asyncLopInsert(key, -1, tooLargeValue,
					new CollectionAttributes());
			fail();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().contains("Cannot cache data larger than"));
		}
	}

}