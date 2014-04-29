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
package net.spy.memcached.emptycollection;

import java.util.Set;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

public class GetWithDropSetTest extends BaseIntegrationTest {

	private final String KEY = this.getClass().getSimpleName();
	private final int VALUE = 1234567890;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		mc.delete(KEY).get();

		boolean insertResult = mc.asyncSopInsert(KEY, VALUE,
				new CollectionAttributes()).get();
		Assert.assertTrue(insertResult);
	}

	public void testGetWithoutDeleteAndDrop() {
		try {
			// check attr
			Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
					.getCount());

			// get value delete=false, drop=true
			Assert.assertTrue(mc.asyncSopGet(KEY, 10, false, false).get()
					.contains(VALUE));

			// check exists
			Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
					.getCount());

			// get value again
			Assert.assertTrue(mc.asyncSopGet(KEY, 10, false, false).get()
					.contains(VALUE));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	public void testGetWithtDeleteAndWithoutDrop() {
		try {
			// check attr
			Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
					.getCount());

			// get value delete=true, drop=false
			Assert.assertTrue(mc.asyncSopGet(KEY, 10, true, false).get()
					.contains(VALUE));

			// check exists empty btree
			CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
			Assert.assertNotNull(attr);
			Assert.assertEquals(new Long(0), attr.getCount());

			Set<Object> set = mc.asyncSopGet(KEY, 10, false, false).get();
			Assert.assertNotNull(set);
			Assert.assertTrue(set.isEmpty());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	public void testGetWithtDeleteAndWithDrop() {
		try {
			// check attr
			Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
					.getCount());

			// get value delete=true, drop=false
			Assert.assertTrue(mc.asyncSopGet(KEY, 10, true, true).get()
					.contains(VALUE));

			// check exists empty set
			CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
			Assert.assertNull(attr);

			Set<Object> set = mc.asyncSopGet(KEY, 10, false, false).get();
			Assert.assertNull(set);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
