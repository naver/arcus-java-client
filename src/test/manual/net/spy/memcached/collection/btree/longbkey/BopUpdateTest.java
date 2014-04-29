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
package net.spy.memcached.collection.btree.longbkey;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagUpdate;

public class BopUpdateTest extends BaseIntegrationTest {

	private final String KEY = this.getClass().getSimpleName();
	private final byte[] BKEY = new byte[] { (byte) 1 };
	private final String VALUE = "VALUE";
	private final String EFLAG = "EFLAG";

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

	public void testNotExistsUpdateWithValue() {
		try {
			Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(new byte[] { 0 }), VALUE).get());

			Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	public void testNotExistsUpdateWithoutValue() {
		try {
			Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(new byte[] { 0 }), null).get());

			Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(EFLAG.getBytes()), null).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	public void testExistsUpdateWithValue() {
		try {
			Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
					new CollectionAttributes()).get());

			Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(new byte[] { 0 }), VALUE).get());

			Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	public void testExistsUpdateWithoutValue() {
		try {
			Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
					new CollectionAttributes()).get());

			Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(new byte[] { 0 }), null).get());

			Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
					new ElementFlagUpdate(EFLAG.getBytes()), null).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	//
	//
	//
	// with bitop
	//
	//
	//

	public void testNotExistsUpdateUsingBitOpWithValue() {
		try {
			Assert.assertFalse(mc.asyncBopUpdate(
					KEY,
					BKEY,
					new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
							.getBytes()), VALUE).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	public void testNotExistsUpdateUsingBitOpWithoutValue() {
		try {
			Assert.assertFalse(mc.asyncBopUpdate(
					KEY,
					BKEY,
					new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
							.getBytes()), null).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	public void testExistsUpdateUsingBitOpWithValue() {
		try {
			Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, EFLAG.getBytes(),
					VALUE, new CollectionAttributes()).get());

			Assert.assertTrue(mc.asyncBopUpdate(
					KEY,
					BKEY,
					new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
							.getBytes()), VALUE).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	public void testExistsUpdateUsingBitOpWithoutValue() {
		try {
			Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, EFLAG.getBytes(),
					VALUE, new CollectionAttributes()).get());

			Assert.assertTrue(mc.asyncBopUpdate(
					KEY,
					BKEY,
					new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
							.getBytes()), null).get());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

}
