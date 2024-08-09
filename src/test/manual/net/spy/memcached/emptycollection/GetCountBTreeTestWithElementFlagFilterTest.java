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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GetCountBTreeTestWithElementFlagFilterTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10L;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assertions.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testGetBKeyCountFromInvalidKey() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.GreaterThan, "1".getBytes());

      CollectionFuture<Integer> future = mc.asyncBopGetItemCount(
              "INVALIDKEY", BKEY, BKEY, filter);
      Integer count = future.get();
      Assertions.assertNull(count);
      Assertions.assertFalse(future.getOperationStatus().isSuccess());
      Assertions.assertEquals(CollectionResponse.NOT_FOUND, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBKeyCountFromInvalidType() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "eflag".getBytes());

      // insert value into set
      Boolean insertResult = mc.asyncSopInsert(KEY, "value",
              new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult);

      // get count from key
      CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY,
              BKEY, BKEY, filter);
      Integer count = future.get();
      Assertions.assertNull(count);
      Assertions.assertFalse(future.getOperationStatus().isSuccess());
      Assertions.assertEquals(CollectionResponse.TYPE_MISMATCH, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBKeyCountFromNotEmpty() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "eflag".getBytes());

      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      // insert two items
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              "eflag".getBytes(), "value", new CollectionAttributes())
              .get();
      Assertions.assertTrue(insertResult);

      Boolean insertResult2 = mc.asyncBopInsert(KEY, BKEY + 1,
              "eflag".getBytes(), "value", new CollectionAttributes())
              .get();
      Assertions.assertTrue(insertResult2);

      // check count in attributes
      Assertions.assertEquals(Long.valueOf(2), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get btree item count
      CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY,
              BKEY, BKEY, filter);
      Integer count = future.get();
      Assertions.assertNotNull(count);
      Assertions.assertEquals(Integer.valueOf(1), count);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBKeyCountFromNotEmpty2() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "eflag".getBytes());

      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      // insert two items
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY, null, "value",
              new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult);

      Boolean insertResult2 = mc.asyncBopInsert(KEY, BKEY + 1, null,
              "value", new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult2);

      // check count in attributes
      Assertions.assertEquals(Long.valueOf(2), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get btree item count
      CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY,
              BKEY, BKEY + 1, filter);
      Integer count = future.get();
      Assertions.assertNotNull(count);
      Assertions.assertEquals(Integer.valueOf(0), count);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBKeyCountFromNotEmpty3() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "eflag".getBytes());

      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      // insert two items
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              "eflag".getBytes(), "value", new CollectionAttributes())
              .get();
      Assertions.assertTrue(insertResult);

      Boolean insertResult2 = mc.asyncBopInsert(KEY, BKEY + 1,
              "eflageflag".getBytes(), "value",
              new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult2);

      // check count in attributes
      Assertions.assertEquals(Long.valueOf(2), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get btree item count
      CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY,
              BKEY, BKEY, filter);
      Integer count = future.get();
      Assertions.assertNotNull(count);
      Assertions.assertEquals(Integer.valueOf(1), count);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBKeyCountByNotExistsBKey() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "eflag".getBytes());

      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      // insert two items
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY, null, "value",
              new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult);

      Boolean insertResult2 = mc.asyncBopInsert(KEY, BKEY + 1, null,
              "value", new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult2);

      // check count in attributes
      Assertions.assertEquals(Long.valueOf(2), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get btree item count
      CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY,
              BKEY + 3, BKEY + 3, filter);
      Integer count = future.get();
      Assertions.assertNotNull(count);
      Assertions.assertEquals(Integer.valueOf(0), count);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetBKeyCountByNotExistsRange() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "eflag".getBytes());

      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      // insert two items
      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY, null, "value",
              new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult);

      Boolean insertResult2 = mc.asyncBopInsert(KEY, BKEY + 1, null,
              "value", new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult2);

      // check count in attributes
      Assertions.assertEquals(Long.valueOf(2), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get btree item count
      CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY,
              BKEY + 2, BKEY + 3, filter);
      Integer count = future.get();
      Assertions.assertNotNull(count);
      Assertions.assertEquals(Integer.valueOf(0), count);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }
}
