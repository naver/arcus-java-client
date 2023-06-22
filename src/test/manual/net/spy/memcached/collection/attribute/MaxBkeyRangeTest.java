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
package net.spy.memcached.collection.attribute;

import java.util.Arrays;
import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;

import org.junit.Assert;
import org.junit.function.ThrowingRunnable;

import static org.junit.Assert.assertThrows;

public class MaxBkeyRangeTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 1;
  private final byte[] BYTE_BKEY = new byte[]{(byte) 1};
  private final String VALUE = "VALUE";
  private final byte[] EFLAG = "eflag".getBytes();

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

  public void testMaxBkeyRangeTest() {
    try {
      // create with default.
      CollectionAttributes attribute = new CollectionAttributes();

      Assert.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              attribute).get());

      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, EFLAG, VALUE,
              attribute).get());

      // get current maxbkeyrange
      final CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      assertThrows(IllegalStateException.class, new ThrowingRunnable() {
        @Override
        public void run() throws Throwable {
          btreeAttrs.getMaxBkeyRangeByBytes();
        }
      });
      Assert.assertEquals(Long.valueOf(0), btreeAttrs.getMaxBkeyRange());

      // change maxbkeyrange
      attribute.setMaxBkeyRange(2L);
      Assert.assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get current maxbkeyrange
      final CollectionAttributes changedBtreeAttrs = mc.asyncGetAttr(KEY).get();
      assertThrows(IllegalStateException.class, new ThrowingRunnable() {
        @Override
        public void run() throws Throwable {
          changedBtreeAttrs.getMaxBkeyRangeByBytes();
        }
      });
      Assert.assertEquals(Long.valueOf(2),
              changedBtreeAttrs.getMaxBkeyRange());

      // insert bkey
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, EFLAG, VALUE,
              attribute).get());

      // get all of bkeys
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get();
      Assert.assertEquals(2, map.size());
      Assert.assertTrue(map.containsKey(BKEY));
      Assert.assertTrue(map.containsKey(BKEY + 1));

      // insert one more bkey
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, EFLAG, VALUE,
              attribute).get());

      // get all of bkeys
      Map<Long, Element<Object>> map2 = mc.asyncBopGet(KEY, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get();
      Assert.assertEquals(3, map2.size());
      Assert.assertTrue(map2.containsKey(BKEY));
      Assert.assertTrue(map2.containsKey(BKEY + 1));
      Assert.assertTrue(map2.containsKey(BKEY + 2));

      // insert one more bkey again
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 3, EFLAG, VALUE,
              attribute).get());

      // get all of bkeys
      Map<Long, Element<Object>> map3 = mc.asyncBopGet(KEY, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get();
      Assert.assertEquals(3, map3.size());
      Assert.assertTrue(map3.containsKey(BKEY + 1));
      Assert.assertTrue(map3.containsKey(BKEY + 2));
      Assert.assertTrue(map3.containsKey(BKEY + 3));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testMaxBkeyRangeByBytesTest() {
    try {
      // create with default.
      CollectionAttributes attribute = new CollectionAttributes();

      Assert.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              attribute).get());

      Assert.assertTrue(mc.asyncBopInsert(KEY, BYTE_BKEY, EFLAG, VALUE,
              attribute).get());

      // get current maxbkeyrange
      CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      Assert.assertEquals(Long.valueOf(0), btreeAttrs.getMaxBkeyRange());

      // change maxbkeyrange
      attribute.setMaxBkeyRangeByBytes(new byte[]{(byte) 2});
      Assert.assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get current maxbkeyrange
      CollectionAttributes changedBtreeAttrs = mc.asyncGetAttr(KEY).get();
      Assert.assertTrue(Arrays.equals(new byte[]{(byte) 2},
              changedBtreeAttrs.getMaxBkeyRangeByBytes()));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
