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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MaxBkeyRangeTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 1;
  private final byte[] BYTE_BKEY = new byte[]{(byte) 1};
  private final String VALUE = "VALUE";
  private final byte[] EFLAG = "eflag".getBytes();

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
  public void testMaxBkeyRangeTest() {
    try {
      // create with default.
      CollectionAttributes attribute = new CollectionAttributes();

      Assertions.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              attribute).get());

      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, EFLAG, VALUE,
              attribute).get());

      // get current maxbkeyrange
      final CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      assertThrows(IllegalStateException.class, () -> btreeAttrs.getMaxBkeyRangeByBytes());
      Assertions.assertEquals(Long.valueOf(0), btreeAttrs.getMaxBkeyRange());

      // change maxbkeyrange
      attribute.setMaxBkeyRange(2L);
      Assertions.assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get current maxbkeyrange
      final CollectionAttributes changedBtreeAttrs = mc.asyncGetAttr(KEY).get();
      assertThrows(IllegalStateException.class, () -> changedBtreeAttrs.getMaxBkeyRangeByBytes());
      Assertions.assertEquals(Long.valueOf(2),
              changedBtreeAttrs.getMaxBkeyRange());

      // insert bkey
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, EFLAG, VALUE,
              attribute).get());

      // get all of bkeys
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get();
      Assertions.assertEquals(2, map.size());
      Assertions.assertTrue(map.containsKey(BKEY));
      Assertions.assertTrue(map.containsKey(BKEY + 1));

      // insert one more bkey
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, EFLAG, VALUE,
              attribute).get());

      // get all of bkeys
      Map<Long, Element<Object>> map2 = mc.asyncBopGet(KEY, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get();
      Assertions.assertEquals(3, map2.size());
      Assertions.assertTrue(map2.containsKey(BKEY));
      Assertions.assertTrue(map2.containsKey(BKEY + 1));
      Assertions.assertTrue(map2.containsKey(BKEY + 2));

      // insert one more bkey again
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 3, EFLAG, VALUE,
              attribute).get());

      // get all of bkeys
      Map<Long, Element<Object>> map3 = mc.asyncBopGet(KEY, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get();
      Assertions.assertEquals(3, map3.size());
      Assertions.assertTrue(map3.containsKey(BKEY + 1));
      Assertions.assertTrue(map3.containsKey(BKEY + 2));
      Assertions.assertTrue(map3.containsKey(BKEY + 3));
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testMaxBkeyRangeByBytesTest() {
    try {
      // create with default.
      CollectionAttributes attribute = new CollectionAttributes();

      Assertions.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              attribute).get());

      Assertions.assertTrue(mc.asyncBopInsert(KEY, BYTE_BKEY, EFLAG, VALUE,
              attribute).get());

      // get current maxbkeyrange
      CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      Assertions.assertEquals(Long.valueOf(0), btreeAttrs.getMaxBkeyRange());

      // change maxbkeyrange
      attribute.setMaxBkeyRangeByBytes(new byte[]{(byte) 2});
      Assertions.assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get current maxbkeyrange
      CollectionAttributes changedBtreeAttrs = mc.asyncGetAttr(KEY).get();
      Assertions.assertTrue(Arrays.equals(new byte[]{(byte) 2},
              changedBtreeAttrs.getMaxBkeyRangeByBytes()));
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
