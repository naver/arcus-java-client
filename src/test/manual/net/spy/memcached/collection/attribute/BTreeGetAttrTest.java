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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.util.BTreeUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BTreeGetAttrTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
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
  public void testGetTrimmedTest() {
    try {
      // create with default.
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setMaxCount(3L);
      Assertions.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              attribute).get());

      Assertions.assertTrue(mc.asyncBopInsert(KEY, 0L, EFLAG, VALUE,
              attribute).get());
      Assertions.assertTrue(mc.asyncBopInsert(KEY, 1L, EFLAG, VALUE,
              attribute).get());
      Assertions.assertTrue(mc.asyncBopInsert(KEY, 2L, EFLAG, VALUE,
              attribute).get());

      // get trimmed
      CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      if (btreeAttrs.getTrimmed() != null) { // not support
        Assertions.assertEquals(Long.valueOf(0L), btreeAttrs.getTrimmed());
      }

      Assertions.assertTrue(mc.asyncBopInsert(KEY, 3L, EFLAG, VALUE,
              attribute).get());
      Assertions.assertTrue(mc.asyncBopInsert(KEY, 4L, EFLAG, VALUE,
              attribute).get());

      // get trimmed
      btreeAttrs = mc.asyncGetAttr(KEY).get();
      if (btreeAttrs.getTrimmed() != null) { // not support
        Assertions.assertEquals(Long.valueOf(1L), btreeAttrs.getTrimmed());
      }

    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetMinMaxBkeyTest() {
    try {
      Assertions.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              new CollectionAttributes()).get());

      CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      Assertions.assertNull(btreeAttrs.getMinBkey());
      Assertions.assertNull(btreeAttrs.getMaxBkey());

      Assertions.assertTrue(mc.asyncBopInsert(KEY, 0L, EFLAG, VALUE, null)
              .get());

      btreeAttrs = mc.asyncGetAttr(KEY).get();
      if (btreeAttrs.getMinBkey() != null) { // not support
        Assertions.assertEquals(Long.valueOf(0L), btreeAttrs.getMinBkey());
        Assertions.assertEquals(Long.valueOf(0L), btreeAttrs.getMaxBkey());
      }

      Assertions.assertTrue(mc.asyncBopInsert(KEY, 1L, EFLAG, VALUE, null)
              .get());
      Assertions.assertTrue(mc.asyncBopInsert(KEY, 2L, EFLAG, VALUE, null)
              .get());

      btreeAttrs = mc.asyncGetAttr(KEY).get();
      if (btreeAttrs.getMinBkey() != null) { // not support
        Assertions.assertEquals(Long.valueOf(0L), btreeAttrs.getMinBkey());
        Assertions.assertEquals(Long.valueOf(2L), btreeAttrs.getMaxBkey());
      }
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetMinMaxBkeyByBytesTest() {
    try {
      Assertions.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              new CollectionAttributes()).get());

      CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      Assertions.assertNull(btreeAttrs.getMinBkeyByBytes());
      Assertions.assertNull(btreeAttrs.getMaxBkeyByBytes());

      Assertions.assertTrue(mc.asyncBopInsert(KEY, new byte[]{0, 0, 1},
              EFLAG, VALUE, null).get());
      btreeAttrs = mc.asyncGetAttr(KEY).get();

      if (btreeAttrs.getMinBkeyByBytes() != null) { // not support
        Assertions.assertEquals("0x000001",
                BTreeUtil.toHex(btreeAttrs.getMinBkeyByBytes()));
        Assertions.assertEquals("0x000001",
                BTreeUtil.toHex(btreeAttrs.getMaxBkeyByBytes()));
      }

      Assertions.assertTrue(mc.asyncBopInsert(KEY, new byte[]{1, 0, 1},
              EFLAG, VALUE, null).get());
      Assertions.assertTrue(mc.asyncBopInsert(KEY, new byte[]{2, 0, 1},
              EFLAG, VALUE, null).get());
      btreeAttrs = mc.asyncGetAttr(KEY).get();
      if (btreeAttrs.getMinBkeyByBytes() != null) { // not support
        Assertions.assertEquals("0x000001",
                BTreeUtil.toHex(btreeAttrs.getMinBkeyByBytes()));
        Assertions.assertEquals("0x020001",
                BTreeUtil.toHex(btreeAttrs.getMaxBkeyByBytes()));
      }
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetOverflowAttrTest() {
    try {
      Assertions.assertTrue(mc.asyncBopCreate(KEY, ElementValueType.STRING,
              new CollectionAttributes()).get());

      CollectionAttributes btreeAttrs = mc.asyncGetAttr(KEY).get();
      CollectionOverflowAction overflowAction = btreeAttrs
              .getOverflowAction();
      Assertions.assertEquals(CollectionOverflowAction.smallest_trim,
              overflowAction);

      // test setattr/getattr smallest_silent_trim
      CollectionAttributes btreeAttrs2 = new CollectionAttributes();
      btreeAttrs2
              .setOverflowAction(CollectionOverflowAction.smallest_silent_trim);
      Assertions.assertTrue(mc.asyncSetAttr(KEY, btreeAttrs2).get());

      btreeAttrs = mc.asyncGetAttr(KEY).get();
      overflowAction = btreeAttrs.getOverflowAction();
      Assertions.assertEquals(CollectionOverflowAction.smallest_silent_trim,
              overflowAction);

      // test setattr/getattr largest_silent_trim
      btreeAttrs2 = new CollectionAttributes();
      btreeAttrs2
              .setOverflowAction(CollectionOverflowAction.largest_silent_trim);
      Assertions.assertTrue(mc.asyncSetAttr(KEY, btreeAttrs2).get());

      btreeAttrs = mc.asyncGetAttr(KEY).get();
      overflowAction = btreeAttrs.getOverflowAction();
      Assertions.assertEquals(CollectionOverflowAction.largest_silent_trim,
              overflowAction);

    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
