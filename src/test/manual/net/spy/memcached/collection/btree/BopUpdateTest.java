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
package net.spy.memcached.collection.btree;

import java.util.concurrent.ExecutionException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;
import net.spy.memcached.collection.ElementFlagUpdate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class BopUpdateTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();

  private final long BKEY = 1000L;

  private final String VALUE = "VALUE";
  private final String NEW_VALUE = "NEWVALUE";

  private final String EFLAG = "EFLAG";
  private final String NEW_EFLAG = "NEW_EFLAG";

  private final byte[] BYTE_EFLAG = new byte[]{1, 0, 0, 0};
  private final byte[] NEW_BYTE_EFLAG = new byte[]{1, 1, 0, 0};

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    assertNull(mc.asyncGetAttr(KEY).get());
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  //
  // without bitop
  //

  @Test
  void testUpdateZeroLengthEflag() {
    byte[] eflag = new byte[]{0, 0, 0, 0};

    try {
      // insert one
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } catch (ExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testUpdatePaddingRequired() {
    byte[] eflag = new byte[]{1, 0, 0, 0};

    try {
      // insert one
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } catch (ExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testUpdateExceededLengthEFlag() {
    byte[] eflag = "1234567890123456789012345678901234567890".getBytes();

    try {
      // insert one
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testUpdateNotExistsKey() {
    try {
      // update value only
      assertFalse(mc.asyncBopUpdate(KEY, BKEY, null, VALUE).get());

      // update eflag only
      assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), null).get());

      // update both value and eflag
      assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());

      // delete eflag
      assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, null).get());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testExistsKey() {
    try {
      //
      // insert one
      //
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      //
      // update value only
      //
      assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY, null, NEW_VALUE)
              .get());

      assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // update eflag only
      //
      assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, NEW_EFLAG
                              .getBytes()), false, false).get().isEmpty());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(NEW_EFLAG.getBytes()), null).get());

      assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, NEW_EFLAG
                              .getBytes()), false, false).get().get(BKEY)
                      .getValue());

      //
      // update both value and eflag
      //
      assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());

      assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // delete eflag
      //
      assertEquals(
              VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().get(BKEY)
                      .getValue());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, VALUE).get());

      assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().isEmpty());

    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  //
  // with bitop
  //
  @Test
  void testExistsKeyWithBitOp() {
    try {
      //
      // insert one
      //
      assertTrue(mc.asyncBopInsert(KEY, BKEY, BYTE_EFLAG, VALUE,
              new CollectionAttributes()).get());
      // 0x01 00 00 00

      //
      // update eflag only
      //
      assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal,
                              NEW_BYTE_EFLAG), false, false).get()
              .isEmpty());

      assertTrue(mc.asyncBopUpdate(
              KEY,
              BKEY,
              new ElementFlagUpdate(1, BitWiseOperands.OR,
                      new byte[]{1}), null).get());
      // 0x01 01 00 00

      assertEquals(
              VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal,
                              NEW_BYTE_EFLAG), false, false).get()
                      .get(BKEY).getValue());

      //
      // update both value and eflag
      //
      assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(NEW_EFLAG.getBytes()), NEW_VALUE)
              .get());

      assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // delete eflag
      //
      assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, NEW_EFLAG
                              .getBytes()), false, false).get().get(BKEY)
                      .getValue());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, VALUE).get());

      assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().isEmpty());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
