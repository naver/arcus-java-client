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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BopUpdateTest extends BaseIntegrationTest {

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
    Assertions.assertNull(mc.asyncGetAttr(KEY).get());
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
  public void testUpdateZeroLengthEflag() {
    byte[] eflag = new byte[]{0, 0, 0, 0};

    try {
      // insert one
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testUpdatePaddingRequired() {
    byte[] eflag = new byte[]{1, 0, 0, 0};

    try {
      // insert one
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testUpdateExceededLengthEFlag() {
    byte[] eflag = "1234567890123456789012345678901234567890".getBytes();

    try {
      // insert one
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testUpdateNotExistsKey() {
    try {
      // update value only
      Assertions.assertFalse(mc.asyncBopUpdate(KEY, BKEY, null, VALUE).get());

      // update eflag only
      Assertions.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), null).get());

      // update both value and eflag
      Assertions.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());

      // delete eflag
      Assertions.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, null).get());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testExistsKey() {
    try {
      //
      // insert one
      //
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      //
      // update value only
      //
      Assertions.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY, null, NEW_VALUE)
              .get());

      Assertions.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // update eflag only
      //
      Assertions.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, NEW_EFLAG
                              .getBytes()), false, false).get().isEmpty());

      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(NEW_EFLAG.getBytes()), null).get());

      Assertions.assertEquals(
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
      Assertions.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());

      Assertions.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // delete eflag
      //
      Assertions.assertEquals(
              VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().get(BKEY)
                      .getValue());

      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, VALUE).get());

      Assertions.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().isEmpty());

    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

  //
  // with bitop
  //
  @Test
  public void testExistsKeyWithBitOp() {
    try {
      //
      // insert one
      //
      Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, BYTE_EFLAG, VALUE,
              new CollectionAttributes()).get());
      // 0x01 00 00 00

      //
      // update eflag only
      //
      Assertions.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal,
                              NEW_BYTE_EFLAG), false, false).get()
              .isEmpty());

      Assertions.assertTrue(mc.asyncBopUpdate(
              KEY,
              BKEY,
              new ElementFlagUpdate(1, BitWiseOperands.OR,
                      new byte[]{1}), null).get());
      // 0x01 01 00 00

      Assertions.assertEquals(
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
      Assertions.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(NEW_EFLAG.getBytes()), NEW_VALUE)
              .get());

      Assertions.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // delete eflag
      //
      Assertions.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, NEW_EFLAG
                              .getBytes()), false, false).get().get(BKEY)
                      .getValue());

      Assertions.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, VALUE).get());

      Assertions.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().isEmpty());
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
