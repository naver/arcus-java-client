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

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;

public class BopUpdateTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();

  private final long BKEY = 1000L;

  private final String VALUE = "VALUE";
  private final String NEW_VALUE = "NEWVALUE";

  private final String EFLAG = "EFLAG";
  private final String NEW_EFLAG = "NEW_EFLAG";

  private final byte[] BYTE_EFLAG = new byte[]{1, 0, 0, 0};
  private final byte[] NEW_BYTE_EFLAG = new byte[]{1, 1, 0, 0};

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

  //
  // without bitop
  //

  public void testUpdateZeroLengthEflag() {
    byte[] eflag = new byte[]{0, 0, 0, 0};

    try {
      // insert one
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } catch (ExecutionException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testUpdatePaddingRequired() {
    byte[] eflag = new byte[]{1, 0, 0, 0};

    try {
      // insert one
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } catch (ExecutionException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testUpdateExceededLengthEFlag() {
    byte[] eflag = "1234567890123456789012345678901234567890".getBytes();

    try {
      // insert one
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      // update eflag only
      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(eflag), null).get());
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testUpdateNotExistsKey() {
    try {
      // update value only
      Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY, null, VALUE).get());

      // update eflag only
      Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), null).get());

      // update both value and eflag
      Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());

      // delete eflag
      Assert.assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, null).get());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testExistsKey() {
    try {
      //
      // insert one
      //
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      //
      // update value only
      //
      Assert.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY, null, NEW_VALUE)
              .get());

      Assert.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // update eflag only
      //
      Assert.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, NEW_EFLAG
                              .getBytes()), false, false).get().isEmpty());

      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(NEW_EFLAG.getBytes()), null).get());

      Assert.assertEquals(
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
      Assert.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());

      Assert.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // delete eflag
      //
      Assert.assertEquals(
              VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().get(BKEY)
                      .getValue());

      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, VALUE).get());

      Assert.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().isEmpty());

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  //
  // with bitop
  //
  public void testExistsKeyWithBitOp() {
    try {
      //
      // insert one
      //
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, BYTE_EFLAG, VALUE,
              new CollectionAttributes()).get());
      // 0x01 00 00 00

      //
      // update eflag only
      //
      Assert.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal,
                              NEW_BYTE_EFLAG), false, false).get()
              .isEmpty());

      Assert.assertTrue(mc.asyncBopUpdate(
              KEY,
              BKEY,
              new ElementFlagUpdate(1, BitWiseOperands.OR,
                      new byte[]{1}), null).get());
      // 0x01 01 00 00

      Assert.assertEquals(
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
      Assert.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(NEW_EFLAG.getBytes()), NEW_VALUE)
              .get());

      Assert.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      //
      // delete eflag
      //
      Assert.assertEquals(
              NEW_VALUE,
              mc.asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, NEW_EFLAG
                              .getBytes()), false, false).get().get(BKEY)
                      .getValue());

      Assert.assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              ElementFlagUpdate.RESET_FLAG, VALUE).get());

      Assert.assertTrue(mc
              .asyncBopGet(
                      KEY,
                      BKEY,
                      new ElementFlagFilter(CompOperands.Equal, EFLAG
                              .getBytes()), false, false).get().isEmpty());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
