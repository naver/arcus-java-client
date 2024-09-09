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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagUpdate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class BopUpdateTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final byte[] BKEY = new byte[]{(byte) 1};
  private final String VALUE = "VALUE";
  private final String EFLAG = "EFLAG";

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

  @Test
  public void testNotExistsUpdateWithValue() {
    try {
      assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(new byte[]{0}), VALUE).get());

      assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testNotExistsUpdateWithoutValue() {
    try {
      assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(new byte[]{0}), null).get());

      assertFalse(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), null).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExistsUpdateWithValue() {
    try {
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(new byte[]{0}), VALUE).get());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), VALUE).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExistsUpdateWithoutValue() {
    try {
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
              new CollectionAttributes()).get());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(new byte[]{0}), null).get());

      assertTrue(mc.asyncBopUpdate(KEY, BKEY,
              new ElementFlagUpdate(EFLAG.getBytes()), null).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  //
  //
  //
  // with bitop
  //
  //
  //

  @Test
  public void testNotExistsUpdateUsingBitOpWithValue() {
    try {
      assertFalse(mc.asyncBopUpdate(
              KEY,
              BKEY,
              new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
                      .getBytes()), VALUE).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testNotExistsUpdateUsingBitOpWithoutValue() {
    try {
      assertFalse(mc.asyncBopUpdate(
              KEY,
              BKEY,
              new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
                      .getBytes()), null).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExistsUpdateUsingBitOpWithValue() {
    try {
      assertTrue(mc.asyncBopInsert(KEY, BKEY, EFLAG.getBytes(),
              VALUE, new CollectionAttributes()).get());

      assertTrue(mc.asyncBopUpdate(
              KEY,
              BKEY,
              new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
                      .getBytes()), VALUE).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testExistsUpdateUsingBitOpWithoutValue() {
    try {
      assertTrue(mc.asyncBopInsert(KEY, BKEY, EFLAG.getBytes(),
              VALUE, new CollectionAttributes()).get());

      assertTrue(mc.asyncBopUpdate(
              KEY,
              BKEY,
              new ElementFlagUpdate(0, BitWiseOperands.AND, EFLAG
                      .getBytes()), null).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

}
