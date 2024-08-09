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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;
import net.spy.memcached.collection.ElementMultiFlagsFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BopInsertAndGetWithElementFlagTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10L;
  private final String VALUE = "VALUE";
  private final byte[] FLAG = "FLAG".getBytes();

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testBopInsertAndGetWithEFlag() throws Exception {
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            ElementFlagFilter.DO_NOT_FILTER, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(1, map.size());
    Assertions.assertEquals(VALUE, map.get(BKEY).getValue());
  }

  @Test
  public void testBopInsertAndGetWithoutEFlag() throws Exception {
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            ElementFlagFilter.DO_NOT_FILTER, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(1, map.size());
    Assertions.assertEquals(VALUE, map.get(BKEY).getValue());
  }

  @Test
  public void testBopInsertAndRangedGetWithEFlag() throws Exception {

    // insert 3 bkeys
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, FLAG, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY, BKEY + 2,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(3, map.size());
    Assertions.assertEquals(VALUE, map.get(BKEY).getValue());
    Assertions.assertEquals(VALUE, map.get(BKEY + 1).getValue());
    Assertions.assertEquals(VALUE, map.get(BKEY + 2).getValue());
  }

  @Test
  public void testBopInsertAndRangedGetWithoutEFlag() throws Exception {

    // insert 3 bkeys
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, null, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, null, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY, BKEY + 2,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(3, map.size());
    Assertions.assertEquals(VALUE, map.get(BKEY).getValue());
    Assertions.assertEquals(VALUE, map.get(BKEY + 1).getValue());
    Assertions.assertEquals(VALUE, map.get(BKEY + 2).getValue());
  }

  @Test
  public void testGetAllOfNotFlaggedBkeys() throws Exception {
    // insert 3 bkeys
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, null, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 3, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 4, null, VALUE,
            new CollectionAttributes()).get());

    // get not flagged bkeys
    ElementFlagFilter filter = new ElementFlagFilter(CompOperands.NotEqual,
            new byte[]{0});
    filter.setBitOperand(BitWiseOperands.AND, new byte[]{0});

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY, BKEY + 100,
            filter, 0, 100, false, false).get();

    Assertions.assertEquals(3, map.size());
  }

  @Test
  public void testBopInsertAndRangedGetWithEFlags() throws Exception {

    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, new byte[]{0}, VALUE,
            new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, new byte[]{1},
            VALUE, new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, new byte[]{2},
            VALUE, new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 3, new byte[]{3},
            VALUE, new CollectionAttributes()).get());
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY + 4, new byte[]{4},
            VALUE, new CollectionAttributes()).get());

    ElementMultiFlagsFilter filter = new ElementMultiFlagsFilter();
    filter.setCompOperand(CompOperands.Equal);
    filter.addCompValue(new byte[]{0});
    filter.addCompValue(new byte[]{1});
    filter.addCompValue(new byte[]{2});
    filter.addCompValue(new byte[]{3});

    // filter.setBitOperand(BitWiseOperands.AND, new byte[] { 0 });

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY, BKEY + 100,
            filter, 0, 100, false, false).get();

    Assertions.assertEquals(4, map.size());

    ElementMultiFlagsFilter filter2 = new ElementMultiFlagsFilter();
    filter2.setCompOperand(CompOperands.NotEqual);
    filter2.addCompValue(new byte[]{0});
    filter2.addCompValue(new byte[]{1});
    filter2.addCompValue(new byte[]{2});
    filter2.addCompValue(new byte[]{3});

    // filter.setBitOperand(BitWiseOperands.AND, new byte[] { 0 });

    Map<Long, Element<Object>> map2 = mc.asyncBopGet(KEY, BKEY, BKEY + 100,
            filter2, 0, 100, false, false).get();

    Assertions.assertEquals(1, map2.size());
  }
}
