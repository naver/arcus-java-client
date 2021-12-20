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

import junit.framework.Assert;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementMultiFlagsFilter;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;

public class BopInsertAndGetWithElementFlagTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10L;
  private final String VALUE = "VALUE";
  private final byte[] FLAG = KeyUtil.getKeyBytes("FLAG");

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  ;

  public void testBopInsertAndGetWithEFlag() throws Exception {
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            ElementFlagFilter.DO_NOT_FILTER, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(VALUE, map.get(BKEY).getValue());
  }

  public void testBopInsertAndGetWithoutEFlag() throws Exception {
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            ElementFlagFilter.DO_NOT_FILTER, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(VALUE, map.get(BKEY).getValue());
  }

  public void testBopInsertAndRangedGetWithEFlag() throws Exception {

    // insert 3 bkeys
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, FLAG, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY, BKEY + 2,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(3, map.size());
    Assert.assertEquals(VALUE, map.get(BKEY).getValue());
    Assert.assertEquals(VALUE, map.get(BKEY + 1).getValue());
    Assert.assertEquals(VALUE, map.get(BKEY + 2).getValue());
  }

  public void testBopInsertAndRangedGetWithoutEFlag() throws Exception {

    // insert 3 bkeys
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, null, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, null, VALUE,
            new CollectionAttributes()).get());

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY, BKEY + 2,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get(
            Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(3, map.size());
    Assert.assertEquals(VALUE, map.get(BKEY).getValue());
    Assert.assertEquals(VALUE, map.get(BKEY + 1).getValue());
    Assert.assertEquals(VALUE, map.get(BKEY + 2).getValue());
  }

  public void testGetAllOfNotFlaggedBkeys() throws Exception {
    // insert 3 bkeys
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, null, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 3, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 4, null, VALUE,
            new CollectionAttributes()).get());

    // get not flagged bkeys
    ElementFlagFilter filter = new ElementFlagFilter(CompOperands.NotEqual,
            new byte[]{0});
    filter.setBitOperand(BitWiseOperands.AND, new byte[]{0});

    Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY, BKEY + 100,
            filter, 0, 100, false, false).get();

    Assert.assertEquals(3, map.size());
  }

  public void testBopInsertAndRangedGetWithEFlags() throws Exception {

    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, new byte[]{0}, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 1, new byte[]{1},
            VALUE, new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 2, new byte[]{2},
            VALUE, new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 3, new byte[]{3},
            VALUE, new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY + 4, new byte[]{4},
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

    Assert.assertEquals(4, map.size());

    ElementMultiFlagsFilter filter2 = new ElementMultiFlagsFilter();
    filter2.setCompOperand(CompOperands.NotEqual);
    filter2.addCompValue(new byte[]{0});
    filter2.addCompValue(new byte[]{1});
    filter2.addCompValue(new byte[]{2});
    filter2.addCompValue(new byte[]{3});

    // filter.setBitOperand(BitWiseOperands.AND, new byte[] { 0 });

    Map<Long, Element<Object>> map2 = mc.asyncBopGet(KEY, BKEY, BKEY + 100,
            filter2, 0, 100, false, false).get();

    Assert.assertEquals(1, map2.size());
  }
}
