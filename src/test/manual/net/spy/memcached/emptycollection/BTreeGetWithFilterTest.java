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

import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BTreeGetWithFilterTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10L;
  private final int VALUE = 1234567890;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();

    boolean insertResult = mc.asyncBopInsert(KEY, BKEY, "flag".getBytes(),
            VALUE, new CollectionAttributes()).get();
    Assertions.assertTrue(insertResult);
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  ;
  @Test
  public void testGetWithDeleteAndWithoutDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());

      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assertions.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, filter, true, false).get()
                      .get(BKEY).getValue());

      // check exists empty btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNotNull(attr);
      Assertions.assertEquals(Long.valueOf(0), attr.getCount());

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertNotNull(map);
      Assertions.assertTrue(map.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetWithDeleteAndWithDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());
      filter.setBitOperand(BitWiseOperands.AND, "flag".getBytes());

      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assertions.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, filter, true, true).get()
                      .get(BKEY).getValue());

      // check btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNull(attr);

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertNull(map);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testRangedGetWithtDeleteAndWithoutDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());

      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assertions.assertEquals(VALUE,
              mc.asyncBopGet(KEY, BKEY, BKEY, filter, 0, 1, true, false)
                      .get().get(BKEY).getValue());

      // check exists empty btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNotNull(attr);
      Assertions.assertEquals(Long.valueOf(0), attr.getCount());

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertNotNull(map);
      Assertions.assertTrue(map.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testRangedGetWithtDeleteAndWithDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());
      filter.setBitOperand(BitWiseOperands.AND, "flag".getBytes());

      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assertions.assertEquals(VALUE,
              mc.asyncBopGet(KEY, BKEY, BKEY, filter, 0, 1, true, true)
                      .get().get(BKEY).getValue());

      // check btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNull(attr);

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertNull(map);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testRangedGetWithtDeleteAndWithDeleteWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());

      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assertions.assertEquals(VALUE,
              mc.asyncBopGet(KEY, BKEY, BKEY, filter, 0, 1, true, false)
                      .get().get(BKEY).getValue());

      // check btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNotNull(attr);

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assertions.assertNotNull(map);
      Assertions.assertTrue(map.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

}
