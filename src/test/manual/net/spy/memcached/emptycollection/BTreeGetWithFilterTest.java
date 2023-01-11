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

import org.junit.Assert;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;

public class BTreeGetWithFilterTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10L;
  private final int VALUE = 1234567890;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();

    boolean insertResult = mc.asyncBopInsert(KEY, BKEY, "flag".getBytes(),
            VALUE, new CollectionAttributes()).get();
    Assert.assertTrue(insertResult);
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  ;

  public void testGetWithDeleteAndWithoutDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());

      // check attr
      Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assert.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, filter, true, false).get()
                      .get(BKEY).getValue());

      // check exists empty btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assert.assertNotNull(attr);
      Assert.assertEquals(new Long(0), attr.getCount());

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNotNull(map);
      Assert.assertTrue(map.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testGetWithDeleteAndWithDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());
      filter.setBitOperand(BitWiseOperands.AND, "flag".getBytes());

      // check attr
      Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assert.assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, filter, true, true).get()
                      .get(BKEY).getValue());

      // check btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assert.assertNull(attr);

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNull(map);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testRangedGetWithtDeleteAndWithoutDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());

      // check attr
      Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assert.assertEquals(VALUE,
              mc.asyncBopGet(KEY, BKEY, BKEY, filter, 0, 1, true, false)
                      .get().get(BKEY).getValue());

      // check exists empty btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assert.assertNotNull(attr);
      Assert.assertEquals(new Long(0), attr.getCount());

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNotNull(map);
      Assert.assertTrue(map.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testRangedGetWithtDeleteAndWithDropWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());
      filter.setBitOperand(BitWiseOperands.AND, "flag".getBytes());

      // check attr
      Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assert.assertEquals(VALUE,
              mc.asyncBopGet(KEY, BKEY, BKEY, filter, 0, 1, true, true)
                      .get().get(BKEY).getValue());

      // check btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assert.assertNull(attr);

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNull(map);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testRangedGetWithtDeleteAndWithDeleteWithFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "flag".getBytes());

      // check attr
      Assert.assertEquals(new Long(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assert.assertEquals(VALUE,
              mc.asyncBopGet(KEY, BKEY, BKEY, filter, 0, 1, true, false)
                      .get().get(BKEY).getValue());

      // check btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assert.assertNotNull(attr);

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNotNull(map);
      Assert.assertTrue(map.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

}
