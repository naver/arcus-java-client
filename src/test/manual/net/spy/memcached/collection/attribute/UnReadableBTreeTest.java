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

import java.util.Map;

import org.junit.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.internal.CollectionFuture;

public class UnReadableBTreeTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String VALUE = "VALUE";
  private final long BKEY = 10L;

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

  public void testCreateUnreadableBTreeTest() {
    try {
      // create unreadable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(false);

      Boolean insertResult = mc.asyncBopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assert.assertEquals(new Long(0), attr.getCount());
      Assert.assertEquals(new Long(4000), attr.getMaxCount());
      Assert.assertEquals(new Integer(0), attr.getExpireTime());
      Assert.assertFalse(attr.getReadable());

      // insert an item
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE, null)
              .get());

      // get an item
      CollectionFuture<Map<Long, Element<Object>>> f = mc.asyncBopGet(
              KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER, false, false);
      Assert.assertNull(f.get());
      Assert.assertEquals("UNREADABLE", f.getOperationStatus()
              .getMessage());

      // set readable
      attribute.setReadable(true);
      Assert.assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get an item again
      f = mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
              false, false);
      Map<Long, Element<Object>> map = f.get();

      Assert.assertNotNull(map);
      Assert.assertEquals(VALUE, map.get(BKEY).getValue());
      Assert.assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testCreateReadableBTreeTest() {
    try {
      // create readable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(true);

      Boolean insertResult = mc.asyncBopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assert.assertEquals(new Long(0), attr.getCount());
      Assert.assertEquals(new Long(4000), attr.getMaxCount());
      Assert.assertEquals(new Integer(0), attr.getExpireTime());
      Assert.assertTrue(attr.getReadable());

      // insert an item
      Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE, null)
              .get());

      // get an item
      CollectionFuture<Map<Long, Element<Object>>> f = mc.asyncBopGet(
              KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER, false, false);

      Map<Long, Element<Object>> map = f.get();
      Assert.assertNotNull(map);
      Assert.assertEquals(VALUE, map.get(BKEY).getValue());
      Assert.assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

}
