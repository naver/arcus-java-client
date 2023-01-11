/*
 * arcus-java-client : Arcus Java client
 * Copyright 2016 JaM2in Co., Ltd.
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
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.internal.CollectionFuture;

public class UnReadableMapTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String MKEY = "MKEY";
  private final String VALUE = "VALUE";

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

  public void testCreateUnreadableMapTest() {
    try {
      // create unreadable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(false);

      Boolean insertResult = mc.asyncMopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assert.assertEquals(Long.valueOf(0), attr.getCount());
      Assert.assertEquals(Long.valueOf(4000), attr.getMaxCount());
      Assert.assertEquals(Integer.valueOf(0), attr.getExpireTime());
      Assert.assertFalse(attr.getReadable());

      // insert an item
      Assert.assertTrue(mc.asyncMopInsert(KEY, MKEY, VALUE, null)
              .get());

      // get an item
      CollectionFuture<Map<String, Object>> f = mc.asyncMopGet(
              KEY, MKEY, false, false);
      Assert.assertNull(f.get());
      Assert.assertEquals("UNREADABLE", f.getOperationStatus()
              .getMessage());

      // set readable
      attribute.setReadable(true);
      Assert.assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get an item again
      f = mc.asyncMopGet(KEY, MKEY, false, false);
      Map<String, Object> map = f.get();

      Assert.assertNotNull(map);
      Assert.assertEquals(VALUE, map.get(MKEY));
      Assert.assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testCreateReadableMapTest() {
    try {
      // create readable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(true);

      Boolean insertResult = mc.asyncMopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assert.assertEquals(Long.valueOf(0), attr.getCount());
      Assert.assertEquals(Long.valueOf(4000), attr.getMaxCount());
      Assert.assertEquals(Integer.valueOf(0), attr.getExpireTime());
      Assert.assertTrue(attr.getReadable());

      // insert an item
      Assert.assertTrue(mc.asyncMopInsert(KEY, MKEY, VALUE, null)
              .get());

      // get an item
      CollectionFuture<Map<String, Object>> f = mc.asyncMopGet(
              KEY, MKEY, false, false);

      Map<String, Object> map = f.get();
      Assert.assertNotNull(map);
      Assert.assertEquals(VALUE, map.get(MKEY));
      Assert.assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

}
