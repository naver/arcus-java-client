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

import junit.framework.Assert;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

public class InsertBTreeWithAttrAndEFlagTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10;
  private final int EXPIRE_TIME_IN_SEC = 1;

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

  public void testInsertWithAttributeAndWithoutFilter() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();
      attr.setExpireTime(EXPIRE_TIME_IN_SEC);
      attr.setMaxCount(3333);

      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, "value", attr).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(new Long(3333),
              collectionAttributes.getMaxCount());

      // check expire time
      Thread.sleep(EXPIRE_TIME_IN_SEC * 2000L);
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNull(map);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithDefaultAttributeAndWithoutFilter() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();

      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              ElementFlagFilter.EMPTY_ELEMENT_FLAG, "value", attr).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(new Long(4000),
              collectionAttributes.getMaxCount());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithAttributeAndFilter() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();
      attr.setExpireTime(EXPIRE_TIME_IN_SEC);
      attr.setMaxCount(3333);

      byte[] filter = "0001".getBytes();

      Boolean insertResult = mc.asyncBopInsert(KEY, BKEY, filter,
              "value", attr).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(new Long(3333),
              collectionAttributes.getMaxCount());

      // check expire time
      Thread.sleep(EXPIRE_TIME_IN_SEC * 2000L);
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNull(map);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithAttributeAndInvalidFilter() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();
      attr.setExpireTime(EXPIRE_TIME_IN_SEC);
      attr.setMaxCount(3333);

      byte[] filter = "1234567890123456789012345678901234567890"
              .getBytes();

      try {
        mc.asyncBopInsert(KEY, BKEY, filter, "value", attr).get();
      } catch (IllegalArgumentException e) {
        return;
      }
      Assert.fail("Something's going wrong.");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.fail("Something's going wrong.");
  }
}
