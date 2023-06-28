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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.Assert;

public class PipedBulkInsertSetWithAttrTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
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

  public void testInsertWithAttribute() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();
      attr.setExpireTime(EXPIRE_TIME_IN_SEC);
      attr.setMaxCount(3333);

      List<Object> valueList = new ArrayList<Object>();
      for (int i = 1; i < 11; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncSopPipedInsertBulk(KEY, valueList, attr).get();
      Assert.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(Long.valueOf(3333),
              collectionAttributes.getMaxCount());
      Assert.assertEquals(Long.valueOf(10), collectionAttributes.getCount());

      // check values
      Set<Object> set = mc.asyncSopGet(KEY, 0, false, false).get();
      Assert.assertEquals(valueList.size(), set.size());

      // check expire time
      Thread.sleep(EXPIRE_TIME_IN_SEC * 1000L + 1000L);
      List<Object> list = mc.asyncLopGet(KEY, 0, false, false).get();
      Assert.assertNull(list);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithDefaultAttribute() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();

      List<Object> valueList = new ArrayList<Object>();
      for (int i = 1; i < 11; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncSopPipedInsertBulk(KEY, valueList, attr).get();
      Assert.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(Long.valueOf(4000),
              collectionAttributes.getMaxCount());
      Assert.assertEquals(Long.valueOf(10), collectionAttributes.getCount());

      // check values
      Set<Object> set = mc.asyncSopGet(KEY, 0, false, false).get();
      Assert.assertEquals(valueList.size(), set.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithoutAttributeCreate() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      List<Object> valueList = new ArrayList<Object>();
      for (int i = 1; i < 11; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncSopPipedInsertBulk(KEY, valueList,
                      new CollectionAttributes()).get();
      Assert.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(Long.valueOf(4000),
              collectionAttributes.getMaxCount());
      Assert.assertEquals(Long.valueOf(10), collectionAttributes.getCount());

      // check values
      Set<Object> set = mc.asyncSopGet(KEY, 0, false, false).get();
      Assert.assertEquals(valueList.size(), set.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithoutAttributeDoNotCreate() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      List<Object> valueList = new ArrayList<Object>();
      for (int i = 1; i < 11; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncSopPipedInsertBulk(KEY, valueList, null).get();
      assertEquals(10, insertResult.size());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      assertNull(collectionAttributes);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
