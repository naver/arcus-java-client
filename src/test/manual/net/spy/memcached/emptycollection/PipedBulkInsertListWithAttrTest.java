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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PipedBulkInsertListWithAttrTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final int INDEX = 0;
  private final int EXPIRE_TIME_IN_SEC = 1;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assertions.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testInsertWithAttribute() {
    try {
      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();
      attr.setExpireTime(EXPIRE_TIME_IN_SEC);
      attr.setMaxCount(3333);

      List<Object> valueList = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncLopPipedInsertBulk(KEY, INDEX, valueList, attr).get();
      Assertions.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertEquals(Long.valueOf(3333),
              collectionAttributes.getMaxCount());
      Assertions.assertEquals(Long.valueOf(10), collectionAttributes.getCount());

      // check values
      List<Object> list2 = mc.asyncLopGet(KEY, 0, 10, false, false).get();
      for (int i = 0; i < list2.size(); i++) {
        Assertions.assertEquals(i +  1, list2.get(i));
      }

      // check expire time
      Thread.sleep(EXPIRE_TIME_IN_SEC * 1000L + 1000L);
      List<Object> list = mc.asyncLopGet(KEY, 0, false, false).get();
      Assertions.assertNull(list);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testInsertWithDefaultAttribute() {
    try {
      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();

      List<Object> valueList = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncLopPipedInsertBulk(KEY, INDEX, valueList, attr).get();
      Assertions.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertEquals(Long.valueOf(4000),
              collectionAttributes.getMaxCount());
      Assertions.assertEquals(Long.valueOf(10), collectionAttributes.getCount());

      // check values
      List<Object> list2 = mc.asyncLopGet(KEY, 0, 10, false, false).get();
      for (int i = 0; i < list2.size(); i++) {
        Assertions.assertEquals(i +  1, list2.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testInsertWithoutAttributeCreate() {
    try {
      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      List<Object> valueList = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncLopPipedInsertBulk(KEY, INDEX, valueList,
                      new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertEquals(Long.valueOf(4000),
              collectionAttributes.getMaxCount());
      Assertions.assertEquals(Long.valueOf(10), collectionAttributes.getCount());

      // check values
      List<Object> list2 = mc.asyncLopGet(KEY, 0, 10, false, false).get();
      for (int i = 0; i < list2.size(); i++) {
        Assertions.assertEquals(i +  1, list2.get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testInsertWithoutAttributeDoNotCreate() {
    try {
      // check not exists
      Assertions.assertNull(mc.asyncGetAttr(KEY).get());

      List<Object> valueList = new ArrayList<>();
      for (int i = 1; i < 11; i++) {
        valueList.add(i);
      }

      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncLopPipedInsertBulk(KEY, INDEX, valueList, null).get();
      Assertions.assertEquals(10, insertResult.size());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertNull(collectionAttributes);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }
}
