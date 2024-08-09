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
package net.spy.memcached.emptycollection;

import java.util.HashMap;
import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PipedBulkInsertMapWithAttrTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String MKEY = "10";
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

      Map<String, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(String.valueOf(i), 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncMopPipedInsertBulk(KEY, elements, attr).get();
      Assertions.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertEquals(Long.valueOf(3333),
              collectionAttributes.getMaxCount());

      // check expire time
      Thread.sleep(EXPIRE_TIME_IN_SEC * 1000L + 1000L);
      Map<String, Object> map = mc.asyncMopGet(KEY, MKEY,
              false, false).get();
      Assertions.assertNull(map);
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

      Map<String, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(String.valueOf(i), 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncMopPipedInsertBulk(KEY, elements, attr).get();
      Assertions.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertEquals(Long.valueOf(4000),
              collectionAttributes.getMaxCount());
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

      Map<String, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(String.valueOf(i), 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncMopPipedInsertBulk(KEY, elements,
                      new CollectionAttributes()).get();
      Assertions.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertEquals(Long.valueOf(4000),
              collectionAttributes.getMaxCount());
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

      Map<String, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(String.valueOf(i), 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncMopPipedInsertBulk(KEY, elements, null).get();
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
