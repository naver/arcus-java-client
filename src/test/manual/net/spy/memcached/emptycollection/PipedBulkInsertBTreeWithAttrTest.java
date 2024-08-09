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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PipedBulkInsertBTreeWithAttrTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10;
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

      Map<Long, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements, attr).get();
      Assertions.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assertions.assertEquals(Long.valueOf(3333),
              collectionAttributes.getMaxCount());

      // check expire time
      Thread.sleep(EXPIRE_TIME_IN_SEC * 1000L + 1000L);
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
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

      Map<Long, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements, attr).get();
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

      Map<Long, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements,
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

      Map<Long, Object> elements = new HashMap<>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements, null).get();
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

  @Test
  public void testInsertWithEflag() {
    try {
      byte[] eflag = new byte[]{0, 1, 0, 1};
      List<Element<Object>> elements = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        elements.add(new Element<>(new byte[]{(byte) i},
                "VALUE" + i, eflag));
      }

      Map<Integer, CollectionOperationStatus> map = mc
              .asyncBopPipedInsertBulk(KEY, elements,
                      new CollectionAttributes()).get();

      Assertions.assertTrue(map.isEmpty());

      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, eflag);

      Map<ByteArrayBKey, Element<Object>> map2 = mc.asyncBopGet(KEY,
              new byte[]{(byte) 0}, new byte[]{(byte) 9}, filter,
              0, 100, false, false).get();

      Assertions.assertEquals(10, map2.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testInsertWithEflagLongBkey() {
    try {
      byte[] eflag = new byte[]{0, 1, 0, 1};

      List<Element<Object>> elements = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        elements.add(new Element<>(i, "VALUE" + i, eflag));
      }

      Map<Integer, CollectionOperationStatus> map = mc
              .asyncBopPipedInsertBulk(KEY, elements,
                      new CollectionAttributes()).get();

      Assertions.assertTrue(map.isEmpty());

      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, eflag);

      Map<Long, Element<Object>> map2 = mc.asyncBopGet(KEY, 0, 10,
              filter, 0, 100, false, false).get();

      Assertions.assertEquals(10, map2.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }
}
