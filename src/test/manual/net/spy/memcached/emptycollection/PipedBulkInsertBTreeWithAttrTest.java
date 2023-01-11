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

import org.junit.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;
import net.spy.memcached.ops.CollectionOperationStatus;

public class PipedBulkInsertBTreeWithAttrTest extends BaseIntegrationTest {

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

  public void testInsertWithAttribute() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      // insert with create option
      CollectionAttributes attr = new CollectionAttributes();
      attr.setExpireTime(EXPIRE_TIME_IN_SEC);
      attr.setMaxCount(3333);

      Map<Long, Object> elements = new HashMap<Long, Object>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements, attr).get();
      Assert.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(new Long(3333),
              collectionAttributes.getMaxCount());

      // check expire time
      Thread.sleep(EXPIRE_TIME_IN_SEC * 1000L + 1000L);
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      Assert.assertNull(map);
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

      Map<Long, Object> elements = new HashMap<Long, Object>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements, attr).get();
      Assert.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(new Long(4000),
              collectionAttributes.getMaxCount());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithoutAttributeCreate() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      Map<Long, Object> elements = new HashMap<Long, Object>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements,
                      new CollectionAttributes()).get();
      Assert.assertTrue(insertResult.isEmpty());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      Assert.assertEquals(new Long(4000),
              collectionAttributes.getMaxCount());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithoutAttributeDoNotCreate() {
    try {
      // check not exists
      Assert.assertNull(mc.asyncGetAttr(KEY).get());

      Map<Long, Object> elements = new HashMap<Long, Object>();
      for (long i = 1; i < 11; i++) {
        elements.put(i, 1);
      }
      Map<Integer, CollectionOperationStatus> insertResult = mc
              .asyncBopPipedInsertBulk(KEY, elements, null).get();
      Assert.assertEquals(10, insertResult.size());

      // check attribute
      CollectionAttributes collectionAttributes = mc.asyncGetAttr(KEY)
              .get();
      assertNull(collectionAttributes);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithEflag() {
    try {
      byte[] eflag = new byte[]{0, 1, 0, 1};
      List<Element<Object>> elements = new ArrayList<Element<Object>>();
      for (int i = 0; i < 10; i++) {
        elements.add(new Element<Object>(new byte[]{(byte) i},
            "VALUE" + i, eflag));
      }

      Map<Integer, CollectionOperationStatus> map = mc
              .asyncBopPipedInsertBulk(KEY, elements,
                      new CollectionAttributes()).get();

      Assert.assertTrue(map.isEmpty());

      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, eflag);

      Map<ByteArrayBKey, Element<Object>> map2 = mc.asyncBopGet(KEY,
              new byte[]{(byte) 0}, new byte[]{(byte) 9}, filter,
              0, 100, false, false).get();

      Assert.assertEquals(10, map2.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertWithEflagLongBkey() {
    try {
      byte[] eflag = new byte[]{0, 1, 0, 1};

      List<Element<Object>> elements = new ArrayList<Element<Object>>();
      for (int i = 0; i < 10; i++) {
        elements.add(new Element<Object>(i, "VALUE" + i, eflag));
      }

      Map<Integer, CollectionOperationStatus> map = mc
              .asyncBopPipedInsertBulk(KEY, elements,
                      new CollectionAttributes()).get();

      Assert.assertTrue(map.isEmpty());

      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, eflag);

      Map<Long, Element<Object>> map2 = mc.asyncBopGet(KEY, 0, 10,
              filter, 0, 100, false, false).get();

      Assert.assertEquals(10, map2.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
