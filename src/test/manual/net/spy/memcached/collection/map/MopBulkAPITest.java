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
package net.spy.memcached.collection.map;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MopBulkAPITest extends BaseIntegrationTest {

  private String key = "MopBulkAPITest33";
  Map<String, Object> elements = new HashMap<String, Object>();
  Map<String, Object> updateMap = new HashMap<String, Object>();


  private int getValueCount() {
    return mc.getMaxPipedItemCount();
  }

  protected void setUp() throws Exception {
    super.setUp();
    for (long i = 0; i < getValueCount(); i++) {
      elements.put("mkey" + String.valueOf(i),
              "value" + String.valueOf(i));
      updateMap.put("mkey" + String.valueOf(i),
              "newvalue" + String.valueOf(i));
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testBulk() throws Exception {
    for (int i = 0; i < 10; i++) {
      mc.delete(key).get();
      bulk();
    }
  }

  public void bulk() {
    try {
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncMopPipedInsertBulk(key, elements,
                      new CollectionAttributes());

      Map<Integer, CollectionOperationStatus> map = future.get(10000,
              TimeUnit.MILLISECONDS);

      Map<String, Object> rmap = mc.asyncMopGet(key, false,
              false).get();
      assertEquals(getValueCount(), rmap.size());
      assertEquals(0, map.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBulkFailed() {
    try {
      mc.asyncMopDelete(key, true).get(1000,
              TimeUnit.MILLISECONDS);

      mc.asyncMopInsert(key, "mkey1", "value1", new CollectionAttributes())
              .get();

      mc.asyncSetAttr(key, new CollectionAttributes(0, 1L, CollectionOverflowAction.error)).get();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncMopPipedInsertBulk(key, elements,
                      new CollectionAttributes());

      Map<Integer, CollectionOperationStatus> map = future.get(10000,
              TimeUnit.MILLISECONDS);

      assertEquals(getValueCount(), map.size());
      assertFalse(future.getOperationStatus().isSuccess());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBulkEmptyElements() {
    try {
      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncMopPipedInsertBulk(key, new HashMap<String, Object>(),
                      new CollectionAttributes());

      future.get(10000, TimeUnit.MILLISECONDS);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      return;
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    Assert.fail();
  }

  public void testUpdateBulk() {
    try {
      mc.asyncMopDelete(key, true).get(1000,
              TimeUnit.MILLISECONDS);

      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncMopPipedInsertBulk(key, elements,
                      new CollectionAttributes());

      Map<Integer, CollectionOperationStatus> map = future.get(10000,
              TimeUnit.MILLISECONDS);

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 = mc
              .asyncMopPipedUpdateBulk(key, updateMap);

      Map<Integer, CollectionOperationStatus> map2 = future2.get(10000,
              TimeUnit.MILLISECONDS);

      Map<String, Object> rmap = mc.asyncMopGet(key, false, false).get();
      assertEquals(getValueCount(), rmap.size());
      assertEquals(0, map.size());
      assertEquals(0, map2.size());

      assertEquals("newvalue1", rmap.get("mkey1"));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
