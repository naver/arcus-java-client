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
package net.spy.memcached.collection.set;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

public class SopBulkAPITest extends BaseIntegrationTest {

  private final String key = "SopBulkAPITest";
  private final List<Object> valueList = new ArrayList<Object>();

  private int getValueCount() {
    return mc.getMaxPipedItemCount();
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    for (long i = 0; i < getValueCount(); i++) {
      valueList.add("value" + String.valueOf(i));
    }
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
              .asyncSopPipedInsertBulk(key, valueList,
                      new CollectionAttributes());

      Map<Integer, CollectionOperationStatus> map = future.get(10000,
              TimeUnit.MILLISECONDS);

      Set<Object> set = mc
              .asyncSopGet(key, getValueCount(), false, false).get();

      assertEquals(getValueCount(), set.size());
      assertEquals(0, map.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBulkFailed() {
    try {
      for (Object v : valueList) {
        mc.asyncSopDelete(key, v, false).get();
      }

      mc.asyncSopInsert(key, "value1", new CollectionAttributes()).get();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncSopPipedInsertBulk(key, valueList,
                      new CollectionAttributes());

      Map<Integer, CollectionOperationStatus> map = future.get(10000,
              TimeUnit.MILLISECONDS);

      assertEquals(1, map.size());
      assertFalse(future.getOperationStatus().isSuccess());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBulkEmptySet() {
    try {
      for (Object v : valueList) {
        mc.asyncSopDelete(key, v, false).get();
      }

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncSopPipedInsertBulk(key, new ArrayList<Object>(),
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
}
