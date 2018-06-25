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
package net.spy.memcached.bulkoperation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

public class LopInsertBulkMultipleValueTest extends BaseIntegrationTest {

  private String key = "LopInsertBulkMultipleValueTest";

  @Override
  protected void tearDown() {
    try {
      mc.delete(key).get();
      super.tearDown();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInsertAndGet() {
    String value = "MyValue";

    int valueCount = 500;
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "MyValue";
    }

    try {
      // REMOVE
      mc.asyncLopDelete(key, 0, 4000, true).get();

      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncLopPipedInsertBulk(key, 0, Arrays.asList(valueList),
                      new CollectionAttributes());
      try {
        Map<Integer, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);
        Assert.assertTrue(errorList.isEmpty());
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }

      // GET
      int errorCount = 0;
      List<Object> list = null;
      Future<List<Object>> f = mc.asyncLopGet(key, 0, valueCount, false,
              false);
      try {
        list = f.get();
      } catch (Exception e) {
        f.cancel(true);
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }

      Assert.assertNotNull("List is null.", list);
      Assert.assertTrue("Cached list is empty.", !list.isEmpty());
      Assert.assertEquals(valueCount, list.size());

      for (Object o : list) {
        if (!value.equals(o)) {
          errorCount++;
        }
      }
      Assert.assertEquals(valueCount, list.size());
      Assert.assertEquals(0, errorCount);

      // REMOVE
      mc.asyncLopDelete(key, 0, 4000, true).get();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testErrorCount() {
    int valueCount = 1200;
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "MyValue";
    }

    try {
      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncLopPipedInsertBulk(key, 0, Arrays.asList(valueList),
                      null);

      Map<Integer, CollectionOperationStatus> map = future.get(1000L,
              TimeUnit.MILLISECONDS);
      assertEquals(valueCount, map.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
