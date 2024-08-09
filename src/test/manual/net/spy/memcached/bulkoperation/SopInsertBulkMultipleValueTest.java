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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SopInsertBulkMultipleValueTest extends BaseIntegrationTest {

  @Test
  public void testInsertAndGet() {
    String key = "testInsertAndGet";
    String prefix = "MyValue";

    int valueCount = mc.getMaxPipedItemCount();
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = String.format("%s%d", prefix, i);
    }

    try {
      // REMOVE
      for (Object v : valueList) {
        mc.asyncSopDelete(key, v, true).get();
      }

      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncSopPipedInsertBulk(key, Arrays.asList(valueList),
                      new CollectionAttributes());
      try {
        Map<Integer, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);
        Assertions.assertTrue(errorList.isEmpty());
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
        Assertions.fail(e.getMessage());
      }

      // GET
      int errorCount = 0;
      Future<Set<Object>> f = mc.asyncSopGet(key, valueCount, false,
              false);
      Set<Object> list = null;
      try {
        list = f.get(10000L, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        f.cancel(true);
        e.printStackTrace();
        Assertions.fail(e.getMessage());
      }

      Assertions.assertNotNull(list, "Cached list is null.");
      Assertions.assertTrue(!list.isEmpty(), "Cached list is empty.");

      for (Object o : list) {
        if (!((String) o).startsWith(prefix)) {
          errorCount++;
        }
      }

      Assertions.assertEquals(valueCount, list.size());
      Assertions.assertEquals(0, errorCount);

      // REMOVE
      for (Object v : valueList) {
        mc.asyncSopDelete(key, v, true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }

  @Test
  public void testErrorCount() {
    String key = "testErrorCount";
    int valueCount = 1200;
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "MyValue" + i;
    }

    try {
      mc.delete(key).get();

      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncSopPipedInsertBulk(key, Arrays.asList(valueList),
                      null);

      Map<Integer, CollectionOperationStatus> map = future.get(2000L,
              TimeUnit.MILLISECONDS);
      Assertions.assertEquals(valueCount, map.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }
}
