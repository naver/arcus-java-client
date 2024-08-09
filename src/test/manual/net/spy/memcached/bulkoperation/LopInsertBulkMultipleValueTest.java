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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LopInsertBulkMultipleValueTest extends BaseIntegrationTest {

  private String key = "LopInsertBulkMultipleValueTest";

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(key).get();
    super.tearDown();
  }

  @Test
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
        Assertions.assertTrue(errorList.isEmpty());
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
        Assertions.fail(e.getMessage());
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
        Assertions.fail(e.getMessage());
      }

      Assertions.assertNotNull(list, "List is null.");
      Assertions.assertTrue(!list.isEmpty(), "Cached list is empty.");
      assertEquals(valueCount, list.size());

      for (Object o : list) {
        if (!value.equals(o)) {
          errorCount++;
        }
      }
      assertEquals(valueCount, list.size());
      assertEquals(0, errorCount);

      // REMOVE
      mc.asyncLopDelete(key, 0, 4000, true).get();
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
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
      Assertions.fail();
    }
  }
}
