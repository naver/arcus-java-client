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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LopInsertBulkTest extends BaseIntegrationTest {

  @Test
  public void testInsertAndGet() {
    String value = "MyValue";
    int keySize = 500;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyLopKey" + i;
    }

    try {
      // REMOVE
      for (String key : keys) {
        mc.asyncLopDelete(key, 0, 4000, true).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncLopInsertBulk(Arrays.asList(keys), 0, value,
                      new CollectionAttributes());
      try {
        Map<String, CollectionOperationStatus> errorList = future.get(
                100L, TimeUnit.MILLISECONDS);
        assertTrue(errorList.isEmpty(),
                "Error list is not empty.");
      } catch (TimeoutException e) {
        future.cancel(true);
        fail(e.getMessage());
      }

      // GET
      int errorCount = 0;
      for (String key : keys) {
        Future<List<Object>> f = mc.asyncLopGet(key, 0, false, false);
        List<Object> cachedList = null;
        try {
          cachedList = f.get();
        } catch (Exception e) {
          f.cancel(true);
          fail(e.getMessage());
        }
        Object value2 = cachedList.get(0);
        if (!value.equals(value2)) {
          errorCount++;
        }
      }
      assertEquals(0, errorCount, "Error count is greater than 0.");

      // REMOVE
      for (String key : keys) {
        mc.asyncLopDelete(key, 0, 4000, true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCountError() {
    String value = "MyValue";

    int keySize = 1200;

    String[] keys = new String[keySize];

    try {
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "MyLopKey" + i;
        mc.delete(keys[i]).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncLopInsertBulk(Arrays.asList(keys), 0, value, null);

      Map<String, CollectionOperationStatus> map = future.get(1000L,
              TimeUnit.MILLISECONDS);
      assertEquals(keySize, map.size());

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
