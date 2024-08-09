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

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BopInsertBulkMultipleTest extends BaseIntegrationTest {

  @Test
  public void testInsertAndGet() {
    String key = "MyBopKey32";
    String value = "MyValue";

    int bkeySize = 500;
    Map<Long, Object> bkeys = new TreeMap<>();
    for (int i = 0; i < bkeySize; i++) {
      bkeys.put((long) i, value);
    }

    try {
      // REMOVE
      mc.asyncBopDelete(key, 0, 4000, ElementFlagFilter.DO_NOT_FILTER, 0,
              true).get();

      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncBopPipedInsertBulk(key, bkeys,
                      new CollectionAttributes());
      try {
        Map<Integer, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(errorList.isEmpty(),
                "Error list is not empty.");
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
      }

      // GET
      int errorCount = 0;
      for (Entry<Long, Object> entry : bkeys.entrySet()) {
        Future<Map<Long, Element<Object>>> f = mc.asyncBopGet(key,
                entry.getKey(), ElementFlagFilter.DO_NOT_FILTER, false,
                false);
        Map<Long, Element<Object>> map = null;
        try {
          map = f.get();
        } catch (Exception e) {
          f.cancel(true);
          e.printStackTrace();
        }
        Object value2 = map.entrySet().iterator().next().getValue()
                .getValue();
        if (!value.equals(value2)) {
          errorCount++;
        }
      }

      Assertions.assertEquals(0, errorCount, "Error count is greater than 0.");

      // REMOVE
      for (Entry<Long, Object> entry : bkeys.entrySet()) {
        mc.asyncBopDelete(key, entry.getKey(),
                ElementFlagFilter.DO_NOT_FILTER, true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }

  @Test
  public void testErrorCount() {
    String key = "MyBopKeyErrorCount";
    String value = "MyValue";

    int bkeySize = 1200;
    Map<Long, Object> bkeys = new TreeMap<>();
    for (int i = 0; i < bkeySize; i++) {
      bkeys.put((long) i, value);
    }

    try {
      System.out.println(11);
      mc.delete(key).get();

      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncBopPipedInsertBulk(key, bkeys, null);

      Map<Integer, CollectionOperationStatus> map = future.get(2000L,
              TimeUnit.MILLISECONDS);
      Assertions.assertEquals(bkeySize, map.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }
}
