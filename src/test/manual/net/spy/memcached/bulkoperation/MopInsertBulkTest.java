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
package net.spy.memcached.bulkoperation;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.ops.CollectionOperationStatus;

public class MopInsertBulkTest extends BaseIntegrationTest {

  private final String MKEY = "mkey";

  public void testInsertAndGet() {
    String value = "MyValue";
    int keySize = 500;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyMopKey" + i;
    }

    try {
      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncMopInsertBulk(Arrays.asList(keys), MKEY, value,
                      new CollectionAttributes());
      try {
        Map<String, CollectionOperationStatus> errorList = future.get(
                100L, TimeUnit.MILLISECONDS);
        Assert.assertTrue("Error list is not empty.",
                errorList.isEmpty());
      } catch (TimeoutException e) {
        future.cancel(true);
        Assert.fail(e.getMessage());
      }

      // GET
      int errorCount = 0;
      for (String key : keys) {
        Future<Map<String, Object>> f = mc.asyncMopGet(key, MKEY, false, false);
        Map<String, Object> cachedMap = null;
        try {
          cachedMap = f.get();
        } catch (Exception e) {
          f.cancel(true);
          Assert.fail(e.getMessage());
        }
        Object value2 = cachedMap.get(MKEY);
        if (!value.equals(value2)) {
          errorCount++;
        }
      }
      Assert.assertEquals("Error count is greater than 0.", 0, errorCount);

      // REMOVE
      for (String key : keys) {
        mc.asyncMopDelete(key, true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testCountError() {
    String value = "MyValue";

    int keySize = 1200;

    String[] keys = new String[keySize];

    try {
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "MyMopKey" + i;
        mc.delete(keys[i]).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncMopInsertBulk(Arrays.asList(keys), MKEY, value, null);

      Map<String, CollectionOperationStatus> map = future.get(1000L,
              TimeUnit.MILLISECONDS);
      assertEquals(keySize, map.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

}
