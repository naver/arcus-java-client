/*
 * arcus-java-client : Arcus Java client
 * Copyright 2020 JaM2in Co., Ltd.
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

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.CollectionOperationStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BulkDeleteTest extends BaseIntegrationTest {
  public void testNullAndEmptyKeyDelete() {
    // DELETE null key
    try {
      List<String> keys = null;
      mc.asyncDeleteBulk(keys);
    } catch (Exception e) {
      assertEquals("Key list is null.", e.getMessage());
    }
    try {
      String[] keys = null;
      mc.asyncDeleteBulk(keys);
    } catch (Exception e) {
      assertEquals("Key list is null.", e.getMessage());
    }
    // DELETE empty key
    try {
      List<String> keys = new ArrayList<String>();
      mc.asyncDeleteBulk(keys);
    } catch (Exception e) {
      assertEquals("Key list is empty.", e.getMessage());
    }
  }

  public void testInsertAndDelete() {
    String value = "MyValue";

    int TEST_COUNT = 64;

    try {
      for (int keySize = 1; keySize < TEST_COUNT; keySize++) {
        // generate key
        String[] keys = new String[keySize];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = "MyKey" + i;
        }

        // SET
        for (String key : keys) {
          mc.set(key, 60, value);
        }

        // Bulk delete
        Future<Map<String, CollectionOperationStatus>> future = mc.
                asyncDeleteBulk(Arrays.asList(keys));

        Map<String, CollectionOperationStatus> errorList;
        try {
          errorList = future.get(20000L, TimeUnit.MILLISECONDS);
          Assert.assertTrue("Error list is not empty.",
                  errorList.isEmpty());
        } catch (TimeoutException e) {
          future.cancel(true);
          e.printStackTrace();
        }

        // GET
        int errorCount = 0;
        for (String key : keys) {
          String v = (String) mc.get(key);
          if (v != null) {
            errorCount++;
          }
        }

        Assert.assertEquals("Error count is greater than 0.", 0,
                errorCount);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testDeleteNotFoundKey() {
    int TEST_COUNT = 64;

    try {
      for (int keySize = 1; keySize < TEST_COUNT; keySize++) {
        // generate key
        String[] keys = new String[keySize];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = "MyKey" + i;
        }

        for (int i = 0; i < keys.length; i++) {
          if (i % 2 == 0) {
            mc.set(keys[i], 60, "value");
          } else {
            mc.delete(keys[i]);
          }
        }

        // Bulk delete
        Future<Map<String, CollectionOperationStatus>> future = mc.
                asyncDeleteBulk(Arrays.asList(keys));

        Map<String, CollectionOperationStatus> errorList;
        try {
          errorList = future.get(20000L, TimeUnit.MILLISECONDS);
          Assert.assertEquals("Error count is less than input.",
                  keys.length/2, errorList.size());
          for (Map.Entry<String, CollectionOperationStatus> error :
                  errorList.entrySet()) {
            Assert.assertEquals(error.getValue().getResponse(),
                    CollectionResponse.NOT_FOUND);
          }
        } catch (TimeoutException e) {
          future.cancel(true);
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
