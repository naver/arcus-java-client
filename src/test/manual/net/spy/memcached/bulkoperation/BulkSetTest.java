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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.transcoders.CollectionTranscoder;

public class BulkSetTest extends BaseIntegrationTest {

  public void testZeroSizedKeys() {
    try {
      mc.asyncSetBulk(new ArrayList<String>(0), 60, "value");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // should get here.
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }

    try {
      mc.asyncSetBulk(new ArrayList<String>(0), 60, new Object(), new CollectionTranscoder());
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // should get here.
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testInsertAndGet2() {
    int TEST_COUNT = 3;

    try {
      // SET null key
      try {
        mc.asyncSetBulk(null, 60);
      } catch (Exception e) {
        assertEquals("Map is null.", e.getMessage());
      }

      for (int keySize = 0; keySize < TEST_COUNT; keySize++) {

        // generate key
        Map<String, Object> o = new HashMap<String, Object>();

        for (int i = 0; i < 600; i++) {
          o.put("MyKey" + i, "MyValue" + i);
        }

        List<String> keys = new ArrayList<String>(o.keySet());

        // REMOVE
        for (String key : keys) {
          mc.delete(key).get();
        }

        // SET
        Future<Map<String, CollectionOperationStatus>> future = mc
                .asyncSetBulk(o, 60);

        Map<String, CollectionOperationStatus> errorList;
        try {
          errorList = future.get(20000L, TimeUnit.MILLISECONDS);
          Assert.assertTrue("Error list is not empty.",
                  errorList.isEmpty());
        } catch (TimeoutException e) {
          future.cancel(true);
          e.printStackTrace();
          Assert.fail(e.getMessage());
        }

        // GET
        int errorCount = 0;
        String k, v;
        for (int i = 0; i < keys.size(); i++) {
          k = keys.get(i);
          v = (String) mc.asyncGet(k).get();

          if (!v.equals(o.get(k))) {
            errorCount++;
          }

          mc.delete(k).get();
        }

        Assert.assertEquals("Error count is greater than 0.", 0,
                errorCount);

      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testInsertAndGet() {
    String value = "MyValue";

    int TEST_COUNT = 64;

    try {
      // SET null key
      try {
        mc.asyncSetBulk(null, 60, value);
      } catch (Exception e) {
        assertEquals("Key list is null.", e.getMessage());
      }

      for (int keySize = 1; keySize < TEST_COUNT; keySize++) {
        // generate key
        String[] keys = new String[keySize];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = "MyKey" + i;
        }

        // REMOVE
        for (String key : keys) {
          mc.delete(key);
        }

        // SET
        Future<Map<String, CollectionOperationStatus>> future = mc
                .asyncSetBulk(Arrays.asList(keys), 60, value);

        Map<String, CollectionOperationStatus> errorList;
        try {
          errorList = future.get(20000L, TimeUnit.MILLISECONDS);
          Assert.assertTrue("Error list is not empty.",
                  errorList.isEmpty());
        } catch (TimeoutException e) {
          future.cancel(true);
          e.printStackTrace();
          Assert.fail(e.getMessage());
        }

        // GET
        int errorCount = 0;
        for (String key : keys) {
          String v = (String) mc.get(key);
          if (!value.equals(v)) {
            errorCount++;
          }
        }

        Assert.assertEquals("Error count is greater than 0.", 0,
                errorCount);

        // REMOVE
        for (String key : keys) {
          mc.delete(key);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
