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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class BopInsertBulkTest extends BaseIntegrationTest {

  private static final byte[] EFLAG = new byte[]{0, 0, 1, 1};

  @Test
  public void testInsertAndGet() {
    String value = "MyValue";
    long bkey = Long.MAX_VALUE;

    int keySize = 1000;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyBopKeyA" + i;
    }

    try {
      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncBopInsertBulk(Arrays.asList(keys), bkey, null, value,
                      new CollectionAttributes());
      try {
        Map<String, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(errorList.isEmpty(),
                "Error list is not empty.");
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
      }

      // GET
      int errorCount = 0;
      for (String key : keys) {
        Future<Map<Long, Element<Object>>> f = mc.asyncBopGet(key,
                bkey, ElementFlagFilter.DO_NOT_FILTER, false, false);
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
      assertEquals(0, errorCount, "Error count is greater than 0.");

      // REMOVE
      for (String key : keys) {
        mc.asyncBopDelete(key, bkey, ElementFlagFilter.DO_NOT_FILTER,
                true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testInsertAndGetByteArrayBkey() {
    String value = "MyValue";
    byte[] bkey = new byte[]{0, 1, 1, 1};

    int keySize = 1000;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyBopKeyA" + i;
    }

    try {
      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncBopInsertBulk(Arrays.asList(keys), bkey, null, value,
                      new CollectionAttributes());
      try {
        Map<String, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(errorList.isEmpty(),
                "Error list is not empty.");
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
      }

      // GET
      int errorCount = 0;
      for (String key : keys) {
        Future<Map<ByteArrayBKey, Element<Object>>> f = mc.asyncBopGet(
                key, bkey, ElementFlagFilter.DO_NOT_FILTER, false,
                false);
        Map<ByteArrayBKey, Element<Object>> map = null;
        try {
          map = f.get();
        } catch (Exception e) {
          f.cancel(true);
          e.printStackTrace();
        }
        Element<Object> value2 = map.entrySet().iterator().next()
                .getValue();
        if (!value.equals(value2.getValue())) {
          errorCount++;
        }
      }
      assertEquals(0, errorCount, "Error count is greater than 0.");

      // REMOVE
      for (String key : keys) {
        mc.asyncBopDelete(key, bkey, ElementFlagFilter.DO_NOT_FILTER,
                true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testInsertAndGetWithEflag() {
    String value = "MyValue";
    long bkey = Long.MAX_VALUE;

    int keySize = 1000;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyBopKeyA" + i;
    }

    try {
      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncBopInsertBulk(Arrays.asList(keys), bkey, EFLAG,
                      value, new CollectionAttributes());
      try {
        Map<String, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(errorList.isEmpty(),
                "Error list is not empty.");
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
      }

      // GET
      int errorCount = 0;
      for (String key : keys) {
        Future<Map<Long, Element<Object>>> f = mc.asyncBopGet(key,
                bkey, ElementFlagFilter.DO_NOT_FILTER, false, false);
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
      assertEquals(0, errorCount, "Error count is greater than 0." );

      // REMOVE
      for (String key : keys) {
        mc.asyncBopDelete(key, bkey, ElementFlagFilter.DO_NOT_FILTER,
                true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testInsertAndGetByteArrayBkeyWithEflag() {
    String value = "MyValue";
    byte[] bkey = new byte[]{0, 1, 1, 1};

    int keySize = 1000;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyBopKeyA" + i;
    }

    try {
      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncBopInsertBulk(Arrays.asList(keys), bkey, EFLAG,
                      value, new CollectionAttributes());
      try {
        Map<String, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(errorList.isEmpty(),
                "Error list is not empty.");
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
      }

      // GET
      int errorCount = 0;
      for (String key : keys) {
        Future<Map<ByteArrayBKey, Element<Object>>> f = mc.asyncBopGet(
                key, bkey, ElementFlagFilter.DO_NOT_FILTER, false,
                false);
        Map<ByteArrayBKey, Element<Object>> map = null;
        try {
          map = f.get();
        } catch (Exception e) {
          f.cancel(true);
          e.printStackTrace();
        }
        Element<Object> value2 = map.entrySet().iterator().next()
                .getValue();
        if (!value.equals(value2.getValue())) {
          errorCount++;
        }
      }
      assertEquals(0, errorCount, "Error count is greater than 0.");

      // REMOVE
      for (String key : keys) {
        mc.asyncBopDelete(key, bkey, ElementFlagFilter.DO_NOT_FILTER,
                true).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testKeyAttributes() {
    String value = "MyValue";
    long bkey = Long.MAX_VALUE;

    int keySize = 50;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyBopKeyA" + i;
    }

    try {
      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      CollectionAttributes keyAttributes = new CollectionAttributes();
      keyAttributes.setExpireTime(10);
      keyAttributes.setMaxCount(200);

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncBopInsertBulk(Arrays.asList(keys), bkey, EFLAG,
                      value, keyAttributes);
      try {
        Map<String, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(errorList.isEmpty(),
                "Error list is not empty.");
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
        fail();
      }

      for (String key : keys) {
        CollectionAttributes attrs = mc.asyncGetAttr(key).get(500L,
                TimeUnit.MILLISECONDS);
        assertEquals(keyAttributes.getMaxCount(), attrs.getMaxCount());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testErrorCount() {
    String value = "MyValue";
    long bkey = Long.MAX_VALUE;

    int keySize = 1200;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      String key = "MyBopKeyErrorCount" + i;
      keys[i] = key;
    }

    try {
      // DELETE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, CollectionOperationStatus>> future = mc
              .asyncBopInsertBulk(Arrays.asList(keys), bkey, EFLAG,
                      value, null);

      Map<String, CollectionOperationStatus> map = future.get(2000L,
              TimeUnit.MILLISECONDS);

      assertEquals(keySize, map.size());

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
