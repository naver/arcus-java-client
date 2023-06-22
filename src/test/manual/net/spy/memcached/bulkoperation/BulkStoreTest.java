/*
 * arcus-java-client : Arcus Java client
 * Copyright 2020-2021 JaM2in Co., Ltd.
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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.transcoders.WhalinTranscoder;

import org.junit.Assert;

public class BulkStoreTest extends BaseIntegrationTest {

  public void testZeroSizedKeys() {
    try {
      mc.asyncStoreBulk(StoreType.set, new ArrayList<String>(0),
              60, "value");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // should get here.
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }

    try {
      mc.asyncStoreBulk(StoreType.set, new ArrayList<String>(0),
              60, new Object(), new CollectionTranscoder());
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // should get here.
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testSetAndGet2() {
    int KEY_SIZE = 10;

    try {
      // SET null key
      try {
        mc.asyncStoreBulk(StoreType.set, null, 60);
      } catch (Exception e) {
        e.printStackTrace();
        assertEquals("Map is null.", e.getMessage());
      }

      // generate key
      Map<String, Object> o = new HashMap<String, Object>();
      for (int i = 0; i < KEY_SIZE; i++) {
        o.put("MyKey" + i, "MyValue" + i);
      }
      List<String> keys = new ArrayList<String>(o.keySet());

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, OperationStatus>> future = mc
              .asyncStoreBulk(StoreType.set, o, 60);

      Map<String, OperationStatus> errorList;
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
      String k, v;
      for (String key : keys) {
        k = key;
        v = (String) mc.asyncGet(k).get();

        Assert.assertEquals(k + " has unexpected value.", o.get(k), v);

        mc.delete(k).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testSetAndGet() {
    String value = "MyValue";

    int KEY_SIZE = 10;

    try {
      // SET null key
      try {
        mc.asyncStoreBulk(StoreType.set, null, 60, value);
      } catch (Exception e) {
        e.printStackTrace();
        assertEquals("Key list is null.", e.getMessage());
      }

      // generate key
      String[] keys = new String[KEY_SIZE];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "MyKey" + i;
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // SET
      Future<Map<String, OperationStatus>> future = mc
              .asyncStoreBulk(StoreType.set, Arrays.asList(keys), 60, value);

      Map<String, OperationStatus> errorList;
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
      for (String key : keys) {
        String v = (String) mc.get(key);
        Assert.assertEquals(key + " has unexpected value.", value, v);
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }


  public void testAddAndGet() {
    String value = "MyValue";

    int KEY_SIZE = 10;

    try {
      // generate key
      String[] keys = new String[KEY_SIZE];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "MyKey" + i;
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // ADD
      Future<Map<String, OperationStatus>> future = mc
              .asyncStoreBulk(StoreType.add, Arrays.asList(keys), 60, value);

      Map<String, OperationStatus> errorList;
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
      for (String key : keys) {
        String v = (String) mc.get(key);
        Assert.assertEquals(key + " has unexpected value.", value, v);
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }


  public void testReplaceAndGet() {
    String value = "MyValue";

    int KEY_SIZE = 10;

    try {
      // generate key
      String[] keys = new String[KEY_SIZE];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "MyKey" + i;
      }

      // SET
      for (String key : keys) {
        mc.set(key, 60, "oldValue").get();
      }

      // REPLACE
      Future<Map<String, OperationStatus>> future = mc
              .asyncStoreBulk(StoreType.replace, Arrays.asList(keys), 60, value);

      Map<String, OperationStatus> errorList;
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
      for (String key : keys) {
        String v = (String) mc.get(key);
        Assert.assertEquals(key + " has unexpected value.", value, v);
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testStoreWithTranscoder() {
    String value = "MyValue";
    Transcoder<Object> transcoder = new WhalinTranscoder();

    int KEY_SIZE = 10;

    try {
      // generate key
      String[] keys = new String[KEY_SIZE];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "MyKey" + i;
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      //  Store
      Future<Map<String, OperationStatus>> future = mc
          .asyncStoreBulk(StoreType.set, Arrays.asList(keys), 60, value, transcoder);

      Map<String, OperationStatus> errorList;
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
      for (String key : keys) {
        String v = (String) mc.get(key, transcoder);
        Assert.assertEquals(key + " has unexpected value.", value, v);
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testStoreWithTranscoder2() {
    Transcoder<Object> transcoder = new WhalinTranscoder();

    int KEY_SIZE = 10;

    try {
      // generate key
      Map<String, Object> o = new HashMap<String, Object>();
      for (int i = 0; i < KEY_SIZE; i++) {
        o.put("MyKey" + i, "MyValue" + i);
      }

      List<String> keys = new ArrayList<String>(o.keySet());

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // Store
      Future<Map<String, OperationStatus>> future = mc
          .asyncStoreBulk(StoreType.set, o, 60, transcoder);

      Map<String, OperationStatus> errorList;
      try {
        errorList = future.get(20000L, TimeUnit.MILLISECONDS);
        Assert.assertTrue("Error list is not empty.",
            errorList.isEmpty());
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }

      for (String key : keys) {
        String v = (String) mc.get(key, transcoder);
        Assert.assertEquals(key + " has unexpected value.", o.get(key), v);
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testSetWithTranscoder() {
    String value = "MyValue";
    Transcoder<Object> transcoder = new WhalinTranscoder();

    int KEY_SIZE = 10;

    try {
      // generate key
      String[] keys = new String[KEY_SIZE];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "MyKey" + i;
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      // Store
      @SuppressWarnings("deprecation")
      Future<Map<String, CollectionOperationStatus>> future = mc
          .asyncSetBulk(Arrays.asList(keys), 60, value, transcoder);

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
      for (String key : keys) {
        String v = (String) mc.get(key, transcoder);
        Assert.assertEquals(key + " has unexpected value.", value, v);
      }

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  public void testSetWithTranscoder2() {
    Transcoder<Object> transcoder = new WhalinTranscoder();

    int KEY_SIZE = 10;

    try {
      // generate key
      Map<String, Object> o = new HashMap<String, Object>();
      for (int i = 0; i < KEY_SIZE; i++) {
        o.put("MyKey" + i, "MyValue" + i);
      }
      List<String> keys = new ArrayList<String>(o.keySet());

      // REMOVE
      for (String key : keys) {
        mc.delete(key).get();
      }

      //  Store
      @SuppressWarnings("deprecation")
      Future<Map<String, CollectionOperationStatus>> future = mc
          .asyncSetBulk(o, 60, transcoder);

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
      for (String key : o.keySet()) {
        String v = (String) mc.get(key, transcoder);
        Assert.assertEquals(key + " has unexpected value.", o.get(key), v);
      }

      // REMOVE
      for (String key : o.keySet()) {
        mc.delete(key).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
