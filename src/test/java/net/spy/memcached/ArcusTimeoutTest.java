/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2016 JaM2in Corp.
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
package net.spy.memcached;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class ArcusTimeoutTest extends TestCase {
  private ArcusClient mc = null;

  private final String KEY = this.getClass().getSimpleName();

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    initClient();
  }

  private void initClient() throws IOException {
    mc = new ArcusClient(new DefaultConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 1;
      }

      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Retry;
      }
    }, AddrUtil.getAddresses("0.0.0.0:23456"));
  }

  @After
  @Override
  public void tearDown() throws Exception {
    // override teardown to avoid the flush phase
    if (mc != null) {
      mc.shutdown();
    }
    super.tearDown();
  }

  @Test(expected = TimeoutException.class)
  public void testCollectionFutureTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    CollectionFuture<Boolean> future;

    future = mc.asyncBopInsert(KEY, 0, null, "hello", new CollectionAttributes());
    future.get(1, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testBulkSetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    int keySize = 100000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyKey" + i;
    }

    @SuppressWarnings("deprecation")
    Future<Map<String, CollectionOperationStatus>> future = mc.asyncSetBulk(
            Arrays.asList(keys), 60, "MyValue");
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testBulkSetTimeoutUsingSingleThread()
          throws InterruptedException, ExecutionException, TimeoutException {
    int keySize = 100000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyKey" + i;
    }

    @SuppressWarnings("deprecation")
    Future<Map<String, CollectionOperationStatus>> future = mc.asyncSetBulk(
            Arrays.asList(keys), 60, "MyValue");
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testBulkDeleteTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    int keySize = 100000;

    List<String> keys = new ArrayList<>(keySize);
    for (int i = 0; i < keySize; i++) {
      keys.add("MyKey" + i);
    }

    Future<Map<String, OperationStatus>> future = mc.asyncDeleteBulk(keys);
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testBulkDeleteTimeoutUsingSingleThread()
          throws InterruptedException, ExecutionException, TimeoutException {
    int keySize = 100000;

    List<String> keys = new ArrayList<>(keySize);
    for (int i = 0; i < keySize; i++) {
      keys.add("MyKey" + i);
    }

    Future<Map<String, OperationStatus>> future = mc.asyncDeleteBulk(keys);
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testSopPipedInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "testTimeout";
    int valueCount = mc.getMaxPipedItemCount();
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "MyValue" + i;
    }

    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncSopPipedInsertBulk(
            key, Arrays.asList(valueList), new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testSopInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String value = "MyValue";
    int keySize = 100000;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = KEY + i;
    }

    Future<Map<String, CollectionOperationStatus>> future = mc.asyncSopInsertBulk(
            Arrays.asList(keys), value, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testLopPipedInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    int valueCount = 500;
    Object[] valueList = new Object[valueCount];
    Arrays.fill(valueList, "MyValue");

    // SET
    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncLopPipedInsertBulk(
            KEY, 0, Arrays.asList(valueList), new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testLopInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String value = "MyValue";
    int keySize = 250000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyLopKey" + i;
    }

    Future<Map<String, CollectionOperationStatus>> future = mc.asyncLopInsertBulk(
            Arrays.asList(keys), 0, value, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testBopPipedInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "MyBopKey";
    String value = "MyValue";

    int bkeySize = mc.getMaxPipedItemCount();
    Map<Long, Object> bkeys = new TreeMap<>();
    for (int i = 0; i < bkeySize; i++) {
      bkeys.put((long) i, value);
    }

    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncBopPipedInsertBulk(
            key, bkeys, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testBopInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String value = "MyValue";
    long bkey = Long.MAX_VALUE;

    int keySize = 10000;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      String key = "MyBopKey" + i;
      keys[i] = key;
    }

    Future<Map<String, CollectionOperationStatus>> future = mc.asyncBopInsertBulk(
            Arrays.asList(keys), bkey, new byte[]{0, 0, 1, 1}, value, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testOldSMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, 0, 1000, ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testSMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, 0, 1000, ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testByteArrayBKeyOldSMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    ArrayList<String> keyList;
    keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 1000};

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, from, to, ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testByteArrayBKeySMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 1000};

    SMGetMode smgetMode = SMGetMode.UNIQUE;

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, from, to, ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    future.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testFlushByPrefixTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    OperationFuture<Boolean> flushFuture = mc.flush("prefix");
    flushFuture.get(1L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testMopInsertBulkMultipleTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "MyMopKey";
    String value = "MyValue";

    int elementSize = mc.getMaxPipedItemCount();
    Map<String, Object> elements = new TreeMap<>();
    for (int i = 0; i < elementSize; i++) {
      elements.put(String.valueOf(i), value);
    }

    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncMopPipedInsertBulk(
            key, elements, new CollectionAttributes());
    future.get(1L, TimeUnit.NANOSECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testMopInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String mkey = "MyMopKey";
    String value = "MyValue";
    int keySize = 250000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyMopKey" + i;
    }

    Future<Map<String, CollectionOperationStatus>> future = mc.asyncMopInsertBulk(
            Arrays.asList(keys), mkey, value, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }
}
