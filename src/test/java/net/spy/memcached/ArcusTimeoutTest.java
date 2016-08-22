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

import net.spy.memcached.collection.*;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

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

@RunWith(JUnit4ClassRunner.class)
public class ArcusTimeoutTest extends BaseIntegrationTest {
  private final String KEY = this.getClass().getSimpleName();

  @Before
  @Override
  public void setUp() throws Exception {
    Assume.assumeTrue(!USE_ZK);
    initClient();
  }

  protected void initClient() throws IOException {
    mc = new ArcusClient(new DefaultConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 1;
      }

      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Retry;
      }
    }, AddrUtil.getAddresses("127.0.0.1:64213"));
  }

  @After
  @Override
  public void tearDown() throws Exception {
    // override teardown to avoid the flush phase
    if(mc != null) {
      mc.shutdown();
    }
  }

  @Test(expected = TimeoutException.class)
  public void testByteArrayBKeyOldSMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    ArrayList<String> keyList;
    keyList = new ArrayList<String>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    byte[] from = new byte[] { (byte) 0 };
    byte[] to = new byte[] { (byte) 1000 };

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, from, to, ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testByteArrayBKeySMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    List<String> keyList = new ArrayList<String>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    byte[] from = new byte[] { (byte) 0 };
    byte[] to = new byte[] { (byte) 1000 };

    SMGetMode smgetMode = SMGetMode.UNIQUE;

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, from, to, ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testOldSMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    List<String> keyList = new ArrayList<String>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, 0, 1000, ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testSMGetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    List<String> keyList = new ArrayList<String>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(KEY + i);
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;

    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            keyList, 0, 1000, ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testBopInsertBulkMultipleTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "MyBopKey";
    String value = "MyValue";

    int bkeySize = mc.getMaxPipedItemCount();
    Map<Long, Object> bkeys = new TreeMap<Long, Object>();
    for (int i = 0; i < bkeySize; i++) {
      bkeys.put((long) i, value);
    }

    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncBopPipedInsertBulk(
            key, bkeys, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testBopPipedInsertBulkTimeoutUsingSingleClient()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "MyBopKey";
    String value = "MyValue";

    int bkeySize = mc.getMaxPipedItemCount();
    Map<Long, Object> bkeys = new TreeMap<Long, Object>();
    for (int i = 0; i < bkeySize; i++) {
      bkeys.put((long) i, value);
    }

    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncBopPipedInsertBulk(
            key, bkeys, new CollectionAttributes());
    future.get(1L, TimeUnit.NANOSECONDS);
    future.cancel(true);
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
            Arrays.asList(keys), bkey, new byte[]{0,0,1,1}, value, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testBulkSetTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    // recreate arcus client with BulkServiceThreadCount 6
    try {
      tearDown();
      mc = new ArcusClient(new DefaultConnectionFactory() {
        @Override
        public int getBulkServiceThreadCount() {
          return 6;
        }
      }, AddrUtil.getAddresses("127.0.0.1:64213"));
    } catch (Exception e) {
      fail(e.getMessage());
    }

    int keySize = 100000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyKey" + i;
    }

    String value = "MyValue";
    Future<Map<String, CollectionOperationStatus>> future = mc.asyncSetBulk(
            Arrays.asList(keys), 60, value);
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testBulkSetTimeoutUsingSingleClient()
          throws InterruptedException, ExecutionException, TimeoutException {
    // no need to recreate client, default BulkServiceThreadCount is 1
    int keySize = 100000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyKey" + i;
    }

    String value = "MyValue";

    Future<Map<String, CollectionOperationStatus>> future = mc.asyncSetBulk(
            Arrays.asList(keys), 60, value);
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testLopInsertBulkMultipleValueTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    int valueCount = 500;
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "MyValue";
    }

    // SET
    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncLopPipedInsertBulk(
            KEY, 0, Arrays.asList(valueList), new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
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
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testSopInsertBulkMultipleValueTimeout()
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
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testSopPipedInsertBulkTimeoutUsingSingleClient()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "testTimeoutUsingSingleClient";
    int valueCount = mc.getMaxPipedItemCount();
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "MyValue" + i;
    }

    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncSopPipedInsertBulk(
            key, Arrays.asList(valueList), new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
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
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testCollectionFutureTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    CollectionFuture<Boolean> future;

    future = mc.asyncBopInsert(KEY, 0, null, "hello", new CollectionAttributes());
    future.get(1, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testFlushByPrefixTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    OperationFuture<Boolean> flushFuture = mc.flush("prefix");
    flushFuture.get(1L, TimeUnit.MILLISECONDS);
    flushFuture.cancel(true);
  }

  @Test(expected = TimeoutException.class)
  public void testMopInsertBulkMultipleTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "MyMopKey";
    String value = "MyValue";

    int mkeySize = mc.getMaxPipedItemCount();
    Map<String, Object> mkeys = new TreeMap<String, Object>();
    for (int i = 0; i < mkeySize; i++) {
      mkeys.put(String.valueOf(i), value);
    }

    Future<Map<Integer, CollectionOperationStatus>> future = mc.asyncMopPipedInsertBulk(
            key, mkeys, new CollectionAttributes());
    future.get(1L, TimeUnit.NANOSECONDS);
    future.cancel(true);
  }

  public void testMopInsertBulkTimeout()
          throws InterruptedException, ExecutionException, TimeoutException {
    String key = "MyMopKey";
    String value = "MyValue";
    int keySize = 250000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyMopKey" + i;
    }

    Future<Map<String, CollectionOperationStatus>> future = mc.asyncMopInsertBulk(
            Arrays.asList(keys), key, value, new CollectionAttributes());
    future.get(1L, TimeUnit.MILLISECONDS);
    future.cancel(true);
  }
}
