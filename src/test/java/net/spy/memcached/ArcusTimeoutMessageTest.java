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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatsOperation;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class ArcusTimeoutMessageTest extends TestCase {
  private ArcusClient mc = null;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    initClient();
  }

  private void initClient() throws IOException {
    List<InetSocketAddress> addresses = AddrUtil.getAddresses("0.0.0.0:23456");
    addresses.add(AddrUtil.getAddresses("0.0.0.0:23457").get(0));
    mc = new ArcusClient(new DefaultConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 1;
      }

      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Retry;
      }
    }, addresses);
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


  @Test
  public void getWithoutBulkMessage() {
    String key = "KEY";

    GetFuture<Object> f = mc.asyncGet(key);

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      Assert.assertEquals(APIType.GET.toString(), messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void getBulkWithBulkMessage() {
    String key = "KEY";
    int keySize = 300;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = key + i;
    }

    BulkFuture<Map<String, Object>> f = mc.asyncGetBulk(Arrays.asList(keys));

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void getBulkWithoutBulkMessage() {
    String key = "KEY";
    int keySize = 1;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = key + i;
    }

    BulkFuture<Map<String, Object>> f = mc.asyncGetBulk(Arrays.asList(keys));

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals(APIType.GET.toString(), messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void storeWithoutBulkMessage() {
    String key = "KEY";
    String value = "value";

    OperationFuture<Boolean> f
            = mc.add(key, 10, value);

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals(APIType.ADD.toString(), messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void setBulkWithBulkMessage() {
    String key = "KEY";
    int keySize = 100;

    Map<String, Object> map = new HashMap<>();

    for (int i = 0; i < keySize; i++) {
      map.put(key + i, i);
    }

    @SuppressWarnings("deprecation")
    Future<Map<String, CollectionOperationStatus>> f = mc.asyncSetBulk(map, 30);

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void setBulkWithoutBulkMessage() {
    String key = "KEY";
    int keySize = 1;

    Map<String, Object> map = new HashMap<>();

    for (int i = 0; i < keySize; i++) {
      map.put(key + i, i);
    }

    @SuppressWarnings("deprecation")
    Future<Map<String, CollectionOperationStatus>> f = mc.asyncSetBulk(map, 30);

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals(APIType.SET.toString(), messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void deleteBulkWithBulkMessage() {
    String key = "KEY";
    int keySize = 100;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = key + i;
    }

    Future<Map<String, OperationStatus>> f = mc.asyncDeleteBulk(Arrays.asList(keys));

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void deleteBulkWithoutBulkMessage() {
    String key = "KEY";
    int keySize = 1;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = key + i;
    }

    Future<Map<String, OperationStatus>> f = mc.asyncDeleteBulk(Arrays.asList(keys));

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals(APIType.DELETE.toString(), messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void sopPipedInsertBulkWithPipeMessage() {
    String key = "key";
    int valueCount = mc.getMaxPipedItemCount();
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "value" + i;
    }

    Future<Map<Integer, CollectionOperationStatus>> f = mc.asyncSopPipedInsertBulk(
            key, Arrays.asList(valueList), new CollectionAttributes());

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("pipe", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void sopPipedExistBulkWithPipeMessage() {
    String key = "key";
    int valueCount = mc.getMaxPipedItemCount();
    Object[] valueList = new Object[valueCount];
    for (int i = 0; i < valueList.length; i++) {
      valueList[i] = "value" + i;
    }

    CollectionFuture<Map<Object, Boolean>> f
            = mc.asyncSopPipedExistBulk(key, Arrays.asList(valueList));

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("pipe", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void sopInsertBulkWithBulkMessage() {
    String key = "key";
    String value = "MyValue";
    int keySize = 100;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = key + i;
    }

    Future<Map<String, CollectionOperationStatus>> f = mc.asyncSopInsertBulk(
            Arrays.asList(keys), value, new CollectionAttributes());

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void lopPipedInsertBulkWithPipeMessage() {
    String key = "key";
    int valueCount = 500;
    Object[] valueList = new Object[valueCount];
    Arrays.fill(valueList, "MyValue");

    // SET
    Future<Map<Integer, CollectionOperationStatus>> f = mc.asyncLopPipedInsertBulk(
            key, 0, Arrays.asList(valueList), new CollectionAttributes());
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("pipe", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void lopInsertBulkWithBulkMessage() {
    String value = "MyValue";
    int keySize = 250000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyLopKey" + i;
    }

    Future<Map<String, CollectionOperationStatus>> f = mc.asyncLopInsertBulk(
            Arrays.asList(keys), 0, value, new CollectionAttributes());
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void bopPipedInsertBulkWithPipeMessage() {
    String key = "MyBopKey";
    String value = "MyValue";

    int bkeySize = mc.getMaxPipedItemCount();
    Map<Long, Object> bkeys = new TreeMap<>();
    for (int i = 0; i < bkeySize; i++) {
      bkeys.put((long) i, value);
    }

    Future<Map<Integer, CollectionOperationStatus>> f = mc.asyncBopPipedInsertBulk(
            key, bkeys, new CollectionAttributes());
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("pipe", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void bopPipedUpdateBulkWithPipeMessage() {
    String key = "MyBopKey";

    List<Element<Object>> updateElements = new ArrayList<>();
    for (int i = 0; i < mc.getMaxPipedItemCount(); i++) {
      updateElements.add(new Element<>(i, "updated" + i,
              new ElementFlagUpdate(new byte[]{1, 1, 1, 1})));
    }

    Future<Map<Integer, CollectionOperationStatus>> f
            = mc.asyncBopPipedUpdateBulk(key, updateElements);
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("pipe", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void bopInsertBulkWithBulkMessage() {
    String value = "MyValue";
    long bkey = Long.MAX_VALUE;

    int keySize = 10000;
    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      String key = "MyBopKey" + i;
      keys[i] = key;
    }

    Future<Map<String, CollectionOperationStatus>> f = mc.asyncBopInsertBulk(
            Arrays.asList(keys), bkey, new byte[]{0, 0, 1, 1}, value, new CollectionAttributes());
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void oldSMGetWithBulkMessage() {
    String key = "MyBopKey";
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(key + i);
    }

    SMGetFuture<List<SMGetElement<Object>>> f = mc.asyncBopSortMergeGet(
            keyList, 0, 1000, ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void smGetTimeoutWithBulkMessage() {
    String key = "MyBopKey";
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(key + i);
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;

    SMGetFuture<List<SMGetElement<Object>>> f = mc.asyncBopSortMergeGet(
            keyList, 0, 1000, ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void byteArrayBKeyOldSMGetWithBulkMessage() {
    String key = "MyBopKey";
    ArrayList<String> keyList;
    keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(key + i);
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 1000};

    SMGetFuture<List<SMGetElement<Object>>> f = mc.asyncBopSortMergeGet(
            keyList, from, to, ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void byteArrayBKeySMGetWithBulkMessage() {
    String key = "MyBopKey";
    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keyList.add(key + i);
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 1000};

    SMGetMode smgetMode = SMGetMode.UNIQUE;

    SMGetFuture<List<SMGetElement<Object>>> f = mc.asyncBopSortMergeGet(
            keyList, from, to, ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void mopPipedInsertBulkWithPipeMessage() {
    String key = "MyMopKey";
    String value = "MyValue";

    int elementSize = mc.getMaxPipedItemCount();
    Map<String, Object> elements = new TreeMap<>();
    for (int i = 0; i < elementSize; i++) {
      elements.put(String.valueOf(i), value);
    }

    Future<Map<Integer, CollectionOperationStatus>> f = mc.asyncMopPipedInsertBulk(
            key, elements, new CollectionAttributes());
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("pipe", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    f.cancel(true);
  }

  @Test
  public void mopPipedUpdateBulkWithBulkMessage() {
    String mkey = "MyMopKey";
    String value = "MyValue";
    int keySize = 250000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyMopKey" + i;
    }

    Map<String, Object> updated = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      updated.put(keys[i], value + i);
    }

    CollectionFuture<Map<Integer, CollectionOperationStatus>> f =
            mc.asyncMopPipedUpdateBulk(mkey, updated);
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("pipe", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    f.cancel(true);
  }

  @Test
  public void mopInsertBulkWithBulkMessage() {
    String mkey = "MyMopKey";
    String value = "MyValue";
    int keySize = 250000;

    String[] keys = new String[keySize];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "MyMopKey" + i;
    }

    Future<Map<String, CollectionOperationStatus>> f = mc.asyncMopInsertBulk(
            Arrays.asList(keys), mkey, value, new CollectionAttributes());
    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      String[] messages = e.getMessage().split(" ");
      assertEquals("bulk", messages[0]);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    f.cancel(true);
  }

  @Test
  public void getVersionWithTimeout() {
    Operation op = mc.opFact.version(getCb());

    try {
      mc.getVersions();
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage());
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op));

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void getStatsWithTimeout() {
    Operation op = mc.opFact.stats(null, new StatsOperation.Callback() {
      @Override
      public void gotStat(String name, String val) {
      }

      @Override
      public void receivedStatus(OperationStatus status) {
      }

      @Override
      public void complete() {
      }
    });

    try {
      mc.getStats();
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage());
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op));

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void flushWithTimeout() {
    Future<Boolean> f = mc.flush();
    Operation op = mc.opFact.flush(-1, getCb());

    try {
      f.get(1L, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage());
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op));

      assertEquals(exceptionMessage, timedOutMessages);
    }
    f.cancel(true);
  }

  private OperationCallback getCb() {
    return new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
      }

      @Override
      public void complete() {
      }
    };
  }

  private String createTimedOutMessage(Operation op) {
    return TimedOutMessageFactory
            .createTimedOutMessage(1, TimeUnit.MILLISECONDS, Collections.singletonList(op));
  }

  private String getTimedOutHeadMessage(String message) {
    return message.split("-")[0];
  }
}
