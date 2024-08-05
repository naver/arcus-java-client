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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import junit.framework.TestCase;

import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkeyOld;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkeyOld;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.collection.SetPipedExist;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperationOld;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.StoreType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import static net.spy.memcached.collection.CollectionBulkInsert.BTreeBulkInsert;
import static net.spy.memcached.collection.CollectionBulkInsert.ListBulkInsert;
import static net.spy.memcached.collection.CollectionBulkInsert.MapBulkInsert;
import static net.spy.memcached.collection.CollectionBulkInsert.SetBulkInsert;
import static net.spy.memcached.collection.CollectionPipedInsert.BTreePipedInsert;
import static net.spy.memcached.collection.CollectionPipedInsert.ListPipedInsert;
import static net.spy.memcached.collection.CollectionPipedInsert.MapPipedInsert;
import static net.spy.memcached.collection.CollectionPipedInsert.SetPipedInsert;
import static net.spy.memcached.collection.CollectionPipedUpdate.BTreePipedUpdate;
import static net.spy.memcached.collection.CollectionPipedUpdate.MapPipedUpdate;
import static net.spy.memcached.collection.ElementFlagFilter.DO_NOT_FILTER;

@RunWith(BlockJUnit4ClassRunner.class)
public class ArcusTimeoutMessageTest extends TestCase {
  private static final int SINGLE = 0x00;
  private static final int BULK = 0x10;
  private static final int PIPE = 0x01;
  private static final int BULK_PIPE = 0x11;

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
    GetFuture<?> f = mc.asyncGet(key);
    Operation op = mc.opFact.get(key, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void getBulkWithBulkMessage() {
    String key = "KEY";
    int keySize = 300;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    BulkFuture<?> f = mc.asyncGetBulk(keys);
    Operation op = mc.opFact.get(keys, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void getBulkWithoutBulkMessage() {
    String key = "KEY";
    int keySize = 1;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    BulkFuture<?> f = mc.asyncGetBulk(keys);
    Operation op = mc.opFact.get(keys, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void storeWithoutBulkMessage() {
    String key = "KEY";
    String value = "value";

    OperationFuture<?> f = mc.add(key, 10, value);
    Operation op = mc.opFact.store(StoreType.add, key, 0, 0, null, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void setBulkWithBulkMessage() {
    String key = "KEY";
    int keySize = 100;
    Map<String, Object> values = new HashMap<>(keySize);

    for (int i = 0; i < keySize; i++) {
      values.put(key + i, i);
    }

    Future<?> f = mc.asyncStoreBulk(StoreType.set, values, 30);
    List<Operation> ops = values.keySet().stream()
            .map((k) -> mc.opFact.store(StoreType.set, k, 0, 0, null, null))
            .collect(Collectors.toList());

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(ops), BULK);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void setBulkWithoutBulkMessage() {
    String key = "KEY";
    int keySize = 1;
    Map<String, Object> values = new HashMap<>(keySize);

    for (int i = 0; i < keySize; i++) {
      values.put(key + i, i);
    }

    Future<?> f = mc.asyncStoreBulk(StoreType.set, values, 30);
    List<Operation> ops = values.keySet().stream()
            .map((k) -> mc.opFact.store(StoreType.set, k, 0, 0, null, null))
            .collect(Collectors.toList());

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(ops), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void deleteBulkWithBulkMessage() {
    String key = "KEY";
    int keySize = 100;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    Future<?> f = mc.asyncDeleteBulk(keys);
    List<Operation> ops = keys.stream()
            .map((k) -> mc.opFact.delete(k, null))
            .collect(Collectors.toList());

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(ops), BULK);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void deleteBulkWithoutBulkMessage() {
    String key = "KEY";
    int keySize = 1;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    Future<?> f = mc.asyncDeleteBulk(keys);
    List<Operation> ops = keys.stream()
            .map((k) -> mc.opFact.delete(k, null))
            .collect(Collectors.toList());

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(ops), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void sopPipedInsertBulkWithPipeMessage() {
    String key = "key";
    int valueCount = mc.getMaxPipedItemCount();
    List<Object> values = new ArrayList<>(valueCount);

    for (int i = 0; i < valueCount; i++) {
      values.add("value" + i);
    }

    CollectionAttributes attr = new CollectionAttributes();
    CollectionFuture<?> f = mc.asyncSopPipedInsertBulk(key, values, attr);
    SetPipedInsert<?> insert = new SetPipedInsert<>(key, new HashSet<>(), attr, null);
    Operation op = mc.opFact.collectionPipedInsert(key, insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void sopPipedExistBulkWithPipeMessage() {
    String key = "key";
    int valueCount = mc.getMaxPipedItemCount();
    List<Object> values = new ArrayList<>(valueCount);

    for (int i = 0; i < valueCount; i++) {
      values.add("value" + i);
    }

    CollectionFuture<?> f = mc.asyncSopPipedExistBulk(key, values);
    SetPipedExist<?> exist = new SetPipedExist<>(key, values, null);
    Operation op = mc.opFact.collectionPipedExist(key, exist, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void sopInsertBulkWithBulkMessage() {
    String key = "key";
    String value = "MyValue";
    int keySize = 100;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    CollectionAttributes attr = new CollectionAttributes();
    Future<?> f = mc.asyncSopInsertBulk(keys, value, attr);
    SetBulkInsert<?> insert = new SetBulkInsert<>(null, keys, null, attr);
    Operation op = mc.opFact.collectionBulkInsert(insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK_PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK_PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void lopPipedInsertBulkWithPipeMessage() {
    String key = "key";
    int valueCount = 500;
    List<Object> values = new ArrayList<>(valueCount);

    for (int i = 0; i < valueCount; i++) {
      values.add("MyValue");
    }

    CollectionAttributes attr = new CollectionAttributes();
    CollectionFuture<?> f = mc.asyncLopPipedInsertBulk(key, 0, values, attr);
    ListPipedInsert<?> insert = new ListPipedInsert<>(key, 0, values, attr, null);
    Operation op = mc.opFact.collectionPipedInsert(key, insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void lopInsertBulkWithBulkMessage() {
    String value = "MyValue";
    int keySize = 250000;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add("MyLopKey" + i);
    }

    CollectionAttributes attr = new CollectionAttributes();
    Future<?> f = mc.asyncLopInsertBulk(keys, 0, value, attr);
    ListBulkInsert<?> insert = new ListBulkInsert<>(null, keys, 0, null, attr);
    Operation op = mc.opFact.collectionBulkInsert(insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK_PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK_PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void bopPipedInsertBulkWithPipeMessage() {
    String key = "MyBopKey";
    String value = "MyValue";
    int elementSize = mc.getMaxPipedItemCount();
    Map<Long, Object> elements = new TreeMap<>();

    for (int i = 0; i < elementSize; i++) {
      elements.put((long) i, value);
    }

    CollectionAttributes attr = new CollectionAttributes();
    CollectionFuture<?> f = mc.asyncBopPipedInsertBulk(key, elements, attr);
    BTreePipedInsert<?> insert = new BTreePipedInsert<>(key, elements, attr, null);
    Operation op = mc.opFact.collectionPipedInsert(key, insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void bopPipedUpdateBulkWithPipeMessage() {
    String key = "MyBopKey";
    String value = "updated";
    ElementFlagUpdate eflagUpdate = new ElementFlagUpdate(new byte[]{1, 1, 1, 1});
    int valueCount = mc.getMaxPipedItemCount();
    List<Element<Object>> updateElements = new ArrayList<>(valueCount);

    for (int i = 0; i < valueCount; i++) {
      updateElements.add(new Element<>(i, value + i, eflagUpdate));
    }

    CollectionFuture<?> f = mc.asyncBopPipedUpdateBulk(key, updateElements);
    BTreePipedUpdate<?> update = new BTreePipedUpdate<>(key, updateElements, null);
    Operation op = mc.opFact.collectionPipedUpdate(key, update, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void bopInsertBulkWithBulkMessage() {
    String value = "MyValue";
    long bkey = Long.MAX_VALUE;
    int keySize = 10000;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add("MyBopKey" + i);
    }

    CollectionAttributes attr = new CollectionAttributes();
    Future<?> f = mc.asyncBopInsertBulk(keys, bkey, new byte[]{0, 0, 1, 1}, value, attr);
    BTreeBulkInsert<?> insert = new BTreeBulkInsert<>(null, keys, "", "" , null, attr);
    Operation op = mc.opFact.collectionBulkInsert(insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK_PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK_PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void oldSMGetWithBulkMessage() {
    String key = "MyBopKey";
    int keySize = 1000;
    int count = keySize / 2;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    SMGetFuture<?> f = mc.asyncBopSortMergeGet(keys, 0, keySize, DO_NOT_FILTER, 0, count);
    BTreeSMGet<?> smGet = new BTreeSMGetWithLongTypeBkeyOld<>(
            null, keys, 0, keySize, DO_NOT_FILTER, 0, count);
    Operation op = mc.opFact.bopsmget(smGet, (BTreeSortMergeGetOperationOld.Callback) null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void smGetTimeoutWithBulkMessage() {
    String key = "MyBopKey";
    int keySize = 1000;
    int count = keySize / 2;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    SMGetMode mode = SMGetMode.UNIQUE;
    SMGetFuture<?> f = mc.asyncBopSortMergeGet(keys, 0, keySize, DO_NOT_FILTER, count, mode);
    BTreeSMGet<?> smGet = new BTreeSMGetWithLongTypeBkey<>(
            null, keys, 0, keySize, DO_NOT_FILTER, count, mode);
    Operation op = mc.opFact.bopsmget(smGet, (BTreeSortMergeGetOperation.Callback) null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void byteArrayBKeyOldSMGetWithBulkMessage() {
    String key = "MyBopKey";
    byte keySize = Byte.MAX_VALUE;
    int count = keySize / 2;
    ArrayList<String> keys = new ArrayList<>(keySize);

    for (byte i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    byte[] from = new byte[]{0};
    byte[] to = new byte[]{keySize};

    SMGetFuture<?> f = mc.asyncBopSortMergeGet(keys, from, to, DO_NOT_FILTER, 0, count);
    BTreeSMGet<?> smGet = new BTreeSMGetWithByteTypeBkeyOld<>(
            null, keys, from, to, DO_NOT_FILTER, 0, count);
    Operation op = mc.opFact.bopsmget(smGet, (BTreeSortMergeGetOperationOld.Callback) null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void byteArrayBKeySMGetWithBulkMessage() {
    String key = "MyBopKey";
    byte keySize = Byte.MAX_VALUE;
    int count = keySize / 2;
    ArrayList<String> keys = new ArrayList<>(keySize);

    for (byte i = 0; i < keySize; i++) {
      keys.add(key + i);
    }

    byte[] from = new byte[]{0};
    byte[] to = new byte[]{keySize};

    SMGetMode mode = SMGetMode.UNIQUE;
    SMGetFuture<?> f = mc.asyncBopSortMergeGet(keys, from, to, DO_NOT_FILTER, 500, mode);
    BTreeSMGet<?> smGet = new BTreeSMGetWithByteTypeBkey<>(
            null, keys, from, to, DO_NOT_FILTER, count, mode);
    Operation op = mc.opFact.bopsmget(smGet, (BTreeSortMergeGetOperation.Callback) null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK);

      assertEquals(exceptionMessage, timedOutMessages);
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

    CollectionAttributes attr = new CollectionAttributes();
    CollectionFuture<?> f = mc.asyncMopPipedInsertBulk(key, elements, attr);
    MapPipedInsert<?> insert = new MapPipedInsert<>(key, elements, attr, null);
    Operation op = mc.opFact.collectionPipedInsert(key, insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void mopPipedUpdateBulkWithBulkMessage() {
    String key = "MyMopKey";
    String value = "MyValue";
    int elementSize = 250000;
    Map<String, Object> elements = new HashMap<>(elementSize);

    for (int i = 0; i < elementSize; i++) {
      elements.put("MyMopKey" + i, value + i);
    }

    CollectionFuture<?> f = mc.asyncMopPipedUpdateBulk(key, elements);
    MapPipedUpdate<?> update = new MapPipedUpdate<>(key, elements, null);
    Operation op = mc.opFact.collectionPipedUpdate(key, update, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void mopInsertBulkWithBulkMessage() {
    String mkey = "MyMopKey";
    String value = "MyValue";
    int keySize = 250000;
    List<String> keys = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      keys.add("MyMopKey" + i);
    }

    CollectionAttributes attr = new CollectionAttributes();
    Future<?> f = mc.asyncMopInsertBulk(keys, mkey, value, attr);
    MapBulkInsert<?> insert = new MapBulkInsert<>(null, keys, mkey, null, attr);
    Operation op = mc.opFact.collectionBulkInsert(insert, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), BULK_PIPE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), BULK_PIPE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void getVersionWithTimeout() {
    Operation op = mc.opFact.version(null);

    try {
      Object v = mc.getVersions();
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void getStatsWithTimeout() {
    Operation op = mc.opFact.stats(null, null);

    try {
      Object v = mc.getStats();
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  @Test
  public void flushWithTimeout() {
    Future<Boolean> f = mc.flush();
    Operation op = mc.opFact.flush(-1, null);

    try {
      Object v = f.get(1L, TimeUnit.MILLISECONDS);
      fail("Timeout not occurred with value: " + v);
    } catch (Exception e) {
      String exceptionMessage = getTimedOutHeadMessage(e.getMessage(), SINGLE);
      String timedOutMessages = getTimedOutHeadMessage(createTimedOutMessage(op), SINGLE);

      assertEquals(exceptionMessage, timedOutMessages);
    }
  }

  private String createTimedOutMessage(Operation op) {
    return createTimedOutMessage(Collections.singleton(op));
  }

  private String createTimedOutMessage(Collection<Operation> ops) {
    return TimedOutMessageFactory.createTimedOutMessage(1, TimeUnit.MILLISECONDS, 0, ops);
  }

  private String getTimedOutHeadMessage(String message, int flags) {
    String[] tokens = message.split("-")[0].split(" >= ");
    String token = tokens[0];

    token = token.substring(0, token.lastIndexOf('('));
    String result = (token + "(>= " + tokens[1]).trim();
    String regex = "[A-Z|_]+ operation timed out \\(>= [0-9]+ [A-Z]+\\)";

    if ((flags & BULK_PIPE) == BULK_PIPE) {
      regex = "(bulk pipe )" + regex;
      assertTrue(result, Pattern.matches(regex, result));
    } else if ((flags & BULK) == BULK) {
      regex = "(bulk )" + regex;
      assertTrue(result, Pattern.matches(regex, result));
    } else if ((flags & PIPE) == PIPE) {
      regex = "(pipe )" + regex;
      assertTrue(result, Pattern.matches(regex, result));
    } else {
      assertTrue(result, Pattern.matches(regex, result));
    }

    return result;
  }
}
