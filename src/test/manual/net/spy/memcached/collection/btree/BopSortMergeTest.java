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
package net.spy.memcached.collection.btree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BopSortMergeTest extends BaseIntegrationTest {

  private final List<String> keyList3 = new ArrayList<>();
  private final List<String> keyList2 = new ArrayList<>();

  public BopSortMergeTest() {
    keyList3.add("key0");
    keyList3.add("key1");
    keyList3.add("key2");

    keyList2.add("key0");
    keyList2.add("key1");
  }

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    for (String s : keyList3) {
      mc.delete(s).get();
    }
    for (String s : keyList2) {
      mc.delete(s).get();
    }
  }

  @Test
  public void testBopSortMergeAscDuplicate1() throws Exception {
    long bkeyFrom;
    long bkeyTo;
    int queryCount;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key1:  5,  4,  3, 2, 1
    // - key2:  8,  7,  6

    bkeyFrom = 0L;
    bkeyTo = 5L;
    queryCount = 10;

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = null;
    List<SMGetElement<Object>> result;

    try {
      future = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, queryCount, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future);

    try {
      result = future.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(5, result.size());
      // expected value set
      // key1 1
      // key1 2
      // key1 3
      // key1 4
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(1, future.getMissedKeys().size());
      for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
        assertEquals("key0", m.getKey());
        assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      }

      /*
       * trim key
       */
      assertEquals(0, future.getTrimmedKeys().size());

      /*
        response
       */
      assertEquals(CollectionResponse.END, future.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeAscUnique1() throws Exception {
    long bkeyFrom;
    long bkeyTo;
    int queryCount;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key1:  5,  4,  3, 2, 1
    // - key2:  8,  7,  6

    bkeyFrom = 0L;
    bkeyTo = 5L;
    queryCount = 10;

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = null;
    List<SMGetElement<Object>> result;

    try {
      future = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, queryCount, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future);

    try {
      result = future.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(5, result.size());
      // expected value set
      // key1 1
      // key1 2
      // key1 3
      // key1 4
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(1, future.getMissedKeys().size());
      for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
        assertEquals("key0", m.getKey());
        assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      }

      /*
       * trim key
       */
      assertEquals(0, future.getTrimmedKeys().size());
      for (SMGetTrimKey e : future.getTrimmedKeys()) {
        //System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
      }

      /*
       * response
       */
      assertEquals(CollectionResponse.END, future.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeDescDuplicate1() throws Exception {
    long bkeyFrom;
    long bkeyTo;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key1:  5,  4,  3, 2, 1
    // - key2:  8,  7,  6

    bkeyFrom = 5L;
    bkeyTo = 0L;

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future10 = null;
    SMGetFuture<List<SMGetElement<Object>>> future6 = null;
    SMGetFuture<List<SMGetElement<Object>>> future5 = null;
    SMGetFuture<List<SMGetElement<Object>>> future3 = null;
    SMGetFuture<List<SMGetElement<Object>>> future1 = null;
    List<SMGetElement<Object>> result10;
    List<SMGetElement<Object>> result6;
    List<SMGetElement<Object>> result5;
    List<SMGetElement<Object>> result3;
    List<SMGetElement<Object>> result1;

    try {
      future10 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
      future6 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 6, smgetMode);
      future5 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 5, smgetMode);
      future3 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 3, smgetMode);
      future1 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 1, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future10);
    assertNotNull(future6);
    assertNotNull(future5);
    assertNotNull(future3);
    assertNotNull(future1);

    try {
      result10 = future10.get(1000L, TimeUnit.MILLISECONDS);
      result6 = future6.get(1000L, TimeUnit.MILLISECONDS);
      result5 = future5.get(1000L, TimeUnit.MILLISECONDS);
      result3 = future3.get(1000L, TimeUnit.MILLISECONDS);
      result1 = future1.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(8, result10.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3
      // key0 3
      // key1 2
      // key1 1

      assertEquals(6, result6.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3
      // key0 3

      assertEquals(5, result5.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3

      assertEquals(3, result3.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4

      assertEquals(1, result1.size());
      // expected value set
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(0, future10.getMissedKeys().size());
      assertEquals(0, future6.getMissedKeys().size());
      assertEquals(0, future5.getMissedKeys().size());
      assertEquals(0, future3.getMissedKeys().size());
      assertEquals(0, future1.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //  assertEquals("key0", m.getKey());
      //  assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * trim key
       */
      assertEquals(1, future10.getTrimmedKeys().size());
      for (SMGetTrimKey e : future10.getTrimmedKeys()) {
        assertEquals("key0", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(0, future6.getTrimmedKeys().size());
      assertEquals(0, future5.getTrimmedKeys().size());
      assertEquals(0, future3.getTrimmedKeys().size());
      assertEquals(0, future1.getTrimmedKeys().size());
      //for (SMGetTrimKey e : future.getTrimmedKeys()) {
      //  System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.DUPLICATED, future10.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.DUPLICATED, future6.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.DUPLICATED, future5.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.DUPLICATED, future3.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future1.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeDescUnique1() throws Exception {
    long bkeyFrom;
    long bkeyTo;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key1:  5,  4,  3, 2, 1
    // - key2:  8,  7,  6

    bkeyFrom = 5L;
    bkeyTo = 0L;

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future10 = null;
    SMGetFuture<List<SMGetElement<Object>>> future6 = null;
    SMGetFuture<List<SMGetElement<Object>>> future5 = null;
    SMGetFuture<List<SMGetElement<Object>>> future3 = null;
    SMGetFuture<List<SMGetElement<Object>>> future1 = null;
    List<SMGetElement<Object>> result10;
    List<SMGetElement<Object>> result6;
    List<SMGetElement<Object>> result5;
    List<SMGetElement<Object>> result3;
    List<SMGetElement<Object>> result1;

    try {
      future10 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
      future6 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 6, smgetMode);
      future5 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 5, smgetMode);
      future3 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 3, smgetMode);
      future1 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 1, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future10);
    assertNotNull(future6);
    assertNotNull(future5);
    assertNotNull(future3);
    assertNotNull(future1);

    try {
      result10 = future10.get(1000L, TimeUnit.MILLISECONDS);
      result6 = future6.get(1000L, TimeUnit.MILLISECONDS);
      result5 = future5.get(1000L, TimeUnit.MILLISECONDS);
      result3 = future3.get(1000L, TimeUnit.MILLISECONDS);
      result1 = future1.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(5, result10.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3
      // key1 2
      // key1 1

      assertEquals(5, result6.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3
      // key1 2
      // key1 1

      assertEquals(5, result5.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3
      // key1 2
      // key1 1

      assertEquals(3, result3.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3

      assertEquals(1, result1.size());
      // expected value set
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      // missed key
      assertEquals(0, future10.getMissedKeys().size());
      assertEquals(0, future6.getMissedKeys().size());
      assertEquals(0, future5.getMissedKeys().size());
      assertEquals(0, future3.getMissedKeys().size());
      assertEquals(0, future1.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //  assertEquals("key0", m.getKey());
      //  assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * trim key
       */
      assertEquals(1, future10.getTrimmedKeys().size());
      for (SMGetTrimKey e : future10.getTrimmedKeys()) {
        assertEquals("key0", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(1, future6.getTrimmedKeys().size());
      for (SMGetTrimKey e : future6.getTrimmedKeys()) {
        assertEquals("key0", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(1, future5.getTrimmedKeys().size());
      for (SMGetTrimKey e : future5.getTrimmedKeys()) {
        assertEquals("key0", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(0, future3.getTrimmedKeys().size());
      assertEquals(0, future1.getTrimmedKeys().size());
      //for (SMGetTrimKey e : future.getTrimmedKeys()) {
      //  System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.END, future10.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future6.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future5.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future3.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future1.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeAscDuplicate2() throws Exception {
    long bkeyFrom;
    long bkeyTo;
    int queryCount;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0:  5,  4,  3, 2, 1
    // - key1: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key2:  8,  7,  6

    bkeyFrom = 0L;
    bkeyTo = 5L;
    queryCount = 10;

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = null;
    List<SMGetElement<Object>> result;

    try {
      future = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, queryCount, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future);

    try {
      result = future.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(5, result.size());
      // expected value set
      // key0 1
      // key0 2
      // key0 3
      // key0 4
      // key0 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(1, future.getMissedKeys().size());
      for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
        assertEquals("key1", m.getKey());
        assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      }

      /*
       * trim key
       */
      assertEquals(0, future.getTrimmedKeys().size());

      /*
       * response
       */
      assertEquals(CollectionResponse.END, future.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeAscUnique2() throws Exception {
    long bkeyFrom;
    long bkeyTo;
    int queryCount;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0:  5,  4,  3, 2, 1
    // - key1: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key2:  8,  7,  6

    bkeyFrom = 0L;
    bkeyTo = 5L;
    queryCount = 10;

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = null;
    List<SMGetElement<Object>> result;

    try {
      future = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, queryCount, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future);

    try {
      result = future.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(5, result.size());
      // expected value set
      // key0 1
      // key0 2
      // key0 3
      // key0 4
      // key0 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(1, future.getMissedKeys().size());
      for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
        assertEquals("key1", m.getKey());
        assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      }

      /*
       * trim key
       */
      assertEquals(0, future.getTrimmedKeys().size());
      //for (SMGetTrimKey e : future.getTrimmedKeys()) {
      //  System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.END, future.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeDescDuplicate2() throws Exception {
    long bkeyFrom;
    long bkeyTo;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0:  5,  4,  3, 2, 1
    // - key1: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key2:  8,  7,  6

    bkeyFrom = 5L;
    bkeyTo = 0L;

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future10 = null;
    SMGetFuture<List<SMGetElement<Object>>> future6 = null;
    SMGetFuture<List<SMGetElement<Object>>> future5 = null;
    SMGetFuture<List<SMGetElement<Object>>> future3 = null;
    SMGetFuture<List<SMGetElement<Object>>> future1 = null;
    List<SMGetElement<Object>> result10;
    List<SMGetElement<Object>> result6;
    List<SMGetElement<Object>> result5;
    List<SMGetElement<Object>> result3;
    List<SMGetElement<Object>> result1;

    try {
      future10 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
      future6 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 6, smgetMode);
      future5 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 5, smgetMode);
      future3 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 3, smgetMode);
      future1 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 1, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future10);
    assertNotNull(future6);
    assertNotNull(future5);
    assertNotNull(future3);
    assertNotNull(future1);

    try {
      result10 = future10.get(1000L, TimeUnit.MILLISECONDS);
      result6 = future6.get(1000L, TimeUnit.MILLISECONDS);
      result5 = future5.get(1000L, TimeUnit.MILLISECONDS);
      result3 = future3.get(1000L, TimeUnit.MILLISECONDS);
      result1 = future1.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(8, result10.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3
      // key0 3
      // key0 2
      // key0 1

      assertEquals(6, result6.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3
      // key0 3

      assertEquals(5, result5.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3

      assertEquals(3, result3.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4

      assertEquals(1, result1.size());
      // expected value set
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(0, future10.getMissedKeys().size());
      assertEquals(0, future6.getMissedKeys().size());
      assertEquals(0, future5.getMissedKeys().size());
      assertEquals(0, future3.getMissedKeys().size());
      assertEquals(0, future1.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //  assertEquals("key0", m.getKey());
      //  assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * trim key
       */
      assertEquals(1, future10.getTrimmedKeys().size());
      for (SMGetTrimKey e : future10.getTrimmedKeys()) {
        assertEquals("key1", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(0, future6.getTrimmedKeys().size());
      assertEquals(0, future5.getTrimmedKeys().size());
      assertEquals(0, future3.getTrimmedKeys().size());
      assertEquals(0, future1.getTrimmedKeys().size());
      //for (SMGetTrimKey e : future.getTrimmedKeys()) {
      //  System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.DUPLICATED, future10.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.DUPLICATED, future6.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.DUPLICATED, future5.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.DUPLICATED, future3.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future1.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeDescUnique2() throws Exception {
    long bkeyFrom;
    long bkeyTo;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList3) {
      if (eachString.equals("key0")) {
        for (long i = 1; i <= 5; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 12; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0:  5,  4,  3, 2, 1
    // - key1: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key2:  8,  7,  6

    bkeyFrom = 5L;
    bkeyTo = 0L;

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future10 = null;
    SMGetFuture<List<SMGetElement<Object>>> future6 = null;
    SMGetFuture<List<SMGetElement<Object>>> future5 = null;
    SMGetFuture<List<SMGetElement<Object>>> future3 = null;
    SMGetFuture<List<SMGetElement<Object>>> future1 = null;
    List<SMGetElement<Object>> result10;
    List<SMGetElement<Object>> result6;
    List<SMGetElement<Object>> result5;
    List<SMGetElement<Object>> result3;
    List<SMGetElement<Object>> result1;

    try {
      future10 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
      future6 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 6, smgetMode);
      future5 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 5, smgetMode);
      future3 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 3, smgetMode);
      future1 = mc.asyncBopSortMergeGet(
          keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 1, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future10);
    assertNotNull(future6);
    assertNotNull(future5);
    assertNotNull(future3);
    assertNotNull(future1);

    try {
      result10 = future10.get(1000L, TimeUnit.MILLISECONDS);
      result6 = future6.get(1000L, TimeUnit.MILLISECONDS);
      result5 = future5.get(1000L, TimeUnit.MILLISECONDS);
      result3 = future3.get(1000L, TimeUnit.MILLISECONDS);
      result1 = future1.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(5, result10.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3
      // key0 2
      // key0 1

      assertEquals(5, result6.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3
      // key0 2
      // key0 1

      assertEquals(5, result5.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3
      // key0 2
      // key0 1

      assertEquals(3, result3.size());
      // expected value set
      // key1 5
      // key1 4
      // key1 3

      assertEquals(1, result1.size());
      // expected value set
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(0, future10.getMissedKeys().size());
      assertEquals(0, future6.getMissedKeys().size());
      assertEquals(0, future5.getMissedKeys().size());
      assertEquals(0, future3.getMissedKeys().size());
      assertEquals(0, future1.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //  assertEquals("key0", m.getKey());
      //  assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * trim key
       */
      assertEquals(1, future10.getTrimmedKeys().size());
      for (SMGetTrimKey e : future10.getTrimmedKeys()) {
        assertEquals("key1", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(1, future6.getTrimmedKeys().size());
      for (SMGetTrimKey e : future6.getTrimmedKeys()) {
        assertEquals("key1", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(1, future5.getTrimmedKeys().size());
      for (SMGetTrimKey e : future5.getTrimmedKeys()) {
        assertEquals("key1", e.getKey());
        assertEquals(3, e.getBkey());
      }
      assertEquals(0, future3.getTrimmedKeys().size());
      assertEquals(0, future1.getTrimmedKeys().size());
      //for (SMGetTrimKey e : future.getTrimmedKeys()) {
      //  System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.END, future10.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future6.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future5.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future3.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future1.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeDescDuplicate3() throws Exception {
    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList2) {
      if (eachString.equals("key0")) {
        for (long i = 19; i <= 29; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 9; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, trim
    // - key1:  9,  8,  7,  6,  5,  4,  3,  2,  1

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future1 = null; // bop smget 9 2 20..10 100 duplicate
    SMGetFuture<List<SMGetElement<Object>>> future2 = null; // bop smget 9 2 9..5 100 duplicate
    List<SMGetElement<Object>> result1;
    List<SMGetElement<Object>> result2;

    try {
      future1 = mc.asyncBopSortMergeGet(
          keyList2, 20, 10, ElementFlagFilter.DO_NOT_FILTER, 100, smgetMode);
      future2 = mc.asyncBopSortMergeGet(
          keyList2, 9, 5, ElementFlagFilter.DO_NOT_FILTER, 100, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future1);
    assertNotNull(future2);

    try {
      result1 = future1.get(1000L, TimeUnit.MILLISECONDS);
      result2 = future2.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(1, result1.size());
      // expected value set
      // key0 20

      assertEquals(5, result2.size());
      // expected value set
      // key1 9
      // key1 8
      // key1 7
      // key1 6
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(0, future1.getMissedKeys().size());
      assertEquals(1, future2.getMissedKeys().size());
      for (Map.Entry<String, CollectionOperationStatus> m : future2.getMissedKeys().entrySet()) {
        assertEquals("key0", m.getKey());
        assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      }

      /*
       * trim key
       */
      assertEquals(1, future1.getTrimmedKeys().size());
      for (SMGetTrimKey e : future1.getTrimmedKeys()) {
        assertEquals("key0", e.getKey());
        assertEquals(20, e.getBkey());
      }
      assertEquals(0, future2.getTrimmedKeys().size());

      /*
       * response
       */
      assertEquals(CollectionResponse.END, future1.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future2.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future1.cancel(true);
      future2.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future1.cancel(true);
      future2.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopSortMergeDescUnique3() throws Exception {
    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList2) {
      if (eachString.equals("key0")) {
        for (long i = 19; i <= 29; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 9; i++) {
          Assertions.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, trim
    // - key1:  9,  8,  7,  6,  5,  4,  3,  2,  1

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future1 = null; // bop smget 9 2 20..10 100 unique
    SMGetFuture<List<SMGetElement<Object>>> future2 = null; // bop smget 9 2 9..5 100 unique
    List<SMGetElement<Object>> result1;
    List<SMGetElement<Object>> result2;

    try {
      future1 = mc.asyncBopSortMergeGet(
          keyList2, 20, 10, ElementFlagFilter.DO_NOT_FILTER, 100, smgetMode);
      future2 = mc.asyncBopSortMergeGet(
          keyList2, 9, 5, ElementFlagFilter.DO_NOT_FILTER, 100, smgetMode);
    } catch (IllegalStateException e) {
      Assertions.fail(e.getMessage());
    }

    assertNotNull(future1);
    assertNotNull(future2);

    try {
      result1 = future1.get(1000L, TimeUnit.MILLISECONDS);
      result2 = future2.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(1, result1.size());
      // expected value set
      // key0 20

      assertEquals(5, result2.size());
      // expected value set
      // key1 9
      // key1 8
      // key1 7
      // key1 6
      // key1 5
      //for (SMGetElement<Object> each : result) {
      //  System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(0, future1.getMissedKeys().size());
      assertEquals(1, future2.getMissedKeys().size());
      for (Map.Entry<String, CollectionOperationStatus> m : future2.getMissedKeys().entrySet()) {
        assertEquals("key0", m.getKey());
        assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      }

      /*
       * trim key
       */
      assertEquals(1, future1.getTrimmedKeys().size());
      for (SMGetTrimKey e : future1.getTrimmedKeys()) {
        assertEquals("key0", e.getKey());
        assertEquals(20, e.getBkey());
      }
      assertEquals(0, future2.getTrimmedKeys().size());

      /*
       * response
       */
      assertEquals(CollectionResponse.END, future1.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future2.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future1.cancel(true);
      future2.cancel(true);
      Assertions.fail(e.getMessage());
    } catch (ExecutionException e) {
      future1.cancel(true);
      future2.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }
}
