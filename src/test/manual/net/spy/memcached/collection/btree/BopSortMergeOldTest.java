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

import junit.framework.Assert;
import net.spy.memcached.collection.*;
import net.spy.memcached.internal.SMGetFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BopSortMergeOldTest extends BaseIntegrationTest {

  private List<String> keyList3 = new ArrayList<String>() {{
    add("key0");
    add("key1");
    add("key2");
  }};

  private List<String> keyList2 = new ArrayList<String>() {{
    add("key0");
    add("key1");
  }};

  protected void setUp() throws Exception {
    try {
      super.setUp();
      for (int i = 0; i < keyList3.size(); i++) {
        mc.delete(keyList3.get(i));
      }

      for (int i = 0; i < keyList2.size(); i++) {
        mc.delete(keyList2.get(i));
      }
    } catch (Exception ignored) {
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testBopSortMergeOldDesc1() throws Exception {
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
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 5; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key1:  5,  4,  3, 2, 1
    // - key2:  8,  7,  6

    bkeyFrom = 5L;
    bkeyTo = 0L;

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
      future10 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      future6 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 6);
      future5 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 5);
      future3 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 3);
      future1 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 1);
    } catch (IllegalStateException e) {
      Assert.fail(e.getMessage());
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
      assertEquals(6, result10.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3
      // key0 3

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
      //	System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * miss key
       */
      assertEquals(0, future10.getMissedKeys().size());
      assertEquals(0, future6.getMissedKeys().size());
      assertEquals(0, future5.getMissedKeys().size());
      assertEquals(0, future3.getMissedKeys().size());
      assertEquals(0, future1.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //	assertEquals("key0", m.getKey());
      //	assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.DUPLICATED_TRIMMED, future10.getOperationStatus().getResponse());
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
      Assert.fail(e.getMessage());
    } catch (ExecutionException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBopSortMergeOldDesc2() throws Exception {
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
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 12; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key2")) {
        for (long i = 6; i <= 8; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0:  5,  4,  3, 2, 1
    // - key1: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, trim
    // - key2:  8,  7,  6

    bkeyFrom = 5L;
    bkeyTo = 0L;

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
      future10 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      future6 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 6);
      future5 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 5);
      future3 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 3);
      future1 = mc.asyncBopSortMergeGet(keyList3, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 1);
    } catch (IllegalStateException e) {
      Assert.fail(e.getMessage());
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
      assertEquals(6, result10.size());
      // expected value set
      // key1 5
      // key0 5
      // key1 4
      // key0 4
      // key1 3
      // key0 3

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
      //	System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * miss key
       */
      assertEquals(0, future10.getMissedKeys().size());
      assertEquals(0, future6.getMissedKeys().size());
      assertEquals(0, future5.getMissedKeys().size());
      assertEquals(0, future3.getMissedKeys().size());
      assertEquals(0, future1.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //	assertEquals("key0", m.getKey());
      //	assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.DUPLICATED_TRIMMED, future10.getOperationStatus().getResponse());
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
      Assert.fail(e.getMessage());
    } catch (ExecutionException e) {
      future10.cancel(true);
      future6.cancel(true);
      future5.cancel(true);
      future3.cancel(true);
      future1.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBopSortMergeOldDesc3() throws Exception {
    long bkeyFrom;
    long bkeyTo;

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList2) {
      if (eachString.equals("key0")) {
        for (long i = 19; i <= 29; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 9; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, trim
    // - key1:  9,  8,  7,  6,  5,  4,  3,  2,  1

    bkeyFrom = 20L;
    bkeyTo = 10L;

    SMGetFuture<List<SMGetElement<Object>>> future = null;
    List<SMGetElement<Object>> result;

    try {
      future = mc.asyncBopSortMergeGet(keyList2, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, 0, 100);
    } catch (IllegalStateException e) {
      Assert.fail(e.getMessage());
    }

    assertNotNull(future);

    try {
      result = future.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(1, result.size());
      // expected value set
      // key0 20
      //for (SMGetElement<Object> each : result) {
      //	System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(0, future.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //	assertEquals("key0", m.getKey());
      //	assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.TRIMMED, future.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future.cancel(true);
      Assert.fail(e.getMessage());
    } catch (ExecutionException e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBopSortMergeOldDesc4() throws Exception {

    // insert test data
    // key list (maxcount = 10)
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);

    for (String eachString : keyList2) {
      if (eachString.equals("key0")) {
        for (long i = 19; i <= 29; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      } else if (eachString.equals("key1")) {
        for (long i = 1; i <= 9; i++) {
          Assert.assertTrue(mc.asyncBopInsert(eachString, i, null, "val", attrs).get());
        }
      }
    }
    // - key0: 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, trim
    // - key1:  9,  8,  7,  6,  5,  4,  3,  2,  1


    SMGetFuture<List<SMGetElement<Object>>> future1 = null; // bop smget 9 2 20..10 5 100
    SMGetFuture<List<SMGetElement<Object>>> future2 = null; // bop smget 9 2 22..20 5 100
    SMGetFuture<List<SMGetElement<Object>>> future3 = null; // bop smget 9 2 50..40 0 100
    SMGetFuture<List<SMGetElement<Object>>> future4 = null; // bop smget 9 2 9..5 100
    List<SMGetElement<Object>> result1;
    List<SMGetElement<Object>> result2;
    List<SMGetElement<Object>> result3;
    List<SMGetElement<Object>> result4;

    try {
      future1 = mc.asyncBopSortMergeGet(keyList2, 20, 10, ElementFlagFilter.DO_NOT_FILTER, 5, 100);
      future2 = mc.asyncBopSortMergeGet(keyList2, 22, 20, ElementFlagFilter.DO_NOT_FILTER, 5, 100);
      future3 = mc.asyncBopSortMergeGet(keyList2, 50, 40, ElementFlagFilter.DO_NOT_FILTER, 0, 100);
      future4 = mc.asyncBopSortMergeGet(keyList2, 9, 5, ElementFlagFilter.DO_NOT_FILTER, 0, 100);
    } catch (IllegalStateException e) {
      Assert.fail(e.getMessage());
    }

    assertNotNull(future1);
    assertNotNull(future2);
    assertNotNull(future3);
    assertNotNull(future4);

    try {
      result1 = future1.get(1000L, TimeUnit.MILLISECONDS);
      result2 = future2.get(1000L, TimeUnit.MILLISECONDS);
      result3 = future3.get(1000L, TimeUnit.MILLISECONDS);
      result4 = future4.get(1000L, TimeUnit.MILLISECONDS);
      assertEquals(0, result1.size());
      assertEquals(0, result2.size());
      assertEquals(0, result3.size());
      assertEquals(0, result4.size());
      //for (SMGetElement<Object> each : result) {
      //	System.out.println(each.getKey() + " " + each.getBkey());
      //}

      /*
       * missed key
       */
      assertEquals(0, future1.getMissedKeys().size());
      assertEquals(0, future2.getMissedKeys().size());
      assertEquals(0, future3.getMissedKeys().size());
      assertEquals(0, future4.getMissedKeys().size());
      //for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {
      //	assertEquals("key0", m.getKey());
      //	assertEquals(CollectionResponse.OUT_OF_RANGE, m.getValue().getResponse());
      //}

      /*
       * response
       */
      assertEquals(CollectionResponse.TRIMMED, future1.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future2.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.END, future3.getOperationStatus().getResponse());
      assertEquals(CollectionResponse.OUT_OF_RANGE, future4.getOperationStatus().getResponse());
    } catch (InterruptedException e) {
      future1.cancel(true);
      future2.cancel(true);
      future3.cancel(true);
      future4.cancel(true);
      Assert.fail(e.getMessage());
    } catch (ExecutionException e) {
      future1.cancel(true);
      future2.cancel(true);
      future3.cancel(true);
      future4.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}