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
package net.spy.memcached.btreesmget;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.SMGetFuture;

public class SMGetErrorTest extends BaseIntegrationTest {

  private static final List<String> KEY_LIST = new ArrayList<String>();

  static {
    String KEY = SMGetErrorTest.class.getSimpleName()
            + new Random().nextLong();
    for (int i = 1; i <= 10; i++)
      KEY_LIST.add(KEY + (i * 9));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    for (String KEY : KEY_LIST) {
      mc.delete(KEY).get();
      Assert.assertNull(mc.asyncGetAttr(KEY).get());
    }
  }

  @Override
  protected void tearDown() throws Exception {
    for (String KEY : KEY_LIST) {
      mc.delete(KEY).get();
    }
    super.tearDown();
  }

  public void testDuplicated() {
    // insert test data
    try {
      Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0), 1, null,
              "VALUE", new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1), 1, null,
              "VALUE", new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1), 2, null,
              "VALUE", new CollectionAttributes()).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(KEY_LIST, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(3, map.size());
      Assert.assertEquals("DUPLICATED", oldFuture.getOperationStatus().getMessage());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // sort merge get
    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(KEY_LIST, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(3, map.size());
      Assert.assertEquals("DUPLICATED", future.getOperationStatus().getMessage());

    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testBkeyMismatch() {
    // insert test data
    try {
      CollectionAttributes attr = new CollectionAttributes();
      attr.setMaxCount(20);

      mc.asyncBopCreate(KEY_LIST.get(0), ElementValueType.STRING, attr)
              .get();

      for (int i = 0; i < 20; i++) {
        // trimmed
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0),
                new byte[]{(byte) i}, null, "VALUE", attr).get());

        // not trimmed
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1), i, null,
                "VALUE", new CollectionAttributes()).get());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(KEY_LIST, 0, 15,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 20);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(0, map.size());
      Assert.assertEquals("BKEY_MISMATCH", oldFuture.getOperationStatus().getMessage());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // sort merge get
    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(KEY_LIST, 0, 15,
                    ElementFlagFilter.DO_NOT_FILTER, 20, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(0, map.size());
      Assert.assertEquals("BKEY_MISMATCH", future.getOperationStatus().getMessage());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testTrimmed() {
    // insert test data
    try {
      CollectionAttributes attr = new CollectionAttributes();
      attr.setMaxCount(10);
      attr.setOverflowAction(CollectionOverflowAction.smallest_trim);

      mc.asyncBopCreate(KEY_LIST.get(0), ElementValueType.STRING, attr)
              .get();

      mc.asyncBopCreate(KEY_LIST.get(1), ElementValueType.STRING, attr)
              .get();

      for (int i = 0; i < 30; i++) {
        // trimmed
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0), i, null,
                "VALUE", attr).get());
      }

      // not trimmed
      for (int i = 0; i < 9; i++) {
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1), i, null,
                "VALUE", attr).get());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // display current bkey list
    try {
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY_LIST.get(0), 0,
              10000, ElementFlagFilter.DO_NOT_FILTER, 0, 10000, false,
              false).get();
      // System.out.println(KEY_LIST.get(0) + " => ");
      // for (Entry<Long, Object> entry : map.entrySet()) {
      // System.out.print(entry.getKey());
      // System.out.print(" , ");
      // }
      // System.out.println("");

      map = mc.asyncBopGet(KEY_LIST.get(1), 0, 10000,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10000, false, false)
              .get();
      // System.out.println(KEY_LIST.get(1) + " => ");
      // for (Entry<Long, Object> entry : map.entrySet()) {
      // System.out.print(entry.getKey());
      // System.out.print(" , ");
      // }
      // System.out.println("");

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // sort merge get
    long from = 20;
    long to = 10;
    long count = from - to;

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(KEY_LIST, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, (int) count);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(1, map.size());
      Assert.assertEquals("TRIMMED", oldFuture.getOperationStatus().getMessage());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(KEY_LIST, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, (int) count, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(1, map.size());
      Assert.assertEquals(1, future.getTrimmedKeys().size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testOutOfRange() {
    // insert test data
    try {
      CollectionAttributes attr = new CollectionAttributes();
      attr.setMaxCount(10);
      attr.setOverflowAction(CollectionOverflowAction.smallest_trim);

      mc.asyncBopCreate(KEY_LIST.get(0), ElementValueType.STRING, attr)
              .get();

      mc.asyncBopCreate(KEY_LIST.get(1), ElementValueType.STRING, attr)
              .get();

      for (int i = 0; i < 30; i++) {
        // trimmed
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0), i, null,
                "VALUE", attr).get());
      }

      // not trimmed
      for (int i = 0; i < 9; i++) {
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1), i, null,
                "VALUE", attr).get());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // display current bkey list
    try {
      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY_LIST.get(0), 0,
              10000, ElementFlagFilter.DO_NOT_FILTER, 0, 10000, false,
              false).get();
      // System.out.println(KEY_LIST.get(0) + " => ");
      // for (Entry<Long, Object> entry : map.entrySet()) {
      // System.out.print(entry.getKey());
      // System.out.print(" , ");
      // }
      // System.out.println("");

      map = mc.asyncBopGet(KEY_LIST.get(1), 0, 10000,
              ElementFlagFilter.DO_NOT_FILTER, 0, 10000, false, false)
              .get();
      // System.out.println(KEY_LIST.get(1) + " => ");
      // for (Entry<Long, Object> entry : map.entrySet()) {
      // System.out.print(entry.getKey());
      // System.out.print(" , ");
      // }
      // System.out.println("");

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // sort merge get
    long from = 10;
    long to = 0;
    long count = from - to;

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(KEY_LIST, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, (int) count);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(0, map.size());
      Assert.assertEquals("OUT_OF_RANGE", oldFuture.getOperationStatus().getMessage());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(KEY_LIST, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, (int) count, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(9, map.size());
      Assert.assertEquals("END", future.getOperationStatus().getMessage());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testDuplicated2() {
    // insert test data
    try {
      Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0), 1, null,
              "VALUE", new CollectionAttributes()).get());

      for (int bkey = 0; bkey < KEY_LIST.size() - 1; bkey++) {
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(bkey), bkey,
                null, "VALUE", new CollectionAttributes()).get());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(KEY_LIST, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertEquals("DUPLICATED", oldFuture.getOperationStatus().getMessage());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // sort merge get
    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(KEY_LIST, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertEquals("DUPLICATED", future.getOperationStatus().getMessage());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testUnreadable() {
    // insert test data
    try {
      CollectionAttributes attr = new CollectionAttributes();
      attr.setReadable(false);

      mc.asyncBopCreate(KEY_LIST.get(0), ElementValueType.STRING, attr)
              .get();

      mc.asyncBopInsert(KEY_LIST.get(0), 0, null, "V", attr).get();
      mc.asyncBopInsert(KEY_LIST.get(0), 1, null, "V", attr).get();

      mc.asyncBopInsert(KEY_LIST.get(1), 0, null, "V", attr).get();
      mc.asyncBopInsert(KEY_LIST.get(1), 1, null, "V", attr).get();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    ArrayList<String> testKeyList = new ArrayList<String>();
    testKeyList.add(KEY_LIST.get(0));
    testKeyList.add(KEY_LIST.get(1));

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(testKeyList, 10, 0, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(0, map.size());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // sort merge get
    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(testKeyList, 10, 0, ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(0, map.size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testInvalidArgumentException() {
    ArrayList<String> testKeyList = new ArrayList<String>();
    testKeyList.add(KEY_LIST.get(0));
    testKeyList.add(KEY_LIST.get(1));

    // insert test data
    try {
      CollectionAttributes attr = new CollectionAttributes();

      mc.delete(KEY_LIST.get(0)).get();
      mc.delete(KEY_LIST.get(1)).get();

      mc.asyncBopCreate(KEY_LIST.get(0), ElementValueType.STRING, attr).get();
      mc.asyncBopCreate(KEY_LIST.get(1), ElementValueType.STRING, attr).get();

      mc.asyncBopInsert(KEY_LIST.get(0), 0, null, "V", attr).get();
      mc.asyncBopInsert(KEY_LIST.get(0), 2, null, "V", attr).get();

      mc.asyncBopInsert(KEY_LIST.get(1), 1, null, "V", attr).get();
      mc.asyncBopInsert(KEY_LIST.get(1), 3, null, "V", attr).get();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // keylist is null
    try {
      mc.asyncBopSortMergeGet(null, 10, 0, ElementFlagFilter.DO_NOT_FILTER, -1, 10);
      fail("This should be an exception");
    } catch (Exception e) {
      assertEquals("Key list is empty.", e.getMessage());
    }

    // keylist is empty
    try {
      mc.asyncBopSortMergeGet(new ArrayList<String>(), 10, 0, ElementFlagFilter.DO_NOT_FILTER, -1, 10);
      fail("This should be an exception");
    } catch (Exception e) {
      assertEquals("Key list is empty.", e.getMessage());
    }

    // offset < 0
    try {
      mc.asyncBopSortMergeGet(testKeyList, 10, 0, ElementFlagFilter.DO_NOT_FILTER, -1, 10);
      fail("This should be an exception");
    } catch (Exception e) {
      assertEquals("Offset must be 0 or positive integer.", e.getMessage());
    }

    // count == 0
    try {
      mc.asyncBopSortMergeGet(testKeyList, 10, 0, ElementFlagFilter.DO_NOT_FILTER, 0, 0);
      fail("This should be an exception");
    } catch (Exception e) {
      assertEquals("Count must be larger than 0.", e.getMessage());
    }

    // offset + count > 1000
    try {
      mc.asyncBopSortMergeGet(testKeyList, 10, 0, ElementFlagFilter.DO_NOT_FILTER, 0, 1001);
      fail("This should be an exception");
    } catch (Exception e) {
      assertEquals("The sum of offset and count must not exceed a maximum of 1000.", e.getMessage());
    }

    // duplicate keys
    try {
      // add duplicate key to testKeyList
      testKeyList.add(KEY_LIST.get(1));
      mc.asyncBopSortMergeGet(testKeyList, 10, 0, ElementFlagFilter.DO_NOT_FILTER, 0, 10);
      fail("This should be an exception");
    } catch (Exception e) {
      assertEquals("Duplicate keys exist in key list.", e.getMessage());
    }
  }
}
