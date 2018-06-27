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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

public class SMGetWithEflagTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  List<String> keyList = null;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    try {
      mc.delete(KEY).get();
    } catch (Exception e) {

    }
    Assert.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      mc.delete(KEY).get();
    } catch (Exception e) {

    }
    super.tearDown();
  }

  public void testSMGetMissAll() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 1, 2,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertTrue(map.isEmpty());
      Assert.assertEquals(oldFuture.getMissedKeyList().size(), 10);
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 1, 2,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertTrue(map.isEmpty());
      Assert.assertEquals(future.getMissedKeys().size(), 10);
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testSMGetHitAll() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 50; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 50; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + i, map.get(i).getKey());
        Assert.assertEquals(i, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + i, map.get(i).getKey());
        Assert.assertEquals(i, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testSMGetHitAllWithOffsetMoreCount() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 50; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 50; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + (i + 1), map.get(i).getKey());
        Assert.assertEquals(i + 1, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + i, map.get(i).getKey());
        Assert.assertEquals(i, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testSMGetHitAllWithOffsetExactCount() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 10; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(9, map.size());
      Assert.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + (i + 1), map.get(i).getKey());
        Assert.assertEquals(i + 1, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + i, map.get(i).getKey());
        Assert.assertEquals(i, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testSMGetHitAllWithOffsetLessThanCount() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 9; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 9; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(8, map.size());
      Assert.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + (i + 1), map.get(i).getKey());
        Assert.assertEquals(i + 1, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(9, map.size());
      Assert.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        Assert.assertEquals(KEY + i, map.get(i).getKey());
        Assert.assertEquals(i, map.get(i).getBkey());
        Assert.assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testSMGetHitAllDesc() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 10; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertTrue(oldFuture.getMissedKeyList().isEmpty());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assert.assertEquals(10, map.size());
      Assert.assertTrue(future.getMissedKeys().isEmpty());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testSMGetHitHalf() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 5; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(oldFuture.getMissedKeyList().size(), 5);
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(future.getMissedKeys().size(), 5);
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testSMGetHitHalfDesc() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 5; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(oldFuture.getMissedKeyList().size(), 5);
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(future.getMissedKeys().size(), 5);
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testPerformanceGet1000KeysWithoutOffset() {
    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < 1000; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 1000; i++) {
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get(1000L,
                TimeUnit.MILLISECONDS);
      }
    } catch (TimeoutException e) {
      fail(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    long start = System.currentTimeMillis();

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 1000,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.MILLISECONDS);
      // System.out.println((System.currentTimeMillis() - start) + "ms");
    } catch (TimeoutException e) {
      oldFuture.cancel(true);
      fail(e.getMessage());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 1000,
                    ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.MILLISECONDS);
      // System.out.println((System.currentTimeMillis() - start) + "ms");
    } catch (TimeoutException e) {
      future.cancel(true);
      fail(e.getMessage());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public void testSMGetWithMassiveKeys() {
    int testSize = 100;

    try {
      keyList = new ArrayList<String>();
      for (int i = 0; i < testSize; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < testSize; i++) {
        if (i % 2 == 0) {
          continue;
        }
        mc.asyncBopInsert(KEY + i, i, "EFLAG".getBytes(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    long start = System.currentTimeMillis();

    /* old  SMGetWithEflagTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, testSize,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      // System.out.println(System.currentTimeMillis() - start + "ms");
      Assert.assertEquals(50, map.size());
      List<String> missed = oldFuture.getMissedKeyList();
      Assert.assertEquals(testSize / 2, missed.size());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, testSize,
                    ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      // System.out.println(System.currentTimeMillis() - start + "ms");
      Assert.assertEquals(50, map.size());
      Map<String, CollectionOperationStatus> missed = future.getMissedKeys();
      Assert.assertEquals(testSize / 2, missed.size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

}
