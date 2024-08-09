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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementMultiFlagsFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SMGetTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private List<String> keyList = null;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assertions.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testSMGetMissAll() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 1, 2,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      Assertions.assertTrue(map.isEmpty());
      assertEquals(oldFuture.getMissedKeyList().size(), 10);
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 1, 2,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      Assertions.assertTrue(map.isEmpty());
      assertEquals(future.getMissedKeys().size(), 10);
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetHitAll() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 50; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      Assertions.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      Assertions.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetHitAllWithOffsetMoreCount() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 50; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      Assertions.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + (i + 1), map.get(i).getKey());
        assertEquals(i + 1, map.get(i).getBkey());
        assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      Assertions.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetHitAllWithOffsetExactCount() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 10; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(9, map.size());
      Assertions.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + (i + 1), map.get(i).getKey());
        assertEquals(i + 1, map.get(i).getBkey());
        assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      Assertions.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetHitAllWithOffsetLessThanCount() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 9; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 9; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(8, map.size());
      Assertions.assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + (i + 1), map.get(i).getKey());
        assertEquals(i + 1, map.get(i).getBkey());
        assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(9, map.size());
      Assertions.assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetHitAllDesc() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 10; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      Assertions.assertTrue(oldFuture.getMissedKeyList().isEmpty());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      Assertions.assertTrue(future.getMissedKeys().isEmpty());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetHitHalf() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 5; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();

      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
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
      Assertions.fail(e.getMessage());
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
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetHitHalfDesc() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 5; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetTest */
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
      Assertions.fail(e.getMessage());
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
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testPerformanceGet1000KeysWithoutOffset() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 1000; i++) {
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get(1000L,
                TimeUnit.MILLISECONDS);
      }
    } catch (TimeoutException e) {
      Assertions.fail(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    long start = System.currentTimeMillis();

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, 1000,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.MILLISECONDS);
      // System.out.println((System.currentTimeMillis() - start) + "ms");
    } catch (TimeoutException e) {
      oldFuture.cancel(true);
      Assertions.fail(e.getMessage());
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
      Assertions.fail(e.getMessage());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetWithMassiveKeys() {
    int testSize = 2000;

    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < testSize; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < testSize; i++) {
        if (i % 2 == 0) {
          continue;
        }
        mc.asyncBopInsert(KEY + i, i, null, "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    long start = System.currentTimeMillis();

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, testSize,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      // System.out.println(System.currentTimeMillis() - start + "ms");
      assertEquals(500, map.size());
      List<String> missed = oldFuture.getMissedKeyList();
      assertEquals(testSize / 2, missed.size());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, testSize,
                    ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      // System.out.println(System.currentTimeMillis() - start + "ms");
      assertEquals(500, map.size());
      Map<String, CollectionOperationStatus> missed = future.getMissedKeys();
      assertEquals(testSize / 2, missed.size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetWithElementMultiFlagsFilter() {
    int testSize = 2000;

    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < testSize; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < testSize; i++) {
        mc.asyncBopInsert(KEY + i, i, ByteBuffer.allocate(4).putInt(i).array(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    ElementMultiFlagsFilter multiFlagsFilter = new ElementMultiFlagsFilter();
    multiFlagsFilter.setCompOperand(ElementFlagFilter.CompOperands.Equal);
    for (int i = 0; i < 99; i++) {
      multiFlagsFilter.addCompValue(ByteBuffer.allocate(4).putInt(i).array());
    }
    multiFlagsFilter.addCompValue(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    /* old SMGet */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, 0, testSize, multiFlagsFilter, 0, 1000);

    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(99, map.size());
      assertEquals(0, oldFuture.getMissedKeyList().size());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, testSize, multiFlagsFilter, 1000, smgetMode);

    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(99, map.size());
      assertEquals(0, future.getMissedKeys().size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testByteArrayBkeySMGetWithElementMultiFlagsFilter() {
    int testSize = 2000;

    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < testSize; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < testSize; i++) {
        mc.asyncBopInsert(KEY + i, ByteBuffer.allocate(4).putInt(i).array(),
                ByteBuffer.allocate(4).putInt(i).array(), "VALUE" + i,
                new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    ElementMultiFlagsFilter multiFlagsFilter = new ElementMultiFlagsFilter();
    multiFlagsFilter.setCompOperand(ElementFlagFilter.CompOperands.Equal);
    for (int i = 0; i < 99; i++) {
      multiFlagsFilter.addCompValue(ByteBuffer.allocate(4).putInt(i).array());
    }
    multiFlagsFilter.addCompValue(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    /* old SMGet */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, new byte[]{0, 0, 0, 0},
                    ByteBuffer.allocate(4).putInt(testSize).array(),
                    multiFlagsFilter, 0, 1000);

    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(99, map.size());
      assertEquals(0, oldFuture.getMissedKeyList().size());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, new byte[]{0, 0, 0, 0},
                    ByteBuffer.allocate(4).putInt(testSize).array(),
                    multiFlagsFilter, 1000, smgetMode);

    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(99, map.size());
      assertEquals(0, future.getMissedKeys().size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testSMGetOverflowMaxCount() {
    try {
      mc.asyncBopSortMergeGet(keyList, 0, 1000,
              ElementFlagFilter.DO_NOT_FILTER, 0, 1001);
    } catch (IllegalArgumentException e) {
      return;
    }
    Assertions.fail("There's no IllegalArgumentException.");
  }
}
