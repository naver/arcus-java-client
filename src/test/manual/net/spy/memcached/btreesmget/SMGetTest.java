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
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SMGetTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private List<String> keyList = null;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    assertNull(mc.asyncGetAttr(KEY).get());
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  void testSMGetMissAll() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 1, 2,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertTrue(map.isEmpty());
      assertEquals(future.getMissedKeys().size(), 10);
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetHitAll() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetHitAllMoreCount() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetHitAllExactCount() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetHitAllLessThanCount() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(9, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertEquals(i, map.get(i).getBkey());
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetHitAllDesc() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetHitHalf() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 10,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(future.getMissedKeys().size(), 5);
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetHitHalfDesc() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 10, 0,
                    ElementFlagFilter.DO_NOT_FILTER, 10, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(future.getMissedKeys().size(), 5);
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testPerformanceGet1000Keys() {
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
      fail(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, 1000,
                    ElementFlagFilter.DO_NOT_FILTER, 500, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      fail(e.getMessage());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetWithMassiveKeys() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, testSize,
                    ElementFlagFilter.DO_NOT_FILTER, 500, true);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(500, map.size());
      Map<String, CollectionOperationStatus> missed = future.getMissedKeys();
      assertEquals(testSize / 2, missed.size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetWithElementMultiFlagsFilter() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, 0, testSize, multiFlagsFilter, 1000, true);

    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(99, map.size());
      assertEquals(0, future.getMissedKeys().size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testByteArrayBkeySMGetWithElementMultiFlagsFilter() {
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

    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, new byte[]{0, 0, 0, 0},
                    ByteBuffer.allocate(4).putInt(testSize).array(),
                    multiFlagsFilter, 1000, true);

    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(99, map.size());
      assertEquals(0, future.getMissedKeys().size());
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testSMGetOverflowMaxCount() {
    try {
      mc.asyncBopSortMergeGet(keyList, 0, 1000,
              ElementFlagFilter.DO_NOT_FILTER, 1001, true);
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("There's no IllegalArgumentException.");
  }
}
