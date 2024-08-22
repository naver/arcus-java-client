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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ByteArrayBKeySMGetTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private List<String> keyList = null;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    assertNull(mc.asyncGetAttr(KEY).get());
  }

  @AfterEach
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

    byte[] from = new byte[]{(byte) 1};
    byte[] to = new byte[]{(byte) 2};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertTrue(map.isEmpty());
      assertEquals(oldFuture.getMissedKeyList().size(), 10);
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
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
  public void testSMGetHitAll() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }
      for (int i = 0; i < 50; i++) {
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 10};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) i}, map
                .get(i).getByteBkey()));
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) i}, map
                .get(i).getByteBkey()));
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
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
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 10};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + (i + 1), map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) (i + 1)},
                map.get(i).getByteBkey()));
        assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) i},
                map.get(i).getByteBkey()));
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
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
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 10};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(9, map.size());
      assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + (i + 1), map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) (i + 1)},
                map.get(i).getByteBkey()));
        assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) i},
                map.get(i).getByteBkey()));
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
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
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 10};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 1, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(8, map.size());
      assertTrue(oldFuture.getMissedKeyList().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + (i + 1), map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) (i + 1)},
                map.get(i).getByteBkey()));
        assertEquals("VALUE" + (i + 1), map.get(i).getValue());
      }
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      assertEquals(9, map.size());
      assertTrue(future.getMissedKeys().isEmpty());

      for (int i = 0; i < map.size(); i++) {
        assertEquals(KEY + i, map.get(i).getKey());
        assertTrue(Arrays.equals(new byte[]{(byte) i},
                map.get(i).getByteBkey()));
        assertEquals("VALUE" + i, map.get(i).getValue());
      }
    } catch (Exception e) {
      future.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
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
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 10};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(10, map.size());
      assertTrue(oldFuture.getMissedKeyList().isEmpty());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
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
  public void testSMGetHitHalf() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 5; i++) {
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 10};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(oldFuture.getMissedKeyList().size(), 5);
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
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
  public void testSMGetHitHalfDesc() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 5; i++) {
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 10};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      assertEquals(5, map.size());
      assertEquals(oldFuture.getMissedKeyList().size(), 5);
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 10, smgetMode);
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
  public void testPerformanceGet1000KeysWithoutOffset() {
    try {
      keyList = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        mc.delete(KEY + i).get();
        keyList.add(KEY + i);
      }

      for (int i = 0; i < 1000; i++) {
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    long start = System.currentTimeMillis();

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 1000};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.MILLISECONDS);
      // System.out.println("elapsed 1 "
      // + (System.currentTimeMillis() - start) + "ms");
      // System.out.println("result size=" + map.size());
    } catch (TimeoutException e) {
      oldFuture.cancel(true);
      fail(e.getMessage());
    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.MILLISECONDS);
      // System.out.println("elapsed 1 "
      // + (System.currentTimeMillis() - start) + "ms");
      // System.out.println("result size=" + map.size());
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
  public void testSMGetWithMassiveKeys() {
    int testSize = 100;

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
        mc.asyncBopInsert(KEY + i, new byte[]{(byte) i}, null,
                "VALUE" + i, new CollectionAttributes()).get();
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    long start = System.currentTimeMillis();

    byte[] from = new byte[]{(byte) 0};
    byte[] to = new byte[]{(byte) 100};

    /* old SMGetTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, 500);
    try {
      List<SMGetElement<Object>> map = oldFuture.get(1000L, TimeUnit.SECONDS);
      // System.out.println(System.currentTimeMillis() - start + "ms");
      assertEquals(50, map.size());
      List<String> missed = oldFuture.getMissedKeyList();
      assertEquals(testSize / 2, missed.size());
    } catch (Exception e) {
      oldFuture.cancel(true);
      fail(e.getMessage());
    }

    SMGetMode smgetMode = SMGetMode.UNIQUE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(keyList, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 500, smgetMode);
    try {
      List<SMGetElement<Object>> map = future.get(1000L, TimeUnit.SECONDS);
      // System.out.println(System.currentTimeMillis() - start + "ms");
      assertEquals(50, map.size());
      Map<String, CollectionOperationStatus> missed = future.getMissedKeys();
      assertEquals(testSize / 2, missed.size());
    } catch (Exception e) {
      future.cancel(true);
      fail(e.getMessage());
    }
  }
}
