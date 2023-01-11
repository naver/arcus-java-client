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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.SMGetFuture;

public class ByteArrayBKeySMGetErrorTest extends BaseIntegrationTest {

  private static final List<String> KEY_LIST = new ArrayList<String>();

  static {
    String KEY = ByteArrayBKeySMGetErrorTest.class.getSimpleName()
            + new Random().nextLong();
    for (int i = 1; i <= 10; i++) {
      KEY_LIST.add(KEY + (i * 9));
    }
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
      Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0),
              new byte[]{(byte) 1}, null, "VALUE",
              new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1),
              new byte[]{(byte) 1}, null, "VALUE",
              new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1),
              new byte[]{(byte) 2}, null, "VALUE",
              new CollectionAttributes()).get());
    } catch (Exception e) {
      fail(e.getMessage());
    }

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(KEY_LIST, new byte[]{(byte) 0},
                    new byte[]{(byte) 10},
                    ElementFlagFilter.DO_NOT_FILTER, 0, 10);
    try {
      List<SMGetElement<Object>> map = oldFuture
              .get(1000L, TimeUnit.SECONDS);

      Assert.assertEquals(3, map.size());

      Assert.assertEquals("DUPLICATED", oldFuture.getOperationStatus()
              .getMessage());

    } catch (Exception e) {
      oldFuture.cancel(true);
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // sort merge get
    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = mc
            .asyncBopSortMergeGet(KEY_LIST, new byte[]{(byte) 0},
                    new byte[]{(byte) 10},
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
            .asyncBopSortMergeGet(KEY_LIST, new byte[]{(byte) 0},
                    new byte[]{(byte) 15},
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
            .asyncBopSortMergeGet(KEY_LIST, new byte[]{(byte) 0},
                    new byte[]{(byte) 15},
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
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0),
                new byte[]{(byte) i}, null, "VALUE", attr).get());
      }

      // not trimmed
      for (int i = 0; i < 9; i++) {
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1),
                new byte[]{(byte) i}, null, "VALUE", attr).get());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // sort merge get
    byte[] from = new byte[]{(byte) 20};
    byte[] to = new byte[]{(byte) 10};
    long count = 100;

    /* old SMGetErrorTest */
    SMGetFuture<List<SMGetElement<Object>>> oldFuture = mc
            .asyncBopSortMergeGet(KEY_LIST, from, to,
                    ElementFlagFilter.DO_NOT_FILTER, 0, (int) count);
    try {
      List<SMGetElement<Object>> map = oldFuture
              .get(1000L, TimeUnit.SECONDS);

      Assert.assertEquals(1, map.size());
      Assert.assertEquals("TRIMMED", oldFuture.getOperationStatus()
              .getMessage());
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
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(0),
                new byte[]{(byte) i}, null, "VALUE", attr).get());
      }

      // not trimmed
      for (int i = 0; i < 9; i++) {
        Assert.assertTrue(mc.asyncBopInsert(KEY_LIST.get(1),
                new byte[]{(byte) i}, null, "VALUE", attr).get());
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // sort merge get
    byte[] from = new byte[]{(byte) 10};
    byte[] to = new byte[]{(byte) 0};
    long count = 100;

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
}
