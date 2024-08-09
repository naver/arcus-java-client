/*
 * arcus-java-client : Arcus Java client
 * Copyright 2016 JaM2in Co., Ltd.
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
package net.spy.memcached.collection.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MopOverflowActionTest extends BaseIntegrationTest {

  private String key = "MopOverflowActionTest";
  private List<String> keyList = new ArrayList<>();

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    keyList.add(key);
  }

  @Test
  public void testMopGet_Maxcount() throws Exception {
    // Test
    for (int maxcount = 100; maxcount <= 200; maxcount += 100) {
      // Create a map
      mc.asyncMopInsert(key, "0", "item0", new CollectionAttributes()).get();

      // Set maxcount
      CollectionAttributes attrs = new CollectionAttributes();
      attrs.setMaxCount(maxcount);
      assertTrue(mc.asyncSetAttr(key, attrs).get(1000,
              TimeUnit.MILLISECONDS));

      for (int i = 1; i < maxcount; i++) {
        assertTrue(mc.asyncMopInsert(key, String.valueOf(i), "item" + i, null).get(
                1000, TimeUnit.MILLISECONDS));
      }

      Map<String, Object> result = mc.asyncMopGet(key, false,
              false).get(10000, TimeUnit.MILLISECONDS);
      assertEquals(maxcount, result.size());
      assertTrue(mc.asyncMopDelete(key, true).get(1000,
              TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testMopGet_Overflow() throws Exception {
    // Create a map
    mc.asyncMopInsert(key, "0", "item0", new CollectionAttributes()).get();

    int maxcount = 100;

    // Set maxcount to 100
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.error);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert more than maxcount
    for (int i = 1; i <= maxcount + 10; i++) {
      mc.asyncMopInsert(key, String.valueOf(i), "item" + i, null).get(1000,
              TimeUnit.MILLISECONDS);

    }

    Map<String, Object> result = mc.asyncMopGet(key, false,
            false).get(10000, TimeUnit.MILLISECONDS);

    // result size should be maxsize(10000)
    assertEquals(maxcount, result.size());
    assertEquals("item0", result.get("0"));
    assertEquals("item99", result.get(String.valueOf(result.size() - 1)));
    assertTrue(mc.asyncMopDelete(key, true).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testMopGet_AvailableOverflowAction() throws Exception {
    // Create a set
    mc.asyncMopInsert(key, "0", "item0", new CollectionAttributes()).get();

    // Set OverflowAction
    // error
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.error))
            .get(1000, TimeUnit.MILLISECONDS));

    // head_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // tail_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.tail_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // smallest_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.smallest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // largest_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.largest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    mc.asyncMopDelete(key, true).get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testMopGet_notAvailableOverflowAction() {
    CollectionAttributes attributesForCreate = new CollectionAttributes();
    Map<String, Object> elem = new TreeMap<>();
    elem.put("0", "item0");

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.head_trim);
      mc.asyncMopInsert(key, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncMopPipedInsertBulk(key, elem, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncMopInsertBulk(keyList, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.tail_trim);
      mc.asyncMopInsert(key, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncMopPipedInsertBulk(key, elem, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncMopInsertBulk(keyList, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.smallest_trim);
      mc.asyncMopInsert(key, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncMopPipedInsertBulk(key, elem, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncMopInsertBulk(keyList, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.smallest_silent_trim);
      mc.asyncMopInsert(key, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncMopPipedInsertBulk(key, elem, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncMopInsertBulk(keyList, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.largest_trim);
      mc.asyncMopInsert(key, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncMopPipedInsertBulk(key, elem, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncMopInsertBulk(keyList, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.largest_silent_trim);
      mc.asyncMopInsert(key, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncMopPipedInsertBulk(key, elem, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncMopInsertBulk(keyList, "0", "item0", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }
}
