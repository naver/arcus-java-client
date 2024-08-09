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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopOverflowActionTest extends BaseIntegrationTest {

  private String key = "BopGetBoundaryTest";
  private List<String> keyList = new ArrayList<>();

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    keyList.add(key);
    mc.delete(key).get();
      // mc.asyncBopDelete(key, 0, 20000, ElementFlagFilter.DO_NOT_FILTER,
      // 0, true).get(1000, TimeUnit.MILLISECONDS);
  }

//  public void testBopGet_Maxcount() throws Exception {
//    // Test
//    for (int maxcount = 100; maxcount <= 300; maxcount += 100) {
//      // Create a B+ Tree
//      mc.asyncBopInsert(key, 0, null, "item0", new CollectionAttributes());
//
//      // Set maxcount
//      CollectionAttributes attrs = new CollectionAttributes();
//      attrs.setMaxCount(maxcount);
//      assertTrue(mc.asyncSetAttr(key, attrs).get(1000,
//              TimeUnit.MILLISECONDS));
//
//      for (int i = 1; i <= maxcount; i++) {
//        mc.asyncBopInsert(key, i, null, "item" + i,
//                new CollectionAttributes()).get();
//      }
//
//      Map<Long, Element<Object>> result = mc.asyncBopGet(key, 0,
//              maxcount + 1000, ElementFlagFilter.DO_NOT_FILTER, 0,
//              maxcount + 1000).get(10000,
//              TimeUnit.MILLISECONDS);
//      assertEquals(maxcount, result.size());
//      assertTrue(mc.asyncBopDelete(key, 0, 20000,
//              ElementFlagFilter.DO_NOT_FILTER, 0, false).get(1000,
//              TimeUnit.MILLISECONDS));
//    }
//  }

  @Test
  public void testBopGet_Overflow() throws Exception {
    // Create a B+ Tree
    mc.asyncBopInsert(key, 0, null, "item0", new CollectionAttributes()).get();

    int maxcount = 100;

    // Set maxcount to 10000
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.error);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert more than maxcount
    for (int i = 1; i <= maxcount + 10; i++) {
      mc.asyncBopInsert(key, i, null, "item" + i, null).get(1000,
              TimeUnit.MILLISECONDS);
    }

    Map<Long, Element<Object>> result = mc.asyncBopGet(key, 0,
            maxcount + 10, ElementFlagFilter.DO_NOT_FILTER, 0,
            maxcount + 1000, false, false)
            .get(10000, TimeUnit.MILLISECONDS);

    // result size should be maxsize(10000)
    assertEquals(maxcount, result.size());
    assertTrue(result instanceof TreeMap);
    assertEquals(0L, ((TreeMap<Long, Element<Object>>) result).firstKey()
            .longValue());
    assertEquals(99L, ((TreeMap<Long, Element<Object>>) result).lastKey()
            .longValue());
    assertTrue(mc.asyncBopDelete(key, 0, 20000,
            ElementFlagFilter.DO_NOT_FILTER, 0, false).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testBopGet_LargestTrim() throws Exception {
    // Create a B+ Tree
    mc.asyncBopInsert(key, 0, null, "item0", new CollectionAttributes()).get();

    int maxcount = 100;

    // Set maxcount to 10000
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.largest_trim);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert more than maxcount
    for (int i = maxcount + 10; i >= 1; i--) {
      assertTrue(mc.asyncBopInsert(key, i, null, "item" + i, null).get(
              1000, TimeUnit.MILLISECONDS));
    }

    Map<Long, Element<Object>> result = mc.asyncBopGet(key, 0,
            maxcount + 10, ElementFlagFilter.DO_NOT_FILTER, 0,
            maxcount + 1000, false, false)
            .get(10000, TimeUnit.MILLISECONDS);

    // result size should be maxsize(10000)
    assertEquals(100, result.size());
    assertTrue(result instanceof TreeMap);
    assertEquals(0L, ((TreeMap<Long, Element<Object>>) result).firstKey()
            .longValue());
    assertEquals(99L, ((TreeMap<Long, Element<Object>>) result).lastKey()
            .longValue());
    assertTrue(mc.asyncBopDelete(key, 0, 20000,
            ElementFlagFilter.DO_NOT_FILTER, 0, false).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testBopGet_SmallestTrim() throws Exception {
    // Create a B+ Tree
    mc.asyncBopInsert(key, 0, null, "item0", new CollectionAttributes()).get();

    int maxcount = 100;

    // Set maxcount to 10000
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert more than maxcount
    for (int i = 1; i <= maxcount + 10; i++) {
      assertTrue(mc.asyncBopInsert(key, i, null, "item" + i, null).get(
              1000, TimeUnit.MILLISECONDS));
    }

    Map<Long, Element<Object>> result = mc.asyncBopGet(key, 0,
            maxcount + 10, ElementFlagFilter.DO_NOT_FILTER, 0,
            maxcount + 1000, false, false)
            .get(10000, TimeUnit.MILLISECONDS);

    // result size should be maxsize(10000)
    assertEquals(100, result.size());
    assertTrue(result instanceof TreeMap);
    assertEquals(11L, ((TreeMap<Long, Element<Object>>) result).firstKey()
            .longValue());
    assertEquals(110L, ((TreeMap<Long, Element<Object>>) result).lastKey()
            .longValue());
    assertTrue(mc.asyncBopDelete(key, 0, 20000,
            ElementFlagFilter.DO_NOT_FILTER, 0, false).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testBopGet_SmallestTrim_OutOfRange() throws Exception {
    // Create a set
    mc.asyncBopInsert(key, 1, null, "item1", new CollectionAttributes()).get();

    // smallest_trim
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, 1L, CollectionOverflowAction.smallest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // test
    assertFalse(mc.asyncBopInsert(key, 0, null, "item0",
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS));

    mc.asyncBopDelete(key, 0, 10, ElementFlagFilter.DO_NOT_FILTER, 0, false)
            .get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testBopGet_LargestTrim_OutOfRange() throws Exception {
    // Create a set
    mc.asyncBopInsert(key, 1, null, "item1", new CollectionAttributes()).get();

    // largest_trim
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, 1L, CollectionOverflowAction.largest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // test
    assertFalse(mc.asyncBopInsert(key, 2, null, "item2", null).get(1000,
            TimeUnit.MILLISECONDS));

    mc.asyncBopDelete(key, 0, 10, ElementFlagFilter.DO_NOT_FILTER, 0, false)
            .get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testBopGet_AvailableOverflowAction() throws Exception {
    // Create a set
    mc.asyncBopInsert(key, 0, null, "item0", new CollectionAttributes()).get();

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
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.smallest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // largest_trim
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.largest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    mc.asyncBopDelete(key, 0, ElementFlagFilter.DO_NOT_FILTER, false).get(
            1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testBopGet_notAvailableOverflowAction() {
    CollectionAttributes attributesForCreate = new CollectionAttributes();

    // create
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.head_trim);
      mc.asyncBopCreate(key, ElementValueType.STRING, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      mc.asyncBopInsert(key, 1, null, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncBopPipedInsertBulk(key, new HashMap<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncBopInsertBulk(keyList, 1, null, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // create
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.tail_trim);
      mc.asyncBopCreate(key, ElementValueType.STRING, attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      mc.asyncBopInsert(key, 1, null, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncBopPipedInsertBulk(key, new HashMap<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncBopInsertBulk(keyList, 1, null, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }
}
