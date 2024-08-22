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
package net.spy.memcached.collection.list;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LopOverflowActionTest extends BaseIntegrationTest {

  private String key = "LopOverflowActionTest";
  private List<String> keyList = new ArrayList<>();

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    keyList.add(key);
    mc.delete(key).get();
  }

  @Test
  public void testLopGet_Maxcount() throws Exception {
    // Test
    for (int maxcount = 100; maxcount <= 200; maxcount += 100) {
      // Create a list
      mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes()).get();

      // Set maxcount
      CollectionAttributes attrs = new CollectionAttributes();
      attrs.setMaxCount(maxcount);
      assertTrue(mc.asyncSetAttr(key, attrs).get(1000,
              TimeUnit.MILLISECONDS));

      for (int i = 1; i < maxcount; i++) {
        assertTrue(mc.asyncLopInsert(key, i, "item" + i, null).get(
                1000, TimeUnit.MILLISECONDS));
      }

      List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
              false).get(10000, TimeUnit.MILLISECONDS);
      assertEquals(maxcount, result.size());
      assertTrue(mc.asyncLopDelete(key, 0, 20000, true).get(1000,
              TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testLopGet_Overflow() throws Exception {
    // Create a List
    mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes()).get();

    int maxcount = 100;

    // Set maxcount to 100
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.error);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert more than maxcount
    for (int i = 1; i <= maxcount + 10; i++) {
      mc.asyncLopInsert(key, -1, "item" + i, null).get(1000,
              TimeUnit.MILLISECONDS);

    }

    List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
            false).get(10000, TimeUnit.MILLISECONDS);

    // result size should be maxsize(10000)
    assertEquals(maxcount, result.size());
    assertEquals("item0", result.get(0));
    assertEquals("item99", result.get(result.size() - 1));
    assertTrue(mc.asyncLopDelete(key, 0, 20000, true).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopGet_HeadTrim() throws Exception {
    // Create a List
    mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes()).get();

    int maxcount = 100;

    // Set maxcount to 10000
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.head_trim);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert more than maxcount
    for (int i = 1; i <= maxcount + 10; i++) {
      assertTrue(mc.asyncLopInsert(key, -1, "item" + i, null).get(1000,
              TimeUnit.MILLISECONDS));
    }

    List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
            false).get(10000, TimeUnit.MILLISECONDS);

    // result size should be maxsize(10000)
    assertEquals(maxcount, result.size());
    assertEquals("item11", result.get(0));
    assertEquals("item110", result.get(result.size() - 1));
    assertTrue(mc.asyncLopDelete(key, 0, 20000, true).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopGet_TailTrim() throws Exception {
    // Create a List
    mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes()).get();

    int maxcount = 100;

    // Set maxcount to 10000
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.tail_trim);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert more than maxcount
    for (int i = 1; i <= maxcount + 10; i++) {
      assertTrue(mc.asyncLopInsert(key, 0, "item" + i, null).get(1000,
              TimeUnit.MILLISECONDS));
    }

    List<Object> result = mc.asyncLopGet(key, 0, maxcount + 10, false,
            false).get(10000, TimeUnit.MILLISECONDS);

    // result size should be maxsize(10000)
    assertEquals(maxcount, result.size());
    assertEquals("item110", result.get(0));
    assertEquals("item11", result.get(result.size() - 1));
    assertTrue(mc.asyncLopDelete(key, 0, 20000, false).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopGet_HeadTrim_OutOfRange() throws Exception {
    // Create a set
    mc.asyncLopInsert(key, 1, "item1", new CollectionAttributes()).get();

    // head_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, 1L, CollectionOverflowAction.head_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // test
    assertFalse(mc.asyncLopInsert(key, 0, "item0", null).get(1000,
            TimeUnit.MILLISECONDS));

    mc.asyncLopDelete(key, 0, 10, false).get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testLopGet_TailTrim_OutOfRange() throws Exception {
    // Create a set
    mc.asyncLopInsert(key, 1, "item1", new CollectionAttributes()).get();

    // tail_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, 1L, CollectionOverflowAction.tail_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // test
    assertFalse(mc.asyncLopInsert(key, 2, "item2", null).get(1000,
            TimeUnit.MILLISECONDS));

    mc.asyncLopDelete(key, 0, 10, false).get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testLopGet_AvailableOverflowAction() throws Exception {
    // Create a set
    mc.asyncLopInsert(key, 0, "item0", new CollectionAttributes()).get();

    // Set OverflowAction
    // error
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.error))
            .get(1000, TimeUnit.MILLISECONDS));

    // head_trim
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // tail_trim
    assertTrue(mc.asyncSetAttr(key,
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

    mc.asyncLopDelete(key, 0, true).get(1000, TimeUnit.MILLISECONDS);
  }

}
