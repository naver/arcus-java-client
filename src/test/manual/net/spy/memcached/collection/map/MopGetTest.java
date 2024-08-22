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
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MopGetTest extends BaseIntegrationTest {

  private String key = "MopGetTest";

  private Long[] items9 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();

    deleteMap(key);
    addToMap(key, items9);

    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    deleteMap(key);
    super.tearDown();
  }

  @Test
  public void testMopGet_NoKey() throws Exception {
    Map<String, Object> rmap = mc.asyncMopGet("no_key", false, false).get(
            1000, TimeUnit.MILLISECONDS);

    // We've got an empty list
    assertNull(rmap);
  }

  @Test
  public void testMopGet_NoMkey() throws Exception {
    Map<String, Object> map = mc.asyncMopGet(key, "20", false, false).get(1000,
            TimeUnit.MILLISECONDS);
    assertNotNull(map);
    assertTrue(map.isEmpty());
  }

  @Test
  public void testMopGet_GetByBestEffort() throws Exception {
    // Retrieve items(2..11) in the list
    List<String> mkeyList = new ArrayList<>();
    for (int i = 2; i < 12; i++) {
      mkeyList.add(String.valueOf(i));
    }
    Map<String, Object> rmap = mc.asyncMopGet(key, mkeyList, false, false).get(1000,
            TimeUnit.MILLISECONDS);

    // By rule of 'best effort',
    // items(2..9) should be retrieved
    assertEquals(7, rmap.size());
    for (int i = 0; i < rmap.size(); i++) {
      assertTrue(rmap.containsValue(items9[i + 2]));
    }
  }

  @Test
  public void testMopGet_GetWithDeletion() throws Exception {
    CollectionAttributes attrs = null;
    Map<String, Object> rmap = null;
    List<String> mkeyList = new ArrayList<>();

    // Retrieve items(0..5) in the list with delete option
    for (int i = 0; i < 6; i++) {
      mkeyList.add(String.valueOf(i));
    }
    rmap = mc.asyncMopGet(key, mkeyList, true, false).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(6, rmap.size());
    mkeyList.clear();

    // Check the remaining item count in the list
    attrs = mc.asyncGetAttr(key).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(3, attrs.getCount().intValue());

    // Retrieve items(6..8) in the list with delete option
    for (int i = 6; i < 9; i++) {
      mkeyList.add(String.valueOf(i));
    }
    rmap = mc.asyncMopGet(key, mkeyList, true, true).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(3, rmap.size());

    // Now our list has no items and would be deleted
    rmap = mc.asyncMopGet(key, true, false).get(1000,
            TimeUnit.MILLISECONDS);
    assertNull(rmap);
  }

//  public void testMopGet_simple() throws Exception {
//    CollectionAttributes attrs = null;
//
//    List<String> mKeyList = new ArrayList<String>();
//    for (int i = 0; i <= 9; i++) {
//      mKeyList.add(String.valueOf(i));
//    }
//
//    // get all elements
//    Map<String, Object> list = mc.asyncMopGet(key, mKeyList).get(1000,
//            TimeUnit.MILLISECONDS);
//
//    assertEquals(list.size(), 9);
//
//    // get count of the map and compare with count of items after using get method
//    attrs = mc.asyncGetAttr(key).get(1000, TimeUnit.MILLISECONDS);
//    assertEquals(9, attrs.getCount().intValue());
//  }
}
