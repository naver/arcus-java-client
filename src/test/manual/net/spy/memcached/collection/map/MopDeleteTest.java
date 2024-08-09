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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MopDeleteTest extends BaseIntegrationTest {

  private String key = "MopDeleteTest";

  private Long[] items9 = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L};

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
  public void testMopDelete_NoKey() throws Exception {
    assertFalse(mc.asyncMopDelete("no_key", false).get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testMopDelete_NoMkey() throws Exception {
    assertFalse(mc.asyncMopDelete(key, "11", false).get(1000, TimeUnit.MILLISECONDS));
  }

//  public void testMopDelete_oneNoMkeyList() throws Exception {
//    // Delete if key is in the map, Skip if key isn't in the map
//    List<String> mkeyList = Arrays.asList("3", "no_key");
//    assertTrue(mc.asyncMopDelete(key, mkeyList, false).get(1000, TimeUnit.MILLISECONDS));
//  }
//
//  public void testMopDelete_AllNoMkeyList() throws Exception {
//    // If all keys aren't in the map, return false
//    List<String> mkeyList = Arrays.asList("no_key1", "no_key2");
//    assertFalse(mc.asyncMopDelete(key, mkeyList, false).get(1000,
//            TimeUnit.MILLISECONDS));
//  }

  @Test
  public void testMopDelete_DeleteByBestEffort() throws Exception {
    // Delete items(2..11) in the map
    for (int i = 2; i < 12; i++) {
      mc.asyncMopDelete(key, String.valueOf(i), false).get(1000,
              TimeUnit.MILLISECONDS);
    }

    // Check that item is inserted
    Map<String, Long> rmap = mc.asyncMopGet(key, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    // By rule of 'best effort',
    // items(2..9) should be deleted
    assertEquals(2, rmap.size());
    assertEquals((Long) 0L, rmap.get("0"));
    assertEquals((Long) 1L, rmap.get("1"));
  }

  @Test
  public void testMopDelete_DeletedDropped() throws Exception {
    // Delete all items in the list
    assertTrue(mc.asyncMopDelete(key, true).get(1000, TimeUnit.MILLISECONDS));

    CollectionAttributes attrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertNull(attrs);
  }
}
