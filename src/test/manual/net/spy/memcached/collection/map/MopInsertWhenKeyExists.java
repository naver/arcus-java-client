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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MopInsertWhenKeyExists extends BaseIntegrationTest {

  private String key = "MopInsertWhenKeyExists";

  private Long[] items9 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.asyncMopDelete(key, true).get(1000, TimeUnit.MILLISECONDS);
    mc.delete(key).get();
    super.tearDown();
  }

  @Test
  public void testMopInsert_Normal() throws Exception {
    // Create a list and add it 9 items
    addToMap(key, items9);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert one item
    assertTrue(mc.asyncMopInsert(key, "10", 10L,
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS));

    // Check inserted item
    Map<String, Long> rmap = mc.asyncMopGet(key, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10, rmap.size());

    Long comp = rmap.get("10");
    assertEquals((Long) 10L, comp);

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());
  }

  @Test
  public void testMopInsert_SameField() throws Exception {
    // Create a list and add it 9 items
    addToMap(key, items9);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert an item same to the last item
    mc.asyncMopInsert(key, "8", 10L, new CollectionAttributes()).get(
            1000, TimeUnit.MILLISECONDS);

    // Check that item is inserted
    Map<String, Long> rmap = mc.asyncMopGet(key, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(9, rmap.size());
  }
}
