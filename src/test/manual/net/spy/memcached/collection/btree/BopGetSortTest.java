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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopGetSortTest extends BaseIntegrationTest {

  private String key = "BopGetSortTest";

  private Long[] items10 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    deleteBTree(key, items10);
    super.tearDown();
  }

  @Test
  public void testBopGet_Asc() throws Exception {
    // Create a list and add 10 items in it
    addToBTree(key, items10);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Get item with offset and index
    Map<Long, Element<Long>> rmap = mc.asyncBopGet(key, 0, 10,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10, rmap.size());
    assertEquals((Long) 10L, rmap.get(9L).getValue());

    // Check ordering
    assertTrue(rmap instanceof TreeMap);
    assertEquals(0L, ((TreeMap<Long, Element<Long>>) rmap).firstKey()
            .longValue());
    assertEquals(9L, ((TreeMap<Long, Element<Long>>) rmap).lastKey()
            .longValue());

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());

    // Get item with offset and index with default transcoder
    Map<Long, Element<Object>> rmap2 = mc.asyncBopGet(key, 0, 10,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rmap2.size());
  }

  @Test
  public void testBopGet_Desc() throws Exception {
    // Create a list and add 10 items in it
    addToBTree(key, items10);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Get item with offset and index
    Map<Long, Element<Long>> rmap = mc.asyncBopGet(key, 10, 0,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10, rmap.size());
    assertEquals((Long) 10L, rmap.get(9L).getValue());

    // Check ordering
    assertTrue(rmap instanceof TreeMap);
    assertEquals(9L, ((TreeMap<Long, Element<Long>>) rmap).firstKey()
            .longValue());
    assertEquals(0L, ((TreeMap<Long, Element<Long>>) rmap).lastKey()
            .longValue());

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());

    // Get item with offset and index with default transcoder
    Map<Long, Element<Object>> rmap2 = mc.asyncBopGet(key, 0, 10,
            ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rmap2.size());
  }

}
