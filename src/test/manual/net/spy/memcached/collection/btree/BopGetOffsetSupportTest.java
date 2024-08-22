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
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopGetOffsetSupportTest extends BaseIntegrationTest {

  private String key = "BopGetOffsetSupportTest";

  private Long[] items10 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    deleteBTree(key, items10);
    super.tearDown();
  }

  @Test
  public void testBopGetOffset_Normal() throws Exception {
    // Create a list and add 10 items in it
    addToBTree(key, items10);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Get item with offset and index
    Map<Long, Element<Long>> rmap = mc.asyncBopGet(key, 0, 10,
            ElementFlagFilter.DO_NOT_FILTER, 5, 10, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(5, rmap.size());
    assertEquals((Long) 10L, rmap.get(9L).getValue());

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());

    // Get item with offset and index with default transcoder
    Map<Long, Element<Object>> rmap2 = mc.asyncBopGet(key, 0, 10,
            ElementFlagFilter.DO_NOT_FILTER, 5, 10, false, false).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(5, rmap2.size());
  }

  @Test
  public void testBopGetOffset_More() throws Exception {
    // Create a list and add 10 items in it
    addToBTree(key, items10);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());

    // Get item with offset and index with default transcoder
    int offset = 0;
    Map<Long, Element<Object>> rmap = mc.asyncBopGet(key, 0, 10,
            ElementFlagFilter.DO_NOT_FILTER, offset, 10, false, false).get(
            1000, TimeUnit.MILLISECONDS);
    assertEquals(10, rmap.size());

    // offset should be >= 0, but the server doesn't care anyway
    offset = -1;
    rmap = mc.asyncBopGet(key, 0, 10, ElementFlagFilter.DO_NOT_FILTER,
            offset, 10, false, false).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10, rmap.size());

    // if offset > max index of b+tree
    offset = 10;
    rmap = mc.asyncBopGet(key, 0, 10, ElementFlagFilter.DO_NOT_FILTER,
            offset, 10, false, false).get(1000, TimeUnit.MILLISECONDS);
    assertNotNull(rmap);
  }

}
