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
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopInsertWhenKeyExists extends BaseIntegrationTest {

  private String key = "BopInsertWhenKeyExists";

  private Long[] items9 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.asyncBopDelete(key, 0, 4000, ElementFlagFilter.DO_NOT_FILTER, 0,
            true).get(1000, TimeUnit.MILLISECONDS);
    mc.delete(key).get();
    super.tearDown();
  }

  @Test
  public void testBopInsert_unreadable_largestTrim() throws Exception {
    // insert with unreadable
    CollectionAttributes attr = new CollectionAttributes();
    attr.setReadable(false);
    attr.setOverflowAction(CollectionOverflowAction.largest_trim);

    // insert
    Boolean result = mc.asyncBopInsert(key, 0L, null, "value", attr).get();
    assertTrue(result);

    // get attr
    CollectionAttributes attr2 = mc.asyncGetAttr(key).get();
    Assertions.assertFalse(attr2.getReadable());
    assertEquals(CollectionOverflowAction.largest_trim,
            attr2.getOverflowAction());

    // get element
    CollectionFuture<Map<Long, Element<Object>>> future = mc.asyncBopGet(
            key, 0L, ElementFlagFilter.DO_NOT_FILTER, false, false);
    Assertions.assertNull(future.get());

    assertEquals(CollectionResponse.UNREADABLE, future
            .getOperationStatus().getResponse());
  }

  @Test
  public void testBopInsert_Normal() throws Exception {
    // Create a list and add it 9 items
    addToBTree(key, items9);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert one item
    assertTrue(mc.asyncBopInsert(key, 20, null, 10L,
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS));

    // Check inserted item
    Map<Long, Element<Long>> rmap = mc.asyncBopGet(key, 0, 100,
            ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10, rmap.size());
    assertEquals((Long) 10L, rmap.get(20L).getValue());

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());
  }

  @Test
  public void testBopInsert_SameItem() throws Exception {
    // Create a list and add it 9 items
    addToBTree(key, items9);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert an item same to the last item
    mc.asyncBopInsert(key, 10, null, 9L, new CollectionAttributes()).get(
            1000, TimeUnit.MILLISECONDS);

    // Check that item is inserted
    Map<Long, Element<Long>> rmap = mc.asyncBopGet(key, 0, 100,
            ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10, rmap.size());
  }

  @Test
  public void testBopInsert_SameBkey() throws Exception {
    // Create a list and add it 9 items
    addToBTree(key, items9);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert an item same to the last item
    mc.asyncBopInsert(key, 8, null, 10L, new CollectionAttributes()).get(
            1000, TimeUnit.MILLISECONDS);

    // Check that item is inserted
    Map<Long, Element<Long>> rmap = mc.asyncBopGet(key, 0, 100,
            ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(9, rmap.size());
  }

}
