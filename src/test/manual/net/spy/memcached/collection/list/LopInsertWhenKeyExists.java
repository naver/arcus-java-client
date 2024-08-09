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

import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LopInsertWhenKeyExists extends BaseIntegrationTest {

  private String key = "LopInsertWhenKeyExists";

  private Long[] items8 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L};
  private Long[] items9 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    deleteList(key, 1000);
    super.tearDown();
  }

  @Test
  public void testLopInsert_Normal() throws Exception {
    // Create a list and add it 9 items
    addToList(key, items9);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert one item at index 1
    assertTrue(mc.asyncLopInsert(key, 1, 10L, new CollectionAttributes())
            .get(1000, TimeUnit.MILLISECONDS));

    // Check inserted item
    List<Long> rlist = mc.asyncLopGet(key, 0, 10, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10L, rlist.get(1).longValue());

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());
  }

  @Test
  public void testLopInsert_SameItem() throws Exception {
    // Create a list and add it 8 items
    addToList(key, items8);

    // Set maxcount to 10
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // Insert 2 items at index 1
    assertTrue(mc.asyncLopInsert(key, 1, 9L, new CollectionAttributes())
            .get(1000, TimeUnit.MILLISECONDS));
    assertTrue(mc.asyncLopInsert(key, 1, 10L, new CollectionAttributes())
            .get(1000, TimeUnit.MILLISECONDS));

    // Check inserted items
    List<Long> rlist = mc.asyncLopGet(key, 0, 10, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(10L, rlist.get(1).longValue());
    assertEquals(9L, rlist.get(2).longValue());

    // Check list attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertEquals(10, rattrs.getCount().intValue());
  }

}
