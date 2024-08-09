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
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LopInsertBoundary extends BaseIntegrationTest {

  private String key = "LopInsertBoundary";

  private Long[] items9 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();

    deleteList(key, 1000);
    addToList(key, items9);

    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    deleteList(key, 1000);
    super.tearDown();
  }

  @Test
  public void testLopInsert_IndexOutOfRange() throws Exception {
    assertFalse(mc.asyncLopInsert(key, 11, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopInsert_PrependTailTrim() throws Exception {
    // Default overflowaction for lists is 'tail_trim'

    // Insert an item
    assertTrue(mc.asyncLopInsert(key, 9, 10L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Prepend an item
    assertTrue(mc.asyncLopInsert(key, 0, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Check if the last item prepended, and the last one discarded
    List<Long> rlist = mc.asyncLopGet(key, 0, 100, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(10, rlist.size());
    assertEquals(11L, rlist.get(0).longValue());
    assertEquals(9L, rlist.get(9).longValue()); // tail_trimmed when
    // prepending
  }

  @Test
  public void testLopInsert_PrependHeadTrim() throws Exception {
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // Insert an item to make the list full
    assertTrue(mc.asyncLopInsert(key, 9, 10L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Prepend an item
    assertTrue(mc.asyncLopInsert(key, 0, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Check if the last item prepended, and the last one discarded
    List<Long> rlist = mc.asyncLopGet(key, 0, 100, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(10, rlist.size());
    assertEquals(11L, rlist.get(0).longValue());
    assertEquals(9L, rlist.get(9).longValue()); // tail_trimmed when
    // prepending
  }

  @Test
  public void testLopInsert_PrependOverflowError() throws Exception {
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.error))
            .get(1000, TimeUnit.MILLISECONDS));

    // Insert an item to make the list full
    mc.asyncLopInsert(key, 9, 10L, null).get(1000, TimeUnit.MILLISECONDS);

    // Prepend an item (FAILED)
    assertFalse(mc.asyncLopInsert(key, 0, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopInsert_AppendTailTrim() throws Exception {
    // Default overflowaction for lists is 'tail_trim'

    // Insert an item
    assertTrue(mc.asyncLopInsert(key, 9, 10L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Prepend an item
    assertTrue(mc.asyncLopInsert(key, -1, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Check if the last item prepended, and the last one discarded
    List<Long> rlist = mc.asyncLopGet(key, 0, 100, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(10, rlist.size());
    assertEquals(2L, rlist.get(0).longValue()); // head_trimmed when
    // appending
    assertEquals(11L, rlist.get(9).longValue());
  }

  @Test
  public void testLopInsert_AppendHeadTrim() throws Exception {
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // Insert an item to make the list full
    assertTrue(mc.asyncLopInsert(key, 9, 10L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Prepend an item
    assertTrue(mc.asyncLopInsert(key, -1, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Check if the last item prepended, and the last one discarded
    List<Long> rlist = mc.asyncLopGet(key, 0, 100, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(10, rlist.size());
    assertEquals(2L, rlist.get(0).longValue()); // head_trimmed when
    // appending
    assertEquals(11L, rlist.get(9).longValue());
  }

  @Test
  public void testLopInsert_AppendOverflowError() throws Exception {
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.error))
            .get(1000, TimeUnit.MILLISECONDS));

    // Insert an item to make the list full
    mc.asyncLopInsert(key, 9, 10L, null).get(1000, TimeUnit.MILLISECONDS);

    // Prepend an item (FAILED)
    assertFalse(mc.asyncLopInsert(key, -1, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopInsert_InsertTailTrim() throws Exception {
    // Default overflowaction for lists is 'tail_trim'

    // Insert an item
    assertTrue(mc.asyncLopInsert(key, 9, 10L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Insert an item
    assertTrue(mc.asyncLopInsert(key, 5, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Check if the last item prepended, and the last one discarded
    List<Long> rlist = mc.asyncLopGet(key, 0, 100, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(10, rlist.size());
    assertEquals(1L, rlist.get(0).longValue());
    assertEquals(9L, rlist.get(9).longValue()); // tail_trimmed
  }

  @Test
  public void testLopInsert_InsertHeadTrim() throws Exception {
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // Insert an item to make the list full
    assertTrue(mc.asyncLopInsert(key, 9, 10L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Insert an item
    assertTrue(mc.asyncLopInsert(key, 5, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));

    // Check if the last item prepended, and the last one discarded
    List<Long> rlist = mc.asyncLopGet(key, 0, 100, false, false,
            new LongTranscoder()).get(1000, TimeUnit.MILLISECONDS);

    assertEquals(10, rlist.size());
    assertEquals(2L, rlist.get(0).longValue()); // head_trimmed
    assertEquals(10L, rlist.get(9).longValue());
  }

  @Test
  public void testLopInsert_InsertOverflowError() throws Exception {
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.error))
            .get(1000, TimeUnit.MILLISECONDS));

    // Insert an item to make the list full
    mc.asyncLopInsert(key, 9, 10L, null).get(1000, TimeUnit.MILLISECONDS);

    // Prepend an item (FAILED)
    assertFalse(mc.asyncLopInsert(key, 0, 11L, null).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopInsert_SetMaxCountUnderCurrentSize() throws Exception {
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.error))
            .get(1000, TimeUnit.MILLISECONDS));
  }

}
