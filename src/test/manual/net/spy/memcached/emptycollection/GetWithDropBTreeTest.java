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
package net.spy.memcached.emptycollection;

import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class GetWithDropBTreeTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10L;
  private final int VALUE = 1234567890;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();

    boolean insertResult = mc.asyncBopInsert(KEY, BKEY, null, VALUE,
            new CollectionAttributes()).get();
    assertTrue(insertResult);
  }

  @Test
  void testGetWithoutDeleteAndDrop() {
    try {
      // check attr
      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=false, drop=true
      assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());

      // check exists
      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value again
      assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      false, false).get().get(BKEY).getValue());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testGetWithDeleteAndWithoutDrop() {
    try {
      // check attr
      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      true, false).get().get(BKEY).getValue());

      // check exists empty btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      assertNotNull(attr);
      assertEquals(Long.valueOf(0), attr.getCount());

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      assertNotNull(map);
      assertTrue(map.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testGetWithDeleteAndWithDrop() {
    try {
      // check attr
      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      assertEquals(
              VALUE,
              mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
                      true, true).get().get(BKEY).getValue());

      // check btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      assertNull(attr);

      Map<Long, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
              ElementFlagFilter.DO_NOT_FILTER, false, false).get();
      assertNull(map);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
