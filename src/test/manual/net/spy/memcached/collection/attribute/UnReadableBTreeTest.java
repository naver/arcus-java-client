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
package net.spy.memcached.collection.attribute;

import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class UnReadableBTreeTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String VALUE = "VALUE";
  private final long BKEY = 10L;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    assertNull(mc.asyncGetAttr(KEY).get());
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testCreateUnreadableBTreeTest() {
    try {
      // create unreadable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(false);

      Boolean insertResult = mc.asyncBopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      assertEquals(Long.valueOf(0), attr.getCount());
      assertEquals(Long.valueOf(4000), attr.getMaxCount());
      assertEquals(Integer.valueOf(0), attr.getExpireTime());
      assertFalse(attr.getReadable());

      // insert an item
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE, null)
              .get());

      // get an item
      CollectionFuture<Map<Long, Element<Object>>> f = mc.asyncBopGet(
              KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER, false, false);
      assertNull(f.get());
      assertEquals("UNREADABLE", f.getOperationStatus()
              .getMessage());

      // set readable
      attribute.setReadable(true);
      assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get an item again
      f = mc.asyncBopGet(KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
              false, false);
      Map<Long, Element<Object>> map = f.get();

      assertNotNull(map);
      assertEquals(VALUE, map.get(BKEY).getValue());
      assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateReadableBTreeTest() {
    try {
      // create readable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(true);

      Boolean insertResult = mc.asyncBopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      assertEquals(Long.valueOf(0), attr.getCount());
      assertEquals(Long.valueOf(4000), attr.getMaxCount());
      assertEquals(Integer.valueOf(0), attr.getExpireTime());
      assertTrue(attr.getReadable());

      // insert an item
      assertTrue(mc.asyncBopInsert(KEY, BKEY, null, VALUE, null)
              .get());

      // get an item
      CollectionFuture<Map<Long, Element<Object>>> f = mc.asyncBopGet(
              KEY, BKEY, ElementFlagFilter.DO_NOT_FILTER, false, false);

      Map<Long, Element<Object>> map = f.get();
      assertNotNull(map);
      assertEquals(VALUE, map.get(BKEY).getValue());
      assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

}
