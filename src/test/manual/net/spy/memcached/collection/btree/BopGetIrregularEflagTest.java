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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class BopGetIrregularEflagTest extends BaseIntegrationTest {

  private final String key = "BopGetIrregularEflagTest";

  private final byte[] eFlag = {1};

  private final Object value = "valvalvalvalvalvalvalvalvalval";

  @Test
  void testGetAll_1() {
    try {
      mc.delete(key).get();
      mc.asyncBopInsert(key, 0, eFlag, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 1, eFlag, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 2, eFlag, value + "2",
              new CollectionAttributes()).get();

      Map<Long, Element<Object>> map = mc.asyncBopGet(key, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get(
              100L, TimeUnit.MILLISECONDS);

      assertNotNull(map);
      assertEquals(3, map.size());

      for (long i = 0; i < map.size(); i++) {
        Object object = map.get(i).getValue();
        assertEquals(value + String.valueOf(i), object);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  void testGetAll_2() {
    try {
      mc.delete(key).get();
      mc.asyncBopInsert(key, 0, null, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 1, eFlag, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 2, eFlag, value + "2",
              new CollectionAttributes()).get();

      Map<Long, Element<Object>> map = mc.asyncBopGet(key, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get(
              100L, TimeUnit.MILLISECONDS);

      assertNotNull(map);
      assertEquals(3, map.size());

      for (long i = 0; i < map.size(); i++) {
        Object object = map.get(i).getValue();
        assertEquals(value + String.valueOf(i), object);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  void testGetAll_3() {
    try {
      mc.delete(key).get();
      mc.asyncBopInsert(key, 0, eFlag, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 1, null, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 2, eFlag, value + "2",
              new CollectionAttributes()).get();

      Map<Long, Element<Object>> map = mc.asyncBopGet(key, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get(
              100L, TimeUnit.MILLISECONDS);

      assertNotNull(map);
      assertEquals(3, map.size());

      for (long i = 0; i < map.size(); i++) {
        Object object = map.get(i).getValue();
        assertEquals(value + String.valueOf(i), object);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  void testGetAll_4() {
    try {
      mc.delete(key).get();
      mc.asyncBopInsert(key, 0, null, value + "0",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 1, null, value + "1",
              new CollectionAttributes()).get();
      mc.asyncBopInsert(key, 2, null, value + "2",
              new CollectionAttributes()).get();

      Map<Long, Element<Object>> map = mc.asyncBopGet(key, 0, 10,
              ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get(
              100L, TimeUnit.MILLISECONDS);

      assertNotNull(map);
      assertEquals(3, map.size());

      for (long i = 0; i < map.size(); i++) {
        Object object = map.get(i).getValue();
        assertEquals(value + String.valueOf(i), object);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
