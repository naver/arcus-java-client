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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class BopUpsertTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10;

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
  void testUpsertNotExistsKey() {
    try {
      boolean result = mc.asyncBopUpsert(KEY, BKEY, "eflag".getBytes(),
              "VALUE", new CollectionAttributes()).get();

      assertTrue(result, "Upsert failed");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  void upsertExistingElement() throws Exception {
    // given
    CollectionFuture<Boolean> future
            = mc.asyncBopUpsert(KEY, BKEY, null, "INITIAL_VALUE", new CollectionAttributes());
    boolean result = future.get();
    assertTrue(result);

    // when
    future = mc.asyncBopUpsert(KEY, BKEY, null, "UPDATED_VALUE", null);
    result = future.get();

    // then
    assertTrue(result);
    CollectionFuture<Map<Long, Element<Object>>> futureToGet
            = mc.asyncBopGet(KEY, BKEY, null, false, false);
    Map<Long, Element<Object>> elements = futureToGet.get();
    assertTrue(elements.containsKey(BKEY));
    assertEquals("UPDATED_VALUE", elements.get(BKEY).getValue());
  }
}
