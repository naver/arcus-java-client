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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LopDeleteTest extends BaseIntegrationTest {

  private String key = "LopDeleteTest";

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
  public void testLopDelete_NoKey() throws Exception {
    assertFalse(mc.asyncLopDelete("no_key", 0, false).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopDelete_OutOfRange() throws Exception {
    assertFalse(mc.asyncLopDelete(key, 11, false).get(1000,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLopDelete_DeleteByBestEffort() throws Exception {
    // Delete items(2..11) in the list
    assertTrue(mc.asyncLopDelete(key, 2, 11, false).get(1000,
            TimeUnit.MILLISECONDS));

    List<Object> rlist = mc.asyncLopGet(key, 0, 100, false, false).get(
            1000, TimeUnit.MILLISECONDS);

    // By rule of 'best effort',
    // items(2..9) should be deleted
    assertEquals(2, rlist.size());
    assertEquals(1L, rlist.get(0));
    assertEquals(2L, rlist.get(1));
  }

  @Test
  public void testLopDelete_DeletedDropped() throws Exception {
    // Delete all items in the list
    assertTrue(mc.asyncLopDelete(key, 0, items9.length, true).get(1000,
            TimeUnit.MILLISECONDS));

    CollectionAttributes attrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertNull(attrs);
  }

}
