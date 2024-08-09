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

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class LopInsertDataType extends BaseIntegrationTest {

  private String key = "LopInsertDataType";
  private Random rand = new Random(new Date().getTime());

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    deleteList(key, 1000);
    super.tearDown();
  }

  @Test
  public void testLopInsert_ElementCountLimit() throws Exception {
    byte[] tooBigByte = new byte[1024 * 1024];
    for (int i = 0; i < tooBigByte.length; i++) {
      tooBigByte[i] = (byte) rand.nextInt(255);
    }

    try {
      mc.asyncLopInsert(key, 0, new String(tooBigByte),
              new CollectionAttributes())
              .get(1000, TimeUnit.MILLISECONDS);
      fail();
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(e.getMessage().contains("Cannot cache data larger than"));
    }
  }

  @Test
  public void testLopInsert_SameDataType() throws Exception {
    // First, create a list and insert one item in it
    assertTrue(mc.asyncLopInsert(key, 0, "a string",
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS));

    // Then insert an another item with same data type
    assertTrue(mc.asyncLopInsert(key, 1, "an another string", null).get(
            1000, TimeUnit.MILLISECONDS));

    // Retrieve items from the list and check they're in same data type
    List<Object> rlist = mc.asyncLopGet(key, 0, 10, false, false).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(2, rlist.size());
    assertEquals(rlist.get(0).getClass(), rlist.get(1).getClass());
    for (Object each : rlist) {
      assertEquals(rlist.get(0).getClass(), each.getClass());
    }
  }

  @Test
  public void testLopInsert_DifferentDataType() throws Exception {
    // First, create a list and insert one item in it
    assertTrue(mc.asyncLopInsert(key, -1, "a string",
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS));

    // Then insert another items with different data types
    assertTrue(mc.asyncLopInsert(key, -1, 100, null).get(1000,
            TimeUnit.MILLISECONDS));

    assertTrue(mc.asyncLopInsert(key, -1, 101L, null).get(1000,
            TimeUnit.MILLISECONDS));

    assertTrue(mc.asyncLopInsert(key, -1, 'f', null).get(
            1000, TimeUnit.MILLISECONDS));

    // Retrieve items from the list and check they're in same data type
    List<Object> rlist = mc.asyncLopGet(key, 0, 10, false, false).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(4, rlist.size());
    for (Object each : rlist) {
      assertEquals(rlist.get(0).getClass(), each.getClass());
    }
  }

  @Test
  public void testLopInsert_DifferentDataType_ErrorCase() throws Exception {
    // First, create a list and insert one item in it
    assertTrue(mc.asyncLopInsert(key, 0, 'a',
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS));

    // Then insert an another item with different data type
    assertTrue(mc.asyncLopInsert(key, 1, "a string", null).get(1000,
            TimeUnit.MILLISECONDS));

    // Retrieve items from the list and check they're in same data type
    List<Object> rlist = mc.asyncLopGet(key, 0, 10, false, false).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(2, rlist.size());
    assertNotNull(rlist.get(0));
    assertNull(rlist.get(1));
  }

}
