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
package net.spy.memcached.collection.set;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SopOverflowActionTest extends BaseIntegrationTest {

  private String key = "SopOverflowActionTest";
  private List<String> keyList = new ArrayList<>();

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    keyList.add(key);
  }

  @Test
  public void testSopGet_Maxcount() throws Exception {
    // Test
    for (int maxcount = 50000; maxcount <= 10000; maxcount += 1000) {
      // Create a B+ Tree
      mc.asyncSopInsert(key, "item0", new CollectionAttributes()).get();

      // Set maxcount
      CollectionAttributes attrs = new CollectionAttributes();
      attrs.setMaxCount(maxcount);
      assertTrue(mc.asyncSetAttr(key, attrs).get(1000,
              TimeUnit.MILLISECONDS));

      for (int i = 1; i <= maxcount + 1000; i++) {
        boolean success = mc.asyncSopInsert(key, "item" + i,
                new CollectionAttributes()).get(1000,
                TimeUnit.MILLISECONDS);
        if (i >= maxcount) {
          assertFalse(success);
        }
      }

      Set<Object> result = mc.asyncSopGet(key, maxcount + 1000, false,
              false).get(10000, TimeUnit.MILLISECONDS);

      assertEquals(maxcount, result.size());
      assertFalse(result.contains("item" + maxcount));

      for (int i = 0; i <= maxcount; i++) {
        mc.asyncSopDelete(key, "item" + i, false).get(1000,
                TimeUnit.MILLISECONDS);
      }
    }
  }

  @Test
  public void testSopGet_AvailableOverflowAction() throws Exception {
    // Create a set
    mc.asyncSopInsert(key, "item0", new CollectionAttributes()).get();

    // Set OverflowAction
    // error
    assertTrue(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.error))
            .get(1000, TimeUnit.MILLISECONDS));

    // head_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.head_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // tail_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.tail_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // smallest_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.smallest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    // largest_trim
    assertFalse(mc.asyncSetAttr(key,
            new CollectionAttributes(null, null, CollectionOverflowAction.largest_trim)).get(1000,
            TimeUnit.MILLISECONDS));

    mc.asyncSopDelete(key, "item0", false).get(1000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testSopGet_notAvailableOverflowAction() {
    CollectionAttributes attributesForCreate = new CollectionAttributes();

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.head_trim);
      mc.asyncSopInsert(key, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncSopPipedInsertBulk(key, new ArrayList<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncSopInsertBulk(keyList, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.tail_trim);
      mc.asyncSopInsert(key, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncSopPipedInsertBulk(key, new ArrayList<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncSopInsertBulk(keyList, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.smallest_trim);
      mc.asyncSopInsert(key, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncSopPipedInsertBulk(key, new ArrayList<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncSopInsertBulk(keyList, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.smallest_silent_trim);
      mc.asyncSopInsert(key, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncSopPipedInsertBulk(key, new ArrayList<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncSopInsertBulk(keyList, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.largest_trim);
      mc.asyncSopInsert(key, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncSopPipedInsertBulk(key, new ArrayList<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncSopInsertBulk(keyList, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // insert
    try {
      attributesForCreate.setOverflowAction(CollectionOverflowAction.largest_silent_trim);
      mc.asyncSopInsert(key, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // pipe insert
    try {
      mc.asyncSopPipedInsertBulk(key, new ArrayList<>(), attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }

    // bulk insert
    try {
      mc.asyncSopInsertBulk(keyList, "1", attributesForCreate).get();
      Assertions.fail();
    } catch (IllegalArgumentException e) {
      // test success
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail();
    }
  }
}
