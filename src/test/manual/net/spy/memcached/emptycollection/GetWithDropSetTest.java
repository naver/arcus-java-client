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

import java.util.Set;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GetWithDropSetTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final int VALUE = 1234567890;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();

    boolean insertResult = mc.asyncSopInsert(KEY, VALUE,
            new CollectionAttributes()).get();
    Assertions.assertTrue(insertResult);
  }

  @Test
  public void testGetWithoutDeleteAndDrop() {
    try {
      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=false, drop=true
      Assertions.assertTrue(mc.asyncSopGet(KEY, 10, false, false).get()
              .contains(VALUE));

      // check exists
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value again
      Assertions.assertTrue(mc.asyncSopGet(KEY, 10, false, false).get()
              .contains(VALUE));
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetWithDeleteAndWithoutDrop() {
    try {
      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assertions.assertTrue(mc.asyncSopGet(KEY, 10, true, false).get()
              .contains(VALUE));

      // check exists empty btree
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNotNull(attr);
      Assertions.assertEquals(Long.valueOf(0), attr.getCount());

      Set<Object> set = mc.asyncSopGet(KEY, 10, false, false).get();
      Assertions.assertNotNull(set);
      Assertions.assertTrue(set.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testGetWithDeleteAndWithDrop() {
    try {
      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=true, drop=false
      Assertions.assertTrue(mc.asyncSopGet(KEY, 10, true, true).get()
              .contains(VALUE));

      // check exists empty set
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNull(attr);

      Set<Object> set = mc.asyncSopGet(KEY, 10, false, false).get();
      Assertions.assertNull(set);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

//  public void testGetWithoutDeleteAndDropOptions() {
//    try {
//      // check attr
//      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
//              .getCount());
//
//      //get value
//      assertTrue(mc.asyncSopGet(KEY, 10).get()
//              .contains(VALUE));
//
//      // check exists
//      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
//              .getCount());
//
//      // get value again
//      assertTrue(mc.asyncSopGet(KEY, 10).get()
//              .contains(VALUE));
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail(e.getMessage());
//    }
//  }
}
