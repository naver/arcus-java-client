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

public class LopGetTest extends BaseIntegrationTest {

  private String key = "LopGetTest";

  private Long[] items9 = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    deleteList(key, 1000);
    addToList(key, items9);

    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));
  }

  @Override
  protected void tearDown() throws Exception {
    deleteList(key, 1000);
    super.tearDown();
  }

  public void testLopGet_NoKey() throws Exception {
    List<Object> rlist = mc.asyncLopGet("no_key", 0, false, false).get(
            1000, TimeUnit.MILLISECONDS);

    // We've got an empty list
    assertNull(rlist);
  }

  public void testLopGet_OutOfRange() throws Exception {
    List<Object> list = mc.asyncLopGet(key, 20, false, false).get(1000,
            TimeUnit.MILLISECONDS);
    assertNotNull(list);
    assertTrue(list.isEmpty());
  }

  public void testLopGet_GetByBestEffort() throws Exception {
    // Retrieve items(2..11) in the list
    List<Object> rlist = mc.asyncLopGet(key, 2, 11, false, false).get(1000,
            TimeUnit.MILLISECONDS);

    // By rule of 'best effort',
    // items(2..9) should be retrieved
    assertEquals(7, rlist.size());
    for (int i = 0; i < rlist.size(); i++) {
      assertEquals(items9[i + 2], rlist.get(i));
    }
  }

  public void testLopGet_GetWithDeletion() throws Exception {
    CollectionAttributes attrs = null;
    List<Object> rlist = null;

    // Retrieve items(0..5) in the list with delete option
    rlist = mc.asyncLopGet(key, 0, 5, true, false).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(6, rlist.size());

    // Check the remaining item count in the list
    attrs = mc.asyncGetAttr(key).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(3, attrs.getCount().intValue());

    // Retrieve items(0..2) in the list with delete option
    rlist = mc.asyncLopGet(key, 0, 2, true, true).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(3, rlist.size());

    // Now our list has no items and would be deleted
    rlist = mc.asyncLopGet(key, 0, 10, true, false).get(1000,
            TimeUnit.MILLISECONDS);
    assertNull(rlist);
  }

  public void testLopGet_simple() throws Exception {
    CollectionAttributes attrs = null;

    // get all elements
    List<Object> list = mc.asyncLopGet(key, 0, 9).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(list.size(), 9);

    // get count of the list and compare with count of items after using get method
    attrs = mc.asyncGetAttr(key).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(9, attrs.getCount().intValue());
  }
}
