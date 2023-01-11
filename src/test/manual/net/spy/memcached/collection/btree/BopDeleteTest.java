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

import org.junit.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;

public class BopDeleteTest extends BaseIntegrationTest {

  private String key = "UnReadableBTreeTest";

  private Long[] items9 = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L};

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    mc.asyncBopDelete(key, 0, 4000, ElementFlagFilter.DO_NOT_FILTER, 0,
            false).get(1000, TimeUnit.MILLISECONDS);
    addToBTree(key, items9);

    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(10);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testBopDelete_NoKey() throws Exception {
    assertFalse(mc.asyncBopDelete("no_key", 0,
            ElementFlagFilter.DO_NOT_FILTER, false).get(1000,
            TimeUnit.MILLISECONDS));
  }

  public void testBopDelete_OutOfRange() throws Exception {
    assertFalse(mc.asyncBopDelete(key, 11, ElementFlagFilter.DO_NOT_FILTER,
            false).get(1000, TimeUnit.MILLISECONDS));
  }

  public void testBopDelete_DeleteByBestEffort() throws Exception {
    // Delete items(2..11) in the list
    assertTrue(mc.asyncBopDelete(key, 2, 11,
            ElementFlagFilter.DO_NOT_FILTER, 0, false).get(1000,
            TimeUnit.MILLISECONDS));

    mc.asyncBopGet(key, 0, 100, ElementFlagFilter.DO_NOT_FILTER, 0, 100,
            false, false).get(1000, TimeUnit.MILLISECONDS);

    // By rule of 'best effort',
    // items(2..9) should be deleted
    // assertEquals(2, rmap.size());
    // assertEquals(1L, rlist.get(0));
    // assertEquals(2L, rlist.get(1));
  }

  public void testBopDelete_DeletedDropped() throws Exception {
    // Delete all items in the list
    assertTrue(mc.asyncBopDelete(key, 0, items9.length,
            ElementFlagFilter.DO_NOT_FILTER, 0, true).get(1000,
            TimeUnit.MILLISECONDS));

    CollectionAttributes attrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);
    assertNull(attrs);
  }

  public void testBopDeleteWithSingleBkey() throws Exception {
    mc.delete(key).get();

    byte[] bkey = new byte[]{(byte) 1};
    Assert.assertTrue(mc.asyncBopInsert(key, bkey, null, "value",
            new CollectionAttributes()).get());

    Map<ByteArrayBKey, Element<Object>> map = mc.asyncBopGet(key, bkey,
            ElementFlagFilter.DO_NOT_FILTER, false, false).get();
    Assert.assertNotNull(map);
    Assert.assertEquals(1, map.size());
    Assert.assertTrue(mc.asyncBopDelete(key, bkey,
            ElementFlagFilter.DO_NOT_FILTER, true).get());

    Assert.assertNull(mc.asyncBopGet(key, bkey,
            ElementFlagFilter.DO_NOT_FILTER, false, false).get());

    mc.delete(key).get();
  }

}
