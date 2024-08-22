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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.CompOperands;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class BTreeDeleteWithFilterTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final long BKEY = 10L;
  private final int VALUE = 1234567890;

  private final String FLAG = "flag";

  @BeforeEach
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
  }

  @AfterEach
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testDeleteWithMatchedFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, FLAG.getBytes());

      boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              FLAG.getBytes(), VALUE, new CollectionAttributes()).get();
      assertTrue(insertResult);

      // check attr
      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // delete one bkey
      Boolean delete = mc.asyncBopDelete(KEY, BKEY, filter, false).get();
      assertTrue(delete);

      // check attr again
      assertEquals(Long.valueOf(0), mc.asyncGetAttr(KEY).get()
              .getCount());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testDeleteWithUnMatchedFilter() {
    try {
      ElementFlagFilter filter = new ElementFlagFilter(
              CompOperands.Equal, "aa".getBytes());

      boolean insertResult = mc.asyncBopInsert(KEY, BKEY,
              FLAG.getBytes(), VALUE, new CollectionAttributes()).get();
      assertTrue(insertResult);

      // check attr
      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // delete one bkey
      Boolean delete = mc.asyncBopDelete(KEY, BKEY, filter, false).get();
      assertFalse(delete);

      // check attr again
      assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
