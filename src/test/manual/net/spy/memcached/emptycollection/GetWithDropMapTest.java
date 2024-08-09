/*
 * arcus-java-client : Arcus Java client
 * Copyright 2016 JaM2in Co., Ltd.
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

import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GetWithDropMapTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String MKEY = "mkey";
  private final int VALUE = 1234567890;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();

    boolean insertResult = mc.asyncMopInsert(KEY, MKEY, VALUE,
            new CollectionAttributes()).get();
    Assertions.assertTrue(insertResult);
  }

  @Test
  public void testGetWithoutDeleteAndDrop() {
    try {
      // check attr
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value delete=false, drop=false
      Assertions.assertEquals(
              VALUE,
              mc.asyncMopGet(KEY, MKEY, false, false).get().get(MKEY));

      // check exists
      Assertions.assertEquals(Long.valueOf(1), mc.asyncGetAttr(KEY).get()
              .getCount());

      // get value again
      Assertions.assertEquals(
              VALUE,
              mc.asyncMopGet(KEY, MKEY, false, false).get().get(MKEY));
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
      Assertions.assertEquals(
              VALUE,
              mc.asyncMopGet(KEY, MKEY, true, false).get().get(MKEY));

      // check exists empty map
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNotNull(attr);
      Assertions.assertEquals(Long.valueOf(0), attr.getCount());

      Map<String, Object> map = mc.asyncMopGet(KEY, MKEY, false, false).get();
      Assertions.assertNotNull(map);
      Assertions.assertTrue(map.isEmpty());
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

      // get value delete=true, drop=true
      Assertions.assertEquals(
              VALUE,
              mc.asyncMopGet(KEY, MKEY, true, true).get().get(MKEY));

      // check map
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();
      Assertions.assertNull(attr);

      Map<String, Object> map = mc.asyncMopGet(KEY, MKEY, false, false).get();
      Assertions.assertNull(map);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }
}
