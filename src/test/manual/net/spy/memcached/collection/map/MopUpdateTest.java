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
package net.spy.memcached.collection.map;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MopUpdateTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();

  private final String mkey = "updateTestMkey";

  private final String VALUE = "VALUE";
  private final String NEW_VALUE = "NEWVALUE";

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assertions.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testUpdateNotExistsKey() {
    try {
      // update value
      Assertions.assertFalse(mc.asyncMopUpdate(KEY, mkey, VALUE).get());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testExistsKey() {
    try {
      // insert one
      Assertions.assertTrue(mc.asyncMopInsert(KEY, mkey, VALUE,
              new CollectionAttributes()).get());

      // update value only
      Map<String, Object> rmap = mc.asyncMopGet(KEY, false, false)
              .get(1000, TimeUnit.MILLISECONDS);

      Assertions.assertEquals(VALUE, rmap.get(mkey));

      Assertions.assertTrue(mc.asyncMopUpdate(KEY, mkey, NEW_VALUE).get());

      Map<String, Object> urmap = mc.asyncMopGet(KEY, false, false)
              .get(1000, TimeUnit.MILLISECONDS);
      Assertions.assertEquals(NEW_VALUE, urmap.get(mkey));
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }
}
