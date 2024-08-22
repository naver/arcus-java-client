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
package net.spy.memcached.collection.btree.longbkey;

import java.util.Map;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopGetTest extends BaseIntegrationTest {

  private static final String KEY = BopGetTest.class.getSimpleName();

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
  public void testBopGet() throws Exception {

    byte[] bkey = new byte[]{(byte) 1};

    Boolean boolean1 = mc.asyncBopInsert(KEY, bkey, null, "value",
            new CollectionAttributes()).get();

    assertTrue(boolean1);

    Map<ByteArrayBKey, Element<Object>> map = mc.asyncBopGet(KEY, bkey,
            ElementFlagFilter.DO_NOT_FILTER, false, false).get();

    assertEquals(1, map.size());

    Element<Object> el = map.get(new ByteArrayBKey(bkey));

    assertNotNull(el);

    assertEquals("value", el.getValue());
    assertEquals("0x01", el.getStringBkey());
    assertEquals("", el.getStringEFlag());
  }
}
