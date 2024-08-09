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

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BopUpsertTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final byte[] BKEY = new byte[]{(byte) 1};
  private final String VALUE = "VALUE";
  private final String VALUE2 = "EULAV";
  private final byte[] FLAG = "FLAG".getBytes();
  private final byte[] FLAG2 = "GALF".getBytes();

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  ;
  @Test
  public void testUpsertExistsValueOnly() throws Exception {
    // insert one
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());

    // get
    Map<ByteArrayBKey, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            BKEY, ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false)
            .get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(1, map.size());

    for (Entry<ByteArrayBKey, Element<Object>> i : map.entrySet()) {
      Assertions.assertTrue(Arrays.equals(BKEY, i.getKey().getBytes()));
      Assertions.assertEquals(VALUE, i.getValue().getValue());
      Assertions.assertTrue(Arrays.equals(FLAG, i.getValue().getEFlag()));
    }

    // upsert
    Assertions.assertTrue(mc.asyncBopUpsert(KEY, BKEY, null, VALUE2, null)
            .get());

    // get again
    map = mc.asyncBopGet(KEY, BKEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
            0, 10, false, false).get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(1, map.size());
    for (Entry<ByteArrayBKey, Element<Object>> i : map.entrySet()) {
      Assertions.assertTrue(Arrays.equals(BKEY, i.getKey().getBytes()));
      Assertions.assertEquals(VALUE2, i.getValue().getValue());
      Assertions.assertNull(i.getValue().getEFlag());
    }
  }

  @Test
  public void testUpsertNotExistsValueOnly() throws Exception {
    // upsert
    Assertions.assertTrue(mc.asyncBopUpsert(KEY, BKEY, null, VALUE2,
            new CollectionAttributes()).get());

    // get again
    Map<ByteArrayBKey, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            BKEY, ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false)
            .get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(1, map.size());
    for (Entry<ByteArrayBKey, Element<Object>> i : map.entrySet()) {
      Assertions.assertTrue(Arrays.equals(BKEY, i.getKey().getBytes()));
      Assertions.assertEquals(VALUE2, i.getValue().getValue());
      Assertions.assertNull(i.getValue().getEFlag());
    }
  }
  @Test
  public void testUpsertExistsEFlagOnly() throws Exception {
    // insert one
    Assertions.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());

    // get
    Map<ByteArrayBKey, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            BKEY, ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false)
            .get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(1, map.size());

    for (Entry<ByteArrayBKey, Element<Object>> i : map.entrySet()) {
      Assertions.assertTrue(Arrays.equals(BKEY, i.getKey().getBytes()));
      Assertions.assertEquals(VALUE, i.getValue().getValue());
      Assertions.assertTrue(Arrays.equals(FLAG, i.getValue().getEFlag()));
    }

    // upsert
    Assertions.assertTrue(mc.asyncBopUpsert(KEY, BKEY, FLAG2, VALUE2, null)
            .get());

    // get again
    map = mc.asyncBopGet(KEY, BKEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
            0, 10, false, false).get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assertions.assertEquals(1, map.size());
    for (Entry<ByteArrayBKey, Element<Object>> i : map.entrySet()) {
      Assertions.assertTrue(Arrays.equals(BKEY, i.getKey().getBytes()));
      Assertions.assertEquals(VALUE2, i.getValue().getValue());
      Assertions.assertTrue(Arrays.equals(FLAG2, i.getValue().getEFlag()));
    }
  }

}
