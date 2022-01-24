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

import org.junit.Assert;

public class BopInsertAndGetWithElementFlagTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final byte[] BKEY = new byte[]{(byte) 1};
  private final byte[] BKEY2 = new byte[]{(byte) 2};
  private final byte[] BKEY3 = new byte[]{(byte) 3};
  private final String VALUE = "VALUE";
  private final byte[] FLAG = "FLAG".getBytes();
  private final byte[] FLAG2 = "GLAF".getBytes();
  private final byte[] FLAG3 = "FFFF".getBytes();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  ;

  public void testSingleLongBkeyWithEFlag() throws Exception {

    // insert one
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());

    // get
    Map<ByteArrayBKey, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            BKEY, ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false)
            .get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, map.size());

    for (Entry<ByteArrayBKey, Element<Object>> i : map.entrySet()) {
      Assert.assertTrue(Arrays.equals(BKEY, i.getKey().getBytes()));
      Assert.assertEquals(VALUE, i.getValue().getValue());
      Assert.assertTrue(Arrays.equals(FLAG, i.getValue().getEFlag()));
    }

    // delete
    Assert.assertTrue(mc.asyncBopDelete(KEY, BKEY, BKEY,
            ElementFlagFilter.DO_NOT_FILTER, 100, false).get());

    // get again
    map = mc.asyncBopGet(KEY, BKEY, BKEY, ElementFlagFilter.DO_NOT_FILTER,
            0, 10, false, false).get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(0, map.size());

  }

  public void testMultipleLongBkeyWithEFlag() throws Exception {

    // insert 3 elements
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY, FLAG, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY2, FLAG2, VALUE,
            new CollectionAttributes()).get());
    Assert.assertTrue(mc.asyncBopInsert(KEY, BKEY3, FLAG3, VALUE,
            new CollectionAttributes()).get());

    // get 3 elements
    Map<ByteArrayBKey, Element<Object>> map = mc.asyncBopGet(KEY, BKEY,
            BKEY3, ElementFlagFilter.DO_NOT_FILTER, 0, 10, false, false)
            .get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(3, map.size());

    Assert.assertEquals(VALUE, map.get(BKEY).getValue());
    Assert.assertEquals(VALUE, map.get(BKEY2).getValue());
    Assert.assertEquals(VALUE, map.get(BKEY3).getValue());

    Assert.assertTrue(Arrays.equals(FLAG, map.get(BKEY).getEFlag()));
    Assert.assertTrue(Arrays.equals(FLAG2, map.get(BKEY2).getEFlag()));
    Assert.assertTrue(Arrays.equals(FLAG3, map.get(BKEY3).getEFlag()));

    // delete only 2 elements
    Assert.assertTrue(mc.asyncBopDelete(KEY, BKEY, BKEY2,
            ElementFlagFilter.DO_NOT_FILTER, 100, false).get());

    // get all again
    map = mc.asyncBopGet(KEY, BKEY, BKEY3, ElementFlagFilter.DO_NOT_FILTER,
            0, 10, false, false).get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    Assert.assertEquals(1, map.size());
    Assert.assertEquals(VALUE, map.get(BKEY3).getValue());
    Assert.assertTrue(Arrays.equals(FLAG3, map.get(BKEY3).getEFlag()));
  }
}
