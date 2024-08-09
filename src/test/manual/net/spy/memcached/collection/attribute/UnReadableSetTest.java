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
package net.spy.memcached.collection.attribute;

import java.util.Set;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UnReadableSetTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String VALUE = "VALUE";
  private final int INDEX = 0;

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
  public void testCreateUnreadableSetTest() {
    try {
      // create unreadable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(false);

      Boolean insertResult = mc.asyncSopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      Assertions.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assertions.assertEquals(Long.valueOf(0), attr.getCount());
      Assertions.assertEquals(Long.valueOf(4000), attr.getMaxCount());
      Assertions.assertEquals(Integer.valueOf(0), attr.getExpireTime());
      Assertions.assertFalse(attr.getReadable());

      // insert an item
      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE,
              new CollectionAttributes()).get());

      // get an item
      CollectionFuture<Set<Object>> f = mc.asyncSopGet(KEY, INDEX, false,
              false);
      Assertions.assertNull(f.get());
      Assertions.assertEquals("UNREADABLE", f.getOperationStatus()
              .getMessage());

      // set readable
      attribute.setReadable(true);
      Assertions.assertTrue(mc.asyncSetAttr(KEY, attribute).get());

      // get an item again
      f = mc.asyncSopGet(KEY, INDEX, false, false);
      Set<Object> set = f.get();

      Assertions.assertNotNull(set);
      Assertions.assertTrue(set.contains(VALUE));
      Assertions.assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testCreateReadableSetTest() {
    try {
      // create readable empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setReadable(true);

      Boolean insertResult = mc.asyncSopCreate(KEY,
              ElementValueType.STRING, attribute).get();
      Assertions.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assertions.assertEquals(Long.valueOf(0), attr.getCount());
      Assertions.assertEquals(Long.valueOf(4000), attr.getMaxCount());
      Assertions.assertEquals(Integer.valueOf(0), attr.getExpireTime());
      Assertions.assertTrue(attr.getReadable());

      // insert an item
      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE,
              new CollectionAttributes()).get());

      // get an item
      CollectionFuture<Set<Object>> f = mc.asyncSopGet(KEY, INDEX, false,
              false);

      Set<Object> set = f.get();
      Assertions.assertNotNull(set);
      Assertions.assertTrue(set.contains(VALUE));
      Assertions.assertEquals("END", f.getOperationStatus().getMessage());
    } catch (Exception e) {
      Assertions.fail(e.getMessage());
    }
  }

}
