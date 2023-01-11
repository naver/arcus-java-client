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

import org.junit.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.CollectionOverflowAction;

public class CreateEmptyMapTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assert.assertNull(mc.asyncGetAttr(KEY).get());
  }

  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  public void testCreateEmptyWithDefaultAttribute() {
    try {
      // create empty
      CollectionAttributes attribute = new CollectionAttributes();
      Boolean insertResult = mc.asyncMopCreate(KEY,
              ElementValueType.OTHERS, attribute).get();
      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assert.assertEquals(Long.valueOf(0), attr.getCount());
      Assert.assertEquals(Long.valueOf(4000), attr.getMaxCount());
      Assert.assertEquals(Integer.valueOf(0), attr.getExpireTime());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testCreateEmptyWithSpecifiedAttribute() {
    try {
      // create empty
      CollectionAttributes attribute = new CollectionAttributes();
      attribute.setMaxCount(10000);
      attribute.setExpireTime(9999);
      attribute.setOverflowAction(CollectionOverflowAction.error);
      Boolean insertResult = mc.asyncMopCreate(KEY,
              ElementValueType.OTHERS, attribute).get();

      Assert.assertTrue(insertResult);

      // check attribute
      CollectionAttributes attr = mc.asyncGetAttr(KEY).get();

      Assert.assertEquals(Long.valueOf(0), attr.getCount());
      Assert.assertEquals(Long.valueOf(10000), attr.getMaxCount());
      Assert.assertEquals(CollectionOverflowAction.error,
              attr.getOverflowAction());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

}
