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
package net.spy.memcached.collection.set;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.internal.CollectionFuture;

public class SopPipedExistTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String VALUE1 = "VALUE1";
  private final String VALUE2 = "VALUE2";
  private final String VALUE3 = "VALUE3";
  private final String VALUE4 = "VALUE4";
  private final String VALUE5 = "VALUE5";
  private final String VALUE6 = "VALUE6";

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

  public void testPipedExist() {
    try {
      Assert.assertTrue(mc.asyncSopCreate(KEY, ElementValueType.STRING,
              new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncSopInsert(KEY, VALUE1,
              new CollectionAttributes()).get());
      Assert.assertTrue(mc.asyncSopInsert(KEY, VALUE2,
              new CollectionAttributes()).get());
      Assert.assertTrue(mc.asyncSopInsert(KEY, VALUE3,
              new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncSopExist(KEY, VALUE1).get());
      Assert.assertTrue(mc.asyncSopExist(KEY, VALUE2).get());
      Assert.assertTrue(mc.asyncSopExist(KEY, VALUE3).get());
      Assert.assertFalse(mc.asyncSopExist(KEY, VALUE4).get());

      List<Object> findValues = new ArrayList<Object>();
      findValues.add(VALUE1);
      findValues.add(VALUE4);
      findValues.add(VALUE2);
      findValues.add(VALUE6);
      findValues.add(VALUE3);
      findValues.add(VALUE5);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assert.assertTrue(future.getOperationStatus().isSuccess());

      Assert.assertTrue(map.get(VALUE1));
      Assert.assertTrue(map.get(VALUE2));
      Assert.assertTrue(map.get(VALUE3));
      Assert.assertFalse(map.get(VALUE4));
      Assert.assertFalse(map.get(VALUE5));
      Assert.assertFalse(map.get(VALUE6));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testPipedExistWithOneValue() {
    try {
      Assert.assertTrue(mc.asyncSopCreate(KEY, ElementValueType.STRING,
              new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncSopInsert(KEY, VALUE1,
              new CollectionAttributes()).get());
      Assert.assertTrue(mc.asyncSopInsert(KEY, VALUE2,
              new CollectionAttributes()).get());
      Assert.assertTrue(mc.asyncSopInsert(KEY, VALUE3,
              new CollectionAttributes()).get());

      Assert.assertTrue(mc.asyncSopExist(KEY, VALUE1).get());
      Assert.assertTrue(mc.asyncSopExist(KEY, VALUE2).get());
      Assert.assertTrue(mc.asyncSopExist(KEY, VALUE3).get());
      Assert.assertFalse(mc.asyncSopExist(KEY, VALUE4).get());

      List<Object> findValues = new ArrayList<Object>();
      findValues.add(VALUE1);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assert.assertTrue(future.getOperationStatus().isSuccess());

      Assert.assertTrue(map.get(VALUE1));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testMaxPipedExist() {
    try {
      List<Object> findValues = new ArrayList<Object>();

      // insert items
      for (int i = 0; i < mc.getMaxPipedItemCount(); i++) {
        findValues.add("VALUE" + i);

        if (i / 2 == 0)
          continue;
        Assert.assertTrue(mc.asyncSopInsert(KEY, "VALUE" + i,
                new CollectionAttributes()).get());
      }

      // exist bulk
      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assert.assertTrue(future.getOperationStatus().isSuccess());

      for (int i = 0; i < mc.getMaxPipedItemCount(); i++) {
        if (i / 2 == 0) {
          Assert.assertFalse(map.get("VALUE" + i));
        } else {
          Assert.assertTrue(map.get("VALUE" + i));
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testPipedExistNotExistsKey() {
    try {
      List<Object> findValues = new ArrayList<Object>();
      findValues.add(VALUE1);
      findValues.add(VALUE4);
      findValues.add(VALUE2);
      findValues.add(VALUE6);
      findValues.add(VALUE3);
      findValues.add(VALUE5);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assert.assertTrue(map.isEmpty());
      Assert.assertFalse(future.getOperationStatus().isSuccess());
      Assert.assertEquals(CollectionResponse.NOT_FOUND, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testPipedExistOneNotExistsKey() {
    try {
      List<Object> findValues = new ArrayList<Object>();
      findValues.add(VALUE1);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assert.assertTrue(map.isEmpty());
      Assert.assertFalse(future.getOperationStatus().isSuccess());
      Assert.assertEquals(CollectionResponse.NOT_FOUND, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testPipedExistTypeMismatchedKey() {
    try {
      Assert.assertTrue(mc.set(KEY, 10, VALUE1).get());

      List<Object> findValues = new ArrayList<Object>();
      findValues.add(VALUE1);
      findValues.add(VALUE2);
      findValues.add(VALUE6);
      findValues.add(VALUE3);
      findValues.add(VALUE5);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assert.assertTrue(map.isEmpty());
      Assert.assertFalse(future.getOperationStatus().isSuccess());
      Assert.assertEquals(CollectionResponse.TYPE_MISMATCH, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  public void testPipedExistOneTypeMismatchedKey() {
    try {
      Assert.assertTrue(mc.set(KEY, 10, VALUE1).get());

      List<Object> findValues = new ArrayList<Object>();
      findValues.add(VALUE1);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assert.assertTrue(map.isEmpty());
      Assert.assertFalse(future.getOperationStatus().isSuccess());
      Assert.assertEquals(CollectionResponse.TYPE_MISMATCH, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
