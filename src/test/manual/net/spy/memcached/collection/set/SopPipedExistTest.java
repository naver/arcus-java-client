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

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SopPipedExistTest extends BaseIntegrationTest {

  private final String KEY = this.getClass().getSimpleName();
  private final String VALUE1 = "VALUE1";
  private final String VALUE2 = "VALUE2";
  private final String VALUE3 = "VALUE3";
  private final String VALUE4 = "VALUE4";
  private final String VALUE5 = "VALUE5";
  private final String VALUE6 = "VALUE6";

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
  public void testPipedExist() {
    try {
      Assertions.assertTrue(mc.asyncSopCreate(KEY, ElementValueType.STRING,
              new CollectionAttributes()).get());

      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE1,
              new CollectionAttributes()).get());
      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE2,
              new CollectionAttributes()).get());
      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE3,
              new CollectionAttributes()).get());

      Assertions.assertTrue(mc.asyncSopExist(KEY, VALUE1).get());
      Assertions.assertTrue(mc.asyncSopExist(KEY, VALUE2).get());
      Assertions.assertTrue(mc.asyncSopExist(KEY, VALUE3).get());
      Assertions.assertFalse(mc.asyncSopExist(KEY, VALUE4).get());

      List<Object> findValues = new ArrayList<>();
      findValues.add(VALUE1);
      findValues.add(VALUE4);
      findValues.add(VALUE2);
      findValues.add(VALUE6);
      findValues.add(VALUE3);
      findValues.add(VALUE5);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assertions.assertTrue(future.getOperationStatus().isSuccess());

      Assertions.assertTrue(map.get(VALUE1));
      Assertions.assertTrue(map.get(VALUE2));
      Assertions.assertTrue(map.get(VALUE3));
      Assertions.assertFalse(map.get(VALUE4));
      Assertions.assertFalse(map.get(VALUE5));
      Assertions.assertFalse(map.get(VALUE6));
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testPipedExistWithOneValue() {
    try {
      Assertions.assertTrue(mc.asyncSopCreate(KEY, ElementValueType.STRING,
              new CollectionAttributes()).get());

      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE1,
              new CollectionAttributes()).get());
      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE2,
              new CollectionAttributes()).get());
      Assertions.assertTrue(mc.asyncSopInsert(KEY, VALUE3,
              new CollectionAttributes()).get());

      Assertions.assertTrue(mc.asyncSopExist(KEY, VALUE1).get());
      Assertions.assertTrue(mc.asyncSopExist(KEY, VALUE2).get());
      Assertions.assertTrue(mc.asyncSopExist(KEY, VALUE3).get());
      Assertions.assertFalse(mc.asyncSopExist(KEY, VALUE4).get());

      List<Object> findValues = new ArrayList<>();
      findValues.add(VALUE1);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assertions.assertTrue(future.getOperationStatus().isSuccess());

      Assertions.assertTrue(map.get(VALUE1));
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testMaxPipedExist() {
    try {
      List<Object> findValues = new ArrayList<>();

      // insert items
      for (int i = 0; i < mc.getMaxPipedItemCount(); i++) {
        findValues.add("VALUE" + i);

        if (i / 2 == 0) {
          continue;
        }
        Assertions.assertTrue(mc.asyncSopInsert(KEY, "VALUE" + i,
                new CollectionAttributes()).get());
      }

      // exist bulk
      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assertions.assertTrue(future.getOperationStatus().isSuccess());

      for (int i = 0; i < mc.getMaxPipedItemCount(); i++) {
        if (i / 2 == 0) {
          Assertions.assertFalse(map.get("VALUE" + i));
        } else {
          Assertions.assertTrue(map.get("VALUE" + i));
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testPipedExistNotExistsKey() {
    try {
      List<Object> findValues = new ArrayList<>();
      findValues.add(VALUE1);
      findValues.add(VALUE4);
      findValues.add(VALUE2);
      findValues.add(VALUE6);
      findValues.add(VALUE3);
      findValues.add(VALUE5);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assertions.assertTrue(map.isEmpty());
      Assertions.assertFalse(future.getOperationStatus().isSuccess());
      Assertions.assertEquals(CollectionResponse.NOT_FOUND, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testPipedExistOneNotExistsKey() {
    try {
      List<Object> findValues = new ArrayList<>();
      findValues.add(VALUE1);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assertions.assertTrue(map.isEmpty());
      Assertions.assertFalse(future.getOperationStatus().isSuccess());
      Assertions.assertEquals(CollectionResponse.NOT_FOUND, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testPipedExistTypeMismatchedKey() {
    try {
      Assertions.assertTrue(mc.set(KEY, 10, VALUE1).get());

      List<Object> findValues = new ArrayList<>();
      findValues.add(VALUE1);
      findValues.add(VALUE2);
      findValues.add(VALUE6);
      findValues.add(VALUE3);
      findValues.add(VALUE5);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assertions.assertTrue(map.isEmpty());
      Assertions.assertFalse(future.getOperationStatus().isSuccess());
      Assertions.assertEquals(CollectionResponse.TYPE_MISMATCH, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testPipedExistOneTypeMismatchedKey() {
    try {
      Assertions.assertTrue(mc.set(KEY, 10, VALUE1).get());

      List<Object> findValues = new ArrayList<>();
      findValues.add(VALUE1);

      CollectionFuture<Map<Object, Boolean>> future = mc
              .asyncSopPipedExistBulk(KEY, findValues);

      Map<Object, Boolean> map = future.get();

      Assertions.assertTrue(map.isEmpty());
      Assertions.assertFalse(future.getOperationStatus().isSuccess());
      Assertions.assertEquals(CollectionResponse.TYPE_MISMATCH, future
              .getOperationStatus().getResponse());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }
}
