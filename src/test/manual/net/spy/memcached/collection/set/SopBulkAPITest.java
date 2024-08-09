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
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SopBulkAPITest extends BaseIntegrationTest {

  private final String key = "SopBulkAPITest";
  private final List<Object> valueList = new ArrayList<>();

  private int getValueCount() {
    return mc.getMaxPipedItemCount();
  }

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    for (long i = 0; i < getValueCount(); i++) {
      valueList.add("value" + String.valueOf(i));
    }
  }

  @Test
  public void testBulk() throws Exception {
    for (int i = 0; i < 10; i++) {
      mc.delete(key).get();
      bulk();
    }
  }

  public void bulk() {
    try {
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncSopPipedInsertBulk(key, valueList,
                      new CollectionAttributes());

      Map<Integer, CollectionOperationStatus> map = future.get(10000,
              TimeUnit.MILLISECONDS);

      Set<Object> set = mc
              .asyncSopGet(key, getValueCount(), false, false).get();

      assertEquals(getValueCount(), set.size());
      assertEquals(0, map.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBulkFailed() {
    try {
      for (Object v : valueList) {
        mc.asyncSopDelete(key, v, false).get();
      }

      mc.asyncSopInsert(key, "value1", new CollectionAttributes()).get();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncSopPipedInsertBulk(key, valueList,
                      new CollectionAttributes());

      Map<Integer, CollectionOperationStatus> map = future.get(10000,
              TimeUnit.MILLISECONDS);

      assertEquals(1, map.size());
      assertFalse(future.getOperationStatus().isSuccess());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBulkEmptySet() {
    try {
      for (Object v : valueList) {
        mc.asyncSopDelete(key, v, false).get();
      }

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncSopPipedInsertBulk(key, new ArrayList<>(),
                      new CollectionAttributes());

      future.get(10000, TimeUnit.MILLISECONDS);

      Assertions.fail();
    } catch (IllegalArgumentException e) {
      return;
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
    Assertions.fail();
  }
}
