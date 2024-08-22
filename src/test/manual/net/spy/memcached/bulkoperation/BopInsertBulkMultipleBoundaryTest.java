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
package net.spy.memcached.bulkoperation;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopInsertBulkMultipleBoundaryTest extends BaseIntegrationTest {

  @Test
  public void testBopGet_Overflow() throws Exception {
    String key = "MyBopOverflowtestKey23";
    String value = "MyValue";

    // delete b+tree
    mc.asyncBopDelete(key, 0, 10000, ElementFlagFilter.DO_NOT_FILTER, 0,
            true).get();

    // Create a B+ Tree
    mc.asyncBopInsert(key, 0, null, "item0", new CollectionAttributes()).get();

    int maxcount = 10;

    // Set maxcount
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(maxcount);
    attrs.setOverflowAction(CollectionOverflowAction.error);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // generate bkey
    Map<Long, Object> bkeys = new TreeMap<>();
    for (int i = 1; i <= maxcount; i++) {
      bkeys.put((long) i, value);
    }

    // SET
    Future<Map<Integer, CollectionOperationStatus>> future = mc
            .asyncBopPipedInsertBulk(key, bkeys, new CollectionAttributes());
    try {
      Map<Integer, CollectionOperationStatus> errorList = future.get(
              20000L, TimeUnit.MILLISECONDS);
      assertEquals(1, errorList.size(), "Failed count is not 1.");
    } catch (TimeoutException e) {
      future.cancel(true);
      e.printStackTrace();
    }
  }
}
