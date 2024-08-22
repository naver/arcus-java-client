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
package net.spy.memcached.collection;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.transcoders.CollectionTranscoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CollectionMaxElementSize extends BaseIntegrationTest {

  private String key = "CollectionMaxElementSize";

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(key).get();
    super.tearDown();
  }

  @Test
  public void testLargeSize() throws Exception {
    int largeSize = 16 * 1024 - 2; //16KB
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < largeSize; i++) {
      sb.append(i % 9);
    }

    String largeValue = sb.toString();

    assertTrue(mc.asyncBopInsert(key, 0, null, largeValue, new CollectionAttributes())
            .get(1000, TimeUnit.MILLISECONDS));
    Map<Long, Element<Object>> m = mc.asyncBopGet(key, 0, 1, ElementFlagFilter.DO_NOT_FILTER, 0,
            1, false, false).get(1000, TimeUnit.MILLISECONDS);
    assertEquals(largeValue, m.get(0L).getValue());
  }

  @Test
  public void testExceed() throws Exception {
    CollectionFuture<Boolean> future;
    future = mc.asyncLopInsert(key, -1, "test", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < CollectionTranscoder.MAX_ELEMENT_BYTES + 1; i++) {
      sb.append(i % 9);
    }

    String tooLargeValue = sb.toString();
    assertEquals(CollectionTranscoder.MAX_ELEMENT_BYTES + 1, tooLargeValue.length());

    try {
      future = mc.asyncLopInsert(key, -1, tooLargeValue,
              new CollectionAttributes());
      fail();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
      assertTrue(e.getMessage().contains("Cannot cache data larger than"));
    }
  }
}
