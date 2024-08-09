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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopPipeUpdateTest extends BaseIntegrationTest {

  private static final String KEY = BopPipeUpdateTest.class.getSimpleName();
  private static final int elementCount = 1200;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(KEY).get();
    Assertions.assertNull(mc.asyncGetAttr(KEY).get());

    List<Element<Object>> elements = new ArrayList<>();

    for (int i = 0; i < elementCount; i++) {
      elements.add(new Element<>(i, "value" + i, new byte[]{1, 1, 1, 1}));
    }

    // long start = System.currentTimeMillis();

    CollectionAttributes attr = new CollectionAttributes();
    attr.setMaxCount(10000L);

    CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
            .asyncBopPipedInsertBulk(KEY, elements, attr);

    Map<Integer, CollectionOperationStatus> map = future.get(5000L,
            TimeUnit.MILLISECONDS);

    // System.out.println(System.currentTimeMillis() - start + "ms");

    assertTrue(map.isEmpty());

    Map<Long, Element<Object>> map3 = mc.asyncBopGet(KEY, 0, 9999,
            ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get();

    assertEquals(elementCount, map3.size());
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(KEY).get();
    super.tearDown();
  }

  @Test
  public void testBopPipeUpdateValue() {

    List<Element<Object>> updateElements = new ArrayList<>();
    for (int i = 0; i < elementCount; i++) {
      updateElements.add(new Element<>(i, "updated" + i,
              new ElementFlagUpdate(new byte[]{1, 1, 1, 1})));
    }

    try {
      // long start = System.currentTimeMillis();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 = mc
              .asyncBopPipedUpdateBulk(KEY, updateElements);

      Map<Integer, CollectionOperationStatus> map2 = future2.get(5000L,
              TimeUnit.MILLISECONDS);

      // System.out.println(System.currentTimeMillis() - start + "ms");
      // System.out.println(map2.size());
      assertTrue(map2.isEmpty());

      for (long i = 0; i < elementCount; i++) {
        assertEquals(
                "updated" + i,
                mc.asyncBopGet(KEY, i, ElementFlagFilter.DO_NOT_FILTER,
                        false, false).get(1000L, TimeUnit.MILLISECONDS)
                        .get(i).getValue());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

  }

  @Test
  public void testBopPipeUpdateEFlags() {

    byte[] NEW_BYTE_EFLAG = new byte[]{1, 1};

    List<Element<Object>> updateElements = new ArrayList<>();
    for (int i = 0; i < elementCount; i++) {
      updateElements.add(new Element<>(i, null,
              new ElementFlagUpdate(1, BitWiseOperands.AND,
                      NEW_BYTE_EFLAG)));
    }

    try {
      // long start = System.currentTimeMillis();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 = mc
              .asyncBopPipedUpdateBulk(KEY, updateElements);

      Map<Integer, CollectionOperationStatus> map2 = future2.get(5000L,
              TimeUnit.MILLISECONDS);

      // System.out.println(System.currentTimeMillis() - start + "ms");
      // System.out.println(map2.size());
      assertTrue(map2.isEmpty());

      for (long i = 0; i < elementCount; i++) {
        Element<Object> element = mc
                .asyncBopGet(KEY, i, ElementFlagFilter.DO_NOT_FILTER,
                        false, false).get(1000L, TimeUnit.MILLISECONDS)
                .get(i);

        // System.out.println(element.getFlagByHex());
        assertEquals("value" + i, element.getValue());
        assertEquals("0x01010101", element.getStringEFlag());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopPipeUpdateEFlagsReset() {

    List<Element<Object>> updateElements = new ArrayList<>();
    for (int i = 0; i < elementCount; i++) {
      updateElements.add(new Element<>(i, null,
              ElementFlagUpdate.RESET_FLAG));
    }

    try {
      // long start = System.currentTimeMillis();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 = mc
              .asyncBopPipedUpdateBulk(KEY, updateElements);

      Map<Integer, CollectionOperationStatus> map2 = future2.get(5000L,
              TimeUnit.MILLISECONDS);

      // System.out.println(System.currentTimeMillis() - start + "ms");
      // System.out.println(map2.size());
      assertTrue(map2.isEmpty());

      for (long i = 0; i < elementCount; i++) {
        Element<Object> element = mc
                .asyncBopGet(KEY, i, ElementFlagFilter.DO_NOT_FILTER,
                        false, false).get(1000L, TimeUnit.MILLISECONDS)
                .get(i);

        // System.out.println(element.getFlagByHex());
        assertEquals("value" + i, element.getValue());
        assertEquals("", element.getStringEFlag());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopPipeUpdateNotFoundElement() {

    try {
      assertTrue(mc.asyncBopDelete(KEY, 0L, 1000L,
              ElementFlagFilter.DO_NOT_FILTER, 600, false).get(1000L,
              TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

    List<Element<Object>> updateElements = new ArrayList<>();
    for (int i = 0; i < elementCount; i++) {
      updateElements.add(new Element<>(i, "updated" + i,
              new ElementFlagUpdate(new byte[]{1, 1, 1, 1})));
    }

    try {
      // long start = System.currentTimeMillis();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 = mc
              .asyncBopPipedUpdateBulk(KEY, updateElements);

      Map<Integer, CollectionOperationStatus> map2 = future2.get(5000L,
              TimeUnit.MILLISECONDS);

      // System.out.println(System.currentTimeMillis() - start + "ms");

      assertEquals(600, map2.size());
      assertEquals(CollectionResponse.NOT_FOUND_ELEMENT, map2.get(0)
              .getResponse());

      for (long i = 600; i < elementCount; i++) {
        assertEquals(
                "updated" + i,
                mc.asyncBopGet(KEY, i, ElementFlagFilter.DO_NOT_FILTER,
                        false, false).get(1000L, TimeUnit.MILLISECONDS)
                        .get(i).getValue());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

  }

  @Test
  public void testBopPipeUpdateNotFoundKey() {

    String key2 = "NEW_BopPipeUpdateTest";

    List<Element<Object>> updateElements = new ArrayList<>();
    for (int i = 0; i < elementCount; i++) {
      updateElements.add(new Element<>(i, "updated" + i,
              new ElementFlagUpdate(new byte[]{1, 1, 1, 1})));
    }

    try {
      // long start = System.currentTimeMillis();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 = mc
              .asyncBopPipedUpdateBulk(key2, updateElements);

      Map<Integer, CollectionOperationStatus> map2 = future2.get(5000L,
              TimeUnit.MILLISECONDS);

      // System.out.println(System.currentTimeMillis() - start + "ms");

      assertEquals(elementCount, map2.size());
      assertEquals(CollectionResponse.NOT_FOUND, map2.get(0)
              .getResponse());

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }

  }
}
