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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PipeInsertTest extends BaseIntegrationTest {

  private static final String KEY = PipeInsertTest.class.getSimpleName();

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
  public void testBopPipeInsert() {
    int elementCount = 5000;

    List<Element<Object>> elements = new ArrayList<>();

    for (int i = 0; i < elementCount; i++) {
      elements.add(new Element<>(i, "value" + i,
              new byte[]{(byte) 1}));
    }

    try {
      CollectionAttributes attr = new CollectionAttributes();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncBopPipedInsertBulk(KEY, elements, attr);

      Map<Integer, CollectionOperationStatus> map = future.get(5000L,
              TimeUnit.MILLISECONDS);

      Assertions.assertTrue(map.isEmpty());

      Map<Long, Element<Object>> map3 = mc.asyncBopGet(KEY, 0, 9999,
              ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get();

      Assertions.assertEquals(4000, map3.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testBopPipeInsert2() {
    int elementCount = 5000;
    Map<Long, Object> elements = new TreeMap<>();
    for (long i = 0; i < elementCount; i++) {
      elements.put(i, "value" + i);
    }

    try {
      long start = System.currentTimeMillis();

      CollectionAttributes attr = new CollectionAttributes();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncBopPipedInsertBulk(KEY, elements, attr);

      Map<Integer, CollectionOperationStatus> map = future.get(5000L,
              TimeUnit.MILLISECONDS);

      // System.out.println(System.currentTimeMillis() - start + "ms");

      Assertions.assertTrue(map.isEmpty());

      Map<Long, Element<Object>> map3 = mc.asyncBopGet(KEY, 0, 9999,
              ElementFlagFilter.DO_NOT_FILTER, 0, 0, false, false).get();

      Assertions.assertEquals(4000, map3.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testLopPipeInsert() {
    int elementCount = 5000;

    List<Object> elements = new ArrayList<>(elementCount);

    for (int i = 0; i < elementCount; i++) {
      elements.add("value" + i);
    }

    try {
      long start = System.currentTimeMillis();

      CollectionAttributes attr = new CollectionAttributes();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncLopPipedInsertBulk(KEY, -1, elements, attr);

      Map<Integer, CollectionOperationStatus> map = future.get(5000L,
              TimeUnit.MILLISECONDS);

      // System.out.println(System.currentTimeMillis() - start + "ms");

      Assertions.assertTrue(map.isEmpty());

      List<Object> list = mc.asyncLopGet(KEY, 0, 9999, false, false)
              .get();

      Assertions.assertEquals(4000, list.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testLopPipeInsertIndex() {
    int elementCount = 3;
    List<Object> middleElements = new ArrayList<>(elementCount);
    List<Object> headerElements = new ArrayList<>(elementCount);
    List<Object> footerElements = new ArrayList<>(elementCount);

    for (int i = 0; i < elementCount; i++) {
      middleElements.add("middleValue" + i);
      headerElements.add("headerValue" + i);
      footerElements.add("footerValue" + i);
    }

    try {
      CollectionAttributes attr = new CollectionAttributes();
      mc.asyncLopInsert(KEY, 0, "FirstValue", attr).get(5000L, TimeUnit.MILLISECONDS);
      mc.asyncLopInsert(KEY, -1, "LastValue", attr).get(5000L, TimeUnit.MILLISECONDS);

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future1 =
              mc.asyncLopPipedInsertBulk(KEY, 1, middleElements, attr);
      CollectionFuture<Map<Integer, CollectionOperationStatus>> future2 =
              mc.asyncLopPipedInsertBulk(KEY, 0, headerElements, attr);
      CollectionFuture<Map<Integer, CollectionOperationStatus>> future3 =
              mc.asyncLopPipedInsertBulk(KEY, -1, footerElements, attr);

      Map<Integer, CollectionOperationStatus> map1
              = future1.get(5000L, TimeUnit.MILLISECONDS);
      Map<Integer, CollectionOperationStatus> map2
              = future2.get(5000L, TimeUnit.MILLISECONDS);
      Map<Integer, CollectionOperationStatus> map3
              = future3.get(5000L, TimeUnit.MILLISECONDS);
      Assertions.assertEquals(map1.size() + map2.size() + map3.size(), 0);

      List<Object> list = mc.asyncLopGet(KEY, 0, 9999, false, false).get();
      Assertions.assertEquals((elementCount * 3) + 2, list.size());

      int offset = 0;
      Assertions.assertEquals(headerElements, list.subList(offset, offset + elementCount));
      offset += (elementCount + 1);
      Assertions.assertEquals(middleElements, list.subList(offset, offset + elementCount));
      offset += (elementCount + 1);
      Assertions.assertEquals(footerElements, list.subList(offset, offset + elementCount));
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

  @Test
  public void testMopPipeInsert() {
    int elementCount = 5000;

    Map<String, Object> elements = new TreeMap<>();

    for (int i = 0; i < elementCount; i++) {
      elements.put(String.valueOf(i), "value" + i);
    }

    try {
      CollectionAttributes attr = new CollectionAttributes();

      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncMopPipedInsertBulk(KEY, elements, attr);

      Map<Integer, CollectionOperationStatus> map = future.get(5000L,
              TimeUnit.MILLISECONDS);

      Assertions.assertEquals(1000, map.size());

      Map<String, Object> rmap = mc.asyncMopGet(KEY, false, false)
              .get();

      Assertions.assertEquals(4000, rmap.size());
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
  }

}
