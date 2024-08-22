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
package net.spy.memcached.collection.btree;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.OperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BopServerMessageTest extends BaseIntegrationTest {

  private String key = "BopServerMessageTest";

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.asyncBopDelete(
        key, 0, 100, ElementFlagFilter.DO_NOT_FILTER, 0, true).get();
  }

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testNotFound() throws Exception {
    CollectionFuture<Map<Long, Element<Object>>> future = mc.asyncBopGet(
            key, 0, ElementFlagFilter.DO_NOT_FILTER, false, false);
    assertNull(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("NOT_FOUND", status.getMessage());
  }

  @Test
  public void testNotFoundElement() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, 0, new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    CollectionFuture<Map<Long, Element<Object>>> future2 = mc.asyncBopGet(
            key, 1, ElementFlagFilter.DO_NOT_FILTER, false, false);
    assertNotNull(future2.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future2.getOperationStatus();
    assertNotNull(status);
    assertEquals("NOT_FOUND_ELEMENT", status.getMessage());
  }

  @Test
  public void testCreatedStored() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, 0, new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("CREATED_STORED", status.getMessage());
  }

  @Test
  public void testStored() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, 0, new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    future = mc.asyncBopInsert(key, 1, null, 1, new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("STORED", status.getMessage());
  }

  @Test
  public void testOutOfRange() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 1, null, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    assertTrue(mc.asyncSetAttr(key, new CollectionAttributes(
        null, 1L, CollectionOverflowAction.largest_trim))
            .get(1000, TimeUnit.MILLISECONDS));

    future = mc.asyncBopInsert(key, 2, null, "bbbb", new CollectionAttributes());
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("OUT_OF_RANGE", status.getMessage());
  }

  @Test
  public void testOverflowed() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    assertTrue(mc.asyncSetAttr(key, new CollectionAttributes(
        null, 1L, CollectionOverflowAction.error))
        .get(1000, TimeUnit.MILLISECONDS));

    future = mc.asyncBopInsert(key, 1, null, "aaa", new CollectionAttributes());
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("OVERFLOWED", status.getMessage());
  }

  @Test
  public void testElementExists() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // insert an item with same bkey
    future = mc.asyncBopInsert(key, 0, null, "bbbb", new CollectionAttributes());
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("ELEMENT_EXISTS", status.getMessage());
  }

  @Test
  public void testDeletedDropped() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // delete
    future = mc.asyncBopDelete(key, 0, ElementFlagFilter.DO_NOT_FILTER, true);
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED_DROPPED", status.getMessage());
  }

  @Test
  public void testDeleted() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // insert
    future = mc.asyncBopInsert(key, 1, null, "bbbb", null);
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // delete
    future = mc.asyncBopDelete(key, 0, ElementFlagFilter.DO_NOT_FILTER, false);
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED", status.getMessage());
  }

  @Test
  public void testDeletedDroppedAfterRetrieval() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // get
    CollectionFuture<Map<Long, Element<Object>>> future2 = mc.asyncBopGet(
        key, 0, ElementFlagFilter.DO_NOT_FILTER, true, true);
    assertNotNull(future2.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future2.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED_DROPPED", status.getMessage());
  }

  @Test
  public void testDeletedAfterRetrieval() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncBopInsert(
        key, 0, null, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // insert
    future = mc.asyncBopInsert(key, 1, null, "bbbb", null);
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // get
    CollectionFuture<Map<Long, Element<Object>>> future2 = mc.asyncBopGet(
            key, 0, ElementFlagFilter.DO_NOT_FILTER, true, true);
    assertNotNull(future2.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future2.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED", status.getMessage());
  }

}
