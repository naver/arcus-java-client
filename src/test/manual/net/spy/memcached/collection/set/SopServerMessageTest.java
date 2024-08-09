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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.OperationStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SopServerMessageTest extends BaseIntegrationTest {

  private String key = "SopServerMessageTest";

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.asyncSopDelete(key, "aaa", true).get();
    mc.asyncSopDelete(key, "bbbb", true).get();
  }

  @Test
  public void testNotFound() throws Exception {
    CollectionFuture<Set<Object>> future = mc.asyncSopGet(key, 1, false, false);
    assertNull(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("NOT_FOUND", status.getMessage());
  }

  @Test
  public void testCreatedStored() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("CREATED_STORED", status.getMessage());
  }

  @Test
  public void testStored() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    future = mc.asyncSopInsert(key, "bbbb", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("STORED", status.getMessage());
  }

  @Test
  public void testOverflowed() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    assertTrue(mc.asyncSetAttr(
        key, new CollectionAttributes(null, 1L, CollectionOverflowAction.error))
        .get(1000, TimeUnit.MILLISECONDS));

    future = mc.asyncSopInsert(key, "bbbb", new CollectionAttributes());
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("OVERFLOWED", status.getMessage());
  }

  @Test
  public void testElementExists() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("ELEMENT_EXISTS", status.getMessage());
  }

  @Test
  public void testDeletedDropped() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // delete
    future = mc.asyncSopDelete(key, "aaa", true);
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED_DROPPED", status.getMessage());
  }

  @Test
  public void testDeleted() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // insert
    future = mc.asyncSopInsert(key, "bbbb", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // delete
    future = mc.asyncSopDelete(key, "aaa", false);
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED", status.getMessage());
  }

  @Test
  public void testDeletedDroppedAfterRetrieval() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // get
    CollectionFuture<Set<Object>> future2 = mc.asyncSopGet(key, 1, true, true);
    assertNotNull(future2.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future2.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED_DROPPED", status.getMessage());
  }

  @Test
  public void testDeletedAfterRetrieval() throws Exception {
    // create
    CollectionFuture<Boolean> future = mc.asyncSopInsert(key, "aaa", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // insert
    future = mc.asyncSopInsert(key, "bbbb", new CollectionAttributes());
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));

    // get
    CollectionFuture<Set<Object>> future2 = mc.asyncSopGet(key, 1, true, false);
    assertNotNull(future2.get(1000, TimeUnit.MILLISECONDS));

    OperationStatus status = future2.getOperationStatus();
    assertNotNull(status);
    assertEquals("DELETED", status.getMessage());
  }

}
