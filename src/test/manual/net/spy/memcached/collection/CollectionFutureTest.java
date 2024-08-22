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
import net.spy.memcached.ops.OperationStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CollectionFutureTest extends BaseIntegrationTest {

  private String key = "CollectionFutureTest";

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.asyncBopDelete(key, 0, 100, ElementFlagFilter.DO_NOT_FILTER, 0, true).get();
  }

  @Test
  public void testAfterSuccess() throws Exception {
    CollectionFuture<Boolean> future;
    OperationStatus status;

    future = mc.asyncBopInsert(key, 0, null, "hello", new CollectionAttributes());

    // OperationStatus should be null before operation completion
    // status = future.getOperationStatus();
    // assertNull(status);

    // After operation completion (SUCCESS)
    Boolean success = future.get(1000, TimeUnit.MILLISECONDS);
    status = future.getOperationStatus();

    assertTrue(success);
    assertNotNull(status);
    assertTrue(status.isSuccess());
    assertEquals("CREATED_STORED", status.getMessage());
  }

  @Test
  public void testAfterFailure() throws Exception {
    CollectionFuture<Map<Long, Element<Object>>> future;
    OperationStatus status;

    future = mc.asyncBopGet(key, 0, ElementFlagFilter.DO_NOT_FILTER, false, false);

    // OperationStatus should be null before operation completion
    // status = future.getOperationStatus();
    // assertNull(status);

    // After operation completion (FAILURE)
    Map<Long, Element<Object>> result = future.get(1000, TimeUnit.MILLISECONDS);
    status = future.getOperationStatus();

    assertNull(result);
    assertNotNull(status);
    assertFalse(status.isSuccess());
    assertEquals("NOT_FOUND", status.getMessage());
  }
}
