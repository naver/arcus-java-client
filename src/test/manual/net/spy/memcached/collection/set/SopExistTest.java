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

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SopExistTest extends BaseIntegrationTest {

  private final String key = "SopExistTest";
  private final String value = "value";

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.delete(key).get();
  }

  @Test
  public void testExist() throws Exception {
    Boolean result = mc.asyncSopInsert(key, value,
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS);
    assertTrue(result);

    CollectionFuture<Boolean> future = mc.asyncSopExist(key, value);
    assertTrue(future.get(1000, TimeUnit.MILLISECONDS));
    assertEquals(CollectionResponse.EXIST, future.getOperationStatus()
            .getResponse());
  }

  @Test
  public void testNotExist() throws Exception {
    Boolean result = mc.asyncSopInsert(key, value,
            new CollectionAttributes()).get(1000, TimeUnit.MILLISECONDS);
    assertTrue(result);

    CollectionFuture<Boolean> future = mc.asyncSopExist(key, "dummy");
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));
    assertEquals(CollectionResponse.NOT_EXIST, future.getOperationStatus()
            .getResponse());
  }

  @Test
  public void testNotFound() throws Exception {
    CollectionFuture<Boolean> future = mc.asyncSopExist(key, value);
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));
    assertEquals(CollectionResponse.NOT_FOUND, future.getOperationStatus()
            .getResponse());
  }

  @Test
  public void testUnreadabled() throws Exception {
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setReadable(false);

    Boolean result = mc.asyncSopInsert(key, value, attrs).get(1000,
            TimeUnit.MILLISECONDS);
    assertTrue(result);

    CollectionFuture<Boolean> future = mc.asyncSopExist(key, "dummy");
    assertFalse(future.get(1000, TimeUnit.MILLISECONDS));
    assertEquals(CollectionResponse.UNREADABLE, future.getOperationStatus()
            .getResponse());
  }
}
