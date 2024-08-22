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
package net.spy.memcached.collection.attribute;

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.CollectionType;
import net.spy.memcached.internal.CollectionFuture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetAttrTest extends BaseIntegrationTest {

  private final String[] list = {"hello1", "hello2", "hello3"};

  @Test
  public void testGetAttr_KV() throws Exception {
    String key = "testGetAttr_KV";

    mc.set(key, 100, "v").get();

    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);

    assertNotNull(rattrs);
    assertEquals(Integer.valueOf(0), rattrs.getFlags());
    assertEquals(CollectionType.kv, rattrs.getType());
  }

  @Test
  public void testGetAttr_ModifiedAttribute() throws Exception {
    String key = "getattr_modified_attribute";

    addToList(key, list);

    // Set attributes
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(1000);
    attrs.setExpireTime(10000);
    assertTrue(mc.asyncSetAttr(key, attrs).get(1000, TimeUnit.MILLISECONDS));

    // 3. Get attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);

    assertNotNull(rattrs);
    assertEquals(attrs.getMaxCount(), rattrs.getMaxCount());
    // assertEquals(attrs.getExpireTime(), rattrs.getExpireTime());
    assertEquals(CollectionType.list, rattrs.getType());

    deleteList(key, list.length);
  }

  @Test
  public void testGetAttr_DefaultAttribute() throws Exception {
    String key = "getattr_default_attribute";

    addToList(key, list);

    // Get attributes
    CollectionAttributes rattrs = mc.asyncGetAttr(key).get(1000,
            TimeUnit.MILLISECONDS);

    assertEquals(CollectionAttributes.DEFAULT_FLAGS, rattrs.getFlags());
    assertEquals(CollectionAttributes.DEFAULT_EXPIRETIME,
            rattrs.getExpireTime());
    assertEquals(CollectionAttributes.DEFAULT_MAXCOUNT,
            rattrs.getMaxCount());
    assertEquals(CollectionAttributes.DEFAULT_OVERFLOWACTION,
            rattrs.getOverflowAction());

    deleteList(key, list.length);
  }

  @Test
  public void testGetAttr_KeyNotFound() throws Exception {
    CollectionFuture<CollectionAttributes> future = mc
            .asyncGetAttr("NOT_EXISTS");

    CollectionAttributes rattrs = future.get(1000, TimeUnit.MILLISECONDS);

    assertNull(rattrs);
    assertEquals(CollectionResponse.NOT_FOUND, future.getOperationStatus()
            .getResponse());
  }
}
