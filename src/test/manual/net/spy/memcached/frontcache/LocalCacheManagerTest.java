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
package net.spy.memcached.frontcache;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.plugin.LocalCacheManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class LocalCacheManagerTest {

  private ArcusClient client;

  private final List<String> keys =
      Arrays.asList("key0" + System.nanoTime(),
          "key1" + System.nanoTime(), "key2" + System.nanoTime(),
          "key3" + System.nanoTime(), "key4" + System.nanoTime(),
          "key5" + System.nanoTime(), "key6" + System.nanoTime(),
          "key7" + System.nanoTime(), "key8" + System.nanoTime(),
          "key9" + System.nanoTime());

  @BeforeEach
  protected void setUp() throws Exception {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    cfb.setFrontCacheExpireTime(5);
    cfb.setMaxFrontCacheElements(10);
    client = ArcusClient.createArcusClient("127.0.0.1:2181", "test", cfb);
  }

  @AfterEach
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
  }

  @Test
  public void testGet() throws Exception {
    for (String k : keys) {
      client.set(k, 2, k + "_value").get();
    }

    String key = keys.get(0);

    Future<Object> f = client.asyncGet(key);
    Object result = f.get();

    Object cached = client.getLocalCacheManager().get(key);

    assertNotNull(result);
    assertNotNull(cached);
    assertSame(result, cached, "not the same result");

    // after 3 seconds : remote expired, locally cached
    Thread.sleep(3000);

    f = client.asyncGet(key);
    result = f.get();

    cached = client.getLocalCacheManager().get(key);

    assertNotNull(result);
    assertNotNull(cached);
    assertEquals(result, cached, "not the same result");

    // after another 3 seconds : both remote and local expired
    Thread.sleep(3000);

    f = client.asyncGet(key);
    result = f.get();

    cached = client.getLocalCacheManager().get(key);

    assertNull(result);
    assertNull(cached);
  }

  @Test
  public void testGetBulk() throws Exception {
    for (String k : keys) {
      client.set(k, 2, k + "_value").get();
    }

    // read-through.
    Map<String, Object> result = client.getBulk(keys);

    // expecting that the keys are locally cached.
    LocalCacheManager lcm = client.getLocalCacheManager();
    for (String k : keys) {
      Object got = lcm.get(k);
      assertNotNull(got);
    }

    // after 3 seconds, all keys should be expired.
    Thread.sleep(3000);

    // but we have locally cached results.
    result = client.getBulk(keys);
    assertNotNull(result);
    assertEquals(keys.size(), result.size());

    // then after additional 3 seconds, locally cached results should be
    // expired.
    Thread.sleep(3000);

    for (String k : keys) {
      Object got = lcm.get(k);
      assertNull(got);
    }

    result = client.getBulk(keys);
    assertNotNull(result);
    assertEquals(0, result.size());
  }

  @Test
  public void testAsyncGetBulk() throws Exception {
    for (String k : keys) {
      client.set(k, 2, k + "_value").get();
    }

    // read-through.
    BulkFuture<Map<String, Object>> f = client.asyncGetBulk(keys);
    Map<String, Object> result = f.get();

    // expecting that the keys are locally cached.
    LocalCacheManager lcm = client.getLocalCacheManager();
    for (String k : keys) {
      Object got = lcm.get(k);
      assertNotNull(got);
    }

    // after 3 seconds, all keys should be expired.
    Thread.sleep(3000);

    // but we have locally cached results.
    f = client.asyncGetBulk(keys);
    result = f.get();
    assertNotNull(result);
    assertEquals(keys.size(), result.size());

    // then after additional 3 seconds, locally cached results should be
    // expired.
    Thread.sleep(3000);

    for (String k : keys) {
      Object got = lcm.get(k);
      assertNull(got);
    }

    f = client.asyncGetBulk(keys);
    result = f.get();
    assertNotNull(result);
    assertEquals(0, result.size());
  }

  @Test
  public void testBulkPartial() throws Exception {
    String[] keySet1 = new String[keys.size() / 2];
    String[] keySet2 = new String[keys.size() / 2];

    for (int i = 0; i < keys.size() / 2; i++) {
      keySet1[i] = keys.get(i * 2);
      keySet2[i] = keys.get(i * 2 + 1);
    }

    // Set 1
    for (String k : keySet1) {
      client.set(k, 2, k + "_value").get();
    }

    // read-through.
    BulkFuture<Map<String, Object>> f = client.asyncGetBulk(keys);
    Map<String, Object> result = f.get();

    // expecting that the keys are locally cached.
    LocalCacheManager lcm = client.getLocalCacheManager();
    for (String k : keySet1) {
      Object got = lcm.get(k);
      assertNotNull(got);
    }

    // after 3 seconds, put another set of keys
    Thread.sleep(3000);

    // Set 2
    for (String k : keySet2) {
      client.set(k, 4, k + "_value").get();
    }

    // Set 1 : locally cached
    // Set 2 : from the remote cache
    f = client.asyncGetBulk(keys);
    result = f.get();
    assertNotNull(result);
    assertEquals(keys.size(), result.size());

    // then after additional 3 seconds, locally cached Set 1 should be
    // expired.
    Thread.sleep(3000);

    for (String k : keySet1) {
      Object got = lcm.get(k);
      assertNull(got);
    }

    f = client.asyncGetBulk(keys);
    result = f.get();
    assertNotNull(result);
    assertEquals(keySet2.length, result.size());
  }

}
