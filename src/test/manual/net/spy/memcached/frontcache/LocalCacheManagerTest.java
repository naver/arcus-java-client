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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import net.spy.memcached.CASValue;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.junit.Ignore;

import junit.framework.TestCase;
import net.spy.memcached.ArcusClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.plugin.LocalCacheManager;
import net.spy.memcached.transcoders.Transcoder;

@Ignore
public class LocalCacheManagerTest extends TestCase {

  private ArcusClient client;

  // put keys
  private final String[] keys = {
      "key0", "key1", "key2", "key3", "key4",
      "key5", "key6", "key7", "key8", "key9"
  };

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    cfb.setFrontCacheExpireTime(5);
    cfb.setMaxFrontCacheElements(10);
    client = ArcusClient.createArcusClient("127.0.0.1:2181", "test", cfb);
  }

  @Override
  protected void tearDown() throws Exception {
    if (client != null) {
      client.shutdown();
    }
    super.tearDown();
  }

  public void testGet() throws Exception {
    for (String k : keys) {
      client.set(k, 2, k + "_value").get();
    }

    Future<Object> f = client.asyncGet(keys[0]);
    Object result = f.get();

    Transcoder<Object> tc = null;
    Object cached = client.getLocalCacheManager().get(keys[0], tc);

    assertNotNull(result);
    assertNotNull(cached);
    assertSame("not the same result", result, cached);

    // after 3 seconds : remote expired, locally cached
    Thread.sleep(3000);

    f = client.asyncGet(keys[0]);
    result = f.get();

    cached = client.getLocalCacheManager().get(keys[0], tc);

    assertNotNull(result);
    assertNotNull(cached);
    assertEquals("not the same result", result, cached);

    // after another 3 seconds : both remote and local expired
    Thread.sleep(3000);

    f = client.asyncGet(keys[0]);
    result = f.get();

    cached = client.getLocalCacheManager().get(keys[0], tc);

    assertNull(result);
    assertNull(cached);
  }

  public void testGets() throws Exception {
    List<String> valueList = new ArrayList<String>();
    for (String k : keys) {
      String value = k + "_value";
      client.set(k, 2, value).get();
      valueList.add(value);
    }

    OperationFuture<CASValue<Object>> f = client.asyncGets(keys[0]);
    GetFuture<Object> future = client.asyncGet(keys[0]);

    Object noResult = f.get();
    Object result = future.get();

    Transcoder<Object> tc = null;
    Object cached = client.getLocalCacheManager().get(keys[0], tc);

    assertNotSame("the same result", noResult, cached);
    assertSame("not the same result", result, cached);

    // after 3 seconds : remote expired, locally cached
    Thread.sleep(3000);

    // but we have locally cached results.
    // Gets save value without CAS in local cache
    // so you can find only value
    f = client.asyncGets(keys[0]);
    future = client.asyncGet(keys[0]);

    noResult = f.get();
    result = future.get();

    cached = client.getLocalCacheManager().get(keys[0], tc);
    assertNotSame("the same result", noResult, cached);
    assertEquals("not the same result", result, cached);

    // after another 3 seconds : both remote and local expired
    Thread.sleep(3000);

    f = client.asyncGets(keys[0]);
    future = client.asyncGet(keys[0]);
    noResult = f.get();
    result = future.get();

    cached = client.getLocalCacheManager().get(keys[0], tc);

    assertNull(noResult);
    assertNull(result);
    assertNull(cached);
  }

  public void testGetBulk() throws Exception {
    for (String k : keys) {
      client.set(k, 2, k + "_value").get();
    }

    // read-through.
    Map<String, Object> result = client.getBulk(keys);

    // expecting that the keys are locally cached.
    LocalCacheManager lcm = client.getLocalCacheManager();
    for (String k : keys) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNotNull(got);
    }

    // after 3 seconds, all keys should be expired.
    Thread.sleep(3000);

    // but we have locally cached results.
    result = client.getBulk(keys);
    assertNotNull(result);
    assertTrue(keys.length == result.size());

    // then after additional 3 seconds, locally cached results should be
    // expired.
    Thread.sleep(3000);

    for (String k : keys) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNull(got);
    }

    result = client.getBulk(keys);
    assertNotNull(result);
    assertTrue(0 == result.size());
  }

  public void testGetsBulk() throws Exception {
    for (String k : keys) {
      client.set(k, 2, k + "_value").get();
    }

    // read-through.
    Map<String, CASValue<Object>> noResult = client.getsBulk(keys);

    // expecting that the keys are locally cached.
    LocalCacheManager lcm = client.getLocalCacheManager();
    for (String k : keys) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNotNull(got);
    }

    // after 3 seconds, all keys should be expired.
    Thread.sleep(3000);

    // but we have locally cached results.
    // GetsBulk save value without CAS in local cache
    // so you can find only value
    Map<String, Object> result = client.getBulk(keys);
    noResult = client.getsBulk(keys);

    assertTrue(keys.length == result.size());
    assertTrue(0 == noResult.size());

    // then after additional 3 seconds, locally cached results should be
    // expired.
    Thread.sleep(3000);

    for (String k : keys) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNull(got);
    }

    result = client.getBulk(keys);
    noResult = client.getsBulk(keys);

    assertTrue(0 == result.size());
    assertTrue(0 == result.size());
  }

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
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNotNull(got);
    }

    // after 3 seconds, all keys should be expired.
    Thread.sleep(3000);

    // but we have locally cached results.
    f = client.asyncGetBulk(keys);
    result = f.get();
    assertNotNull(result);
    assertTrue(keys.length == result.size());

    // then after additional 3 seconds, locally cached results should be
    // expired.
    Thread.sleep(3000);

    for (String k : keys) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNull(got);
    }

    f = client.asyncGetBulk(keys);
    result = f.get();
    assertNotNull(result);
    assertTrue(0 == result.size());
  }

  public void testBulkPartial() throws Exception {
    String keySet1[] = new String[keys.length / 2];
    String keySet2[] = new String[keys.length / 2];

    for (int i = 0; i < keys.length / 2; i++) {
      keySet1[i] = keys[i * 2];
      keySet2[i] = keys[i * 2 + 1];
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
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
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
    assertTrue(keys.length == result.size());

    // then after additional 3 seconds, locally cached Set 1 should be
    // expired.
    Thread.sleep(3000);

    for (String k : keySet1) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNull(got);
    }

    f = client.asyncGetBulk(keys);
    result = f.get();
    assertNotNull(result);
    assertTrue(keySet2.length == result.size());
  }

  public void testAsyncGetsBulk() throws Exception {
    for (String k : keys) {
      client.set(k, 2, k + "_value").get();
    }

    // read-through.
    BulkFuture<Map<String, CASValue<Object>>> f = client.asyncGetsBulk(keys);
    f.get();

    // expecting that the keys are locally cached.
    LocalCacheManager lcm = client.getLocalCacheManager();
    for (String k : keys) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNotNull(got);
    }

    // after 3 seconds, all keys should be expired.
    Thread.sleep(3000);

    // but we have locally cached results.
    // asyncGetsBulk save value without CAS in local cache
    // so you can find only value
    f = client.asyncGetsBulk(keys);
    BulkFuture<Map<String, Object>> future = client.asyncGetBulk(keys);
    Map<String, Object> result = future.get();
    Map<String, CASValue<Object>> noResult = f.get();

    assertEquals(noResult.size(), 0);
    assertTrue(keys.length == result.size());

    // then after additional 3 seconds, locally cached results should be
    // expired.
    Thread.sleep(3000);

    for (String k : keys) {
      Transcoder<Object> tc = null;
      Object got = lcm.get(k, tc);
      assertNull(got);
    }

    f = client.asyncGetsBulk(keys);
    future = client.asyncGetBulk(keys);
    result = future.get();
    noResult = f.get();

    assertTrue(0 == result.size());
    assertTrue(0 == noResult.size());
  }

}
