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
package net.spy.memcached;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.compat.SyncThread;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.junit.jupiter.api.Test;

import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class ProtocolBaseCase extends ClientBaseCase {

  @Test
  public void testAssertions() {
    boolean caught = false;
    try {
      assert false;
    } catch (AssertionError e) {
      caught = true;
    }
    assertTrue(caught, "Assertions are not enabled!");
  }

  @Test
  public void testGetStats() throws Exception {
    Map<SocketAddress, Map<String, String>> stats = client.getStats();
    System.out.println("Stats:  " + stats);
    assertEquals(client.getAllNodes().size(), stats.size());
    Map<String, String> oneStat = stats.values().iterator().next();
    assertTrue(oneStat.containsKey("total_items"));
  }

  @Test
  public void testGetStatsSlabs() throws Exception {
    // There needs to at least have been one value set or there may be
    // no slabs to check.
    assertTrue(client.set("slabinitializer", 0, "hi").get());
    Map<SocketAddress, Map<String, String>> stats = client.getStats("slabs");
    System.out.println("Stats:  " + stats);
    assertEquals(client.getAllNodes().size(), stats.size());
    Map<String, String> oneStat = stats.values().iterator().next();
    assertTrue(oneStat.containsKey("0:chunk_size"));
  }

  @Test
  public void testGetStatsSizes() throws Exception {
    // Arcus does not support "stats sizes"
    if (true) {
      return;
    }
    // There needs to at least have been one value set or there may be
    // no sizes to check.
    assertTrue(client.set("sizeinitializer", 0, "hi").get());
    Map<SocketAddress, Map<String, String>> stats = client.getStats("sizes");
    System.out.println("Stats:  " + stats);
    assertEquals(client.getAllNodes().size(), stats.size());
    Map<String, String> oneStat = stats.values().iterator().next();
    assertEquals("1", oneStat.get("96"));
  }

  @Test
  public void testGetStatsCacheDump() throws Exception {
    // There needs to at least have been one value set or there
    // won't be anything to dump
    assertTrue(client.set("dumpinitializer", 0, "hi").get());
    Map<SocketAddress, Map<String, String>> stats = client.getStats("cachedump 0 10000");
    System.out.println("Stats:  " + stats);
    assertEquals(client.getAllNodes().size(), stats.size());
    Map<String, String> oneStat = stats.values().iterator().next();
    String val = oneStat.get("dumpinitializer");
    assertTrue(val.matches("\\[acctime=\\d+, exptime=\\d+\\]"), val + "doesn't match");
  }

  @Test
  public void testDelayedFlush() throws Exception {
    assertNull(client.get("test1"));
    assertTrue(client.set("test1", 5, "test1value").get());
    assertTrue(client.set("test2", 5, "test2value").get());
    assertEquals("test1value", client.get("test1"));
    assertEquals("test2value", client.get("test2"));
    assertTrue(client.flush(2).get());
    Thread.sleep(2100);
    assertNull(client.get("test1"));
    assertNull(client.get("test2"));
  }

  @Test
  public void testNoop() {
    // This runs through the startup/flush cycle
  }

  @Test
  public void testDoubleShutdown() {
    client.shutdown();
    client.shutdown();
  }

  @Test
  public void testSimpleGet() throws Exception {
    assertNull(client.get("test1"));
    assertTrue(client.set("test1", 5, "test1value").get());
    assertEquals("test1value", client.get("test1"));
  }

  @Test
  public void testSimpleCASGets() throws Exception {
    assertNull(client.gets("test1"));
    assertTrue(client.set("test1", 5, "test1value").get());
    assertEquals("test1value", client.gets("test1").getValue());
  }

  @Test
  public void testCAS() throws Exception {
    final String key = "castestkey";
    // First, make sure it doesn't work for a non-existing value.
    assertSame(CASResponse.NOT_FOUND,
            client.cas(key, 0x7fffffffffL, "bad value"),
            "Expected error CASing with no existing value.");

    // OK, stick a value in here.
    assertTrue(client.add(key, 5, "original value").get());
    CASValue<?> getsVal = client.gets(key);
    assertEquals("original value", getsVal.getValue());

    // Now try it with an existing value, but wrong CAS id
    assertSame(CASResponse.EXISTS,
            client.cas(key, getsVal.getCas() + 1, "broken value"),
            "Expected error CASing with invalid id");
    // Validate the original value is still in tact.
    assertEquals("original value", getsVal.getValue());

    // OK, now do a valid update
    assertSame(CASResponse.OK,
            client.cas(key, getsVal.getCas(), "new value"),
            "Expected successful CAS with correct id ("
                        + getsVal.getCas() + ")");
    assertEquals("new value", client.get(key));

    // Test a CAS replay
    assertSame(CASResponse.EXISTS,
            client.cas(key, getsVal.getCas(), "crap value"),
            "Expected unsuccessful CAS with replayed id");
    assertEquals("new value", client.get(key));
  }

  @Test
  public void testReallyLongCASId() throws Exception {
    String key = "this-is-my-key";
    assertSame(CASResponse.NOT_FOUND,
            client.cas(key, 9223372036854775807L, "bad value"),
            "Expected error CASing with no existing value.");
  }

  @Test
  public void testExtendedUTF8Key() throws Exception {
    String key = "\u2013\u00ba\u2013\u220f\u2014\u00c4";
    assertNull(client.get(key));
    assertTrue(client.set(key, 5, "test1value").get());
    assertEquals("test1value", client.get(key));
  }

  @Test
  public void testInvalidKey1() throws Exception {
    try {
      client.get("key with spaces");
      fail("Expected IllegalArgumentException getting key with spaces");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testInvalidKey2() throws Exception {
    try {
      StringBuilder longKey = new StringBuilder();
      // MAX_KEY_LENGTH is 4000.
      for (int i = 0; i <= 4000; i++) {
        longKey.append("a");
      }
      client.get(longKey.toString());
      fail("Expected IllegalArgumentException getting too long of a key");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testInvalidKey3() throws Exception {
    try {
      Object val = client.get("Key\n");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testInvalidKey4() throws Exception {
    try {
      Object val = client.get("Key\r");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testInvalidKey5() throws Exception {
    try {
      Object val = client.get("Key\0");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testInvalidKeyBlank() throws Exception {
    try {
      Object val = client.get("");
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testInvalidKeyBulk() throws Exception {
    try {
      Object val = client.getBulk(Collections.singletonList("Key key2"));
      fail("Expected IllegalArgumentException, got " + val);
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testParallelSetGet() throws Throwable {
    int cnt = SyncThread.getDistinctResultCount(10, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        int size = 10;
        List<String> keys = new ArrayList<>(size);
        List<String> values = new ArrayList<>(size);
        List<Future<Boolean>> setFutures = new ArrayList<>(size);
        List<GetFuture<Object>> getFutures = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
          keys.add("test" + i);
          values.add("value" + i);
          setFutures.add(client.set(keys.get(i), 60, values.get(i)));
        }
        for (int i = 0; i < size; i++) {
          assertTrue(setFutures.get(i).get());
        }

        for (int i = 0; i < size; i++) {
          getFutures.add(client.asyncGet(keys.get(i)));
        }
        for (int i = 0; i < size; i++) {
          assertEquals(getFutures.get(i).get(), values.get(i));
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(1, cnt);
  }

  @Test
  public void testParallelSetMultiGet() throws Throwable {
    int cnt = SyncThread.getDistinctResultCount(10, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        int size = 10;
        List<String> keys = new ArrayList<>(size);
        List<String> values = new ArrayList<>(size);
        List<Future<Boolean>> futures = new ArrayList<>(size);

        for (int i = 0; i < 10; i++) {
          keys.add("test" + i);
          values.add("value" + i);
          futures.add(client.set(keys.get(i), 60, values.get(i)));
        }
        for (int i = 0; i < size; i++) {
          assertTrue(futures.get(i).get());
          assertEquals(client.get(keys.get(i)), values.get(i));
        }

        Map<String, Object> m = client.getBulk(Arrays.asList("test0", "test1", "test2",
                "test3", "test4", "test5", "test6", "test7", "test8",
                "test9", "test10")); // Yes, I intentionally ran over.
        for (int i = 0; i < 10; i++) {
          assertEquals("value" + i, m.get("test" + i));
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(1, cnt);
  }

  @Test
  public void testParallelSetAutoMultiGet() throws Throwable {
    int cnt = SyncThread.getDistinctResultCount(10, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        assertTrue(client.set("testparallel", 60, "parallelvalue").get());
        for (int i = 0; i < 10; i++) {
          assertEquals(client.get("testparallel"), "parallelvalue");
        }
        return Boolean.TRUE;
      }
    });
    assertEquals(1, cnt);
  }

  @Test
  public void testAdd() throws Exception {
    assertNull(client.get("test1"));
    assertTrue(client.set("test1", 5, "test1value").get());
    assertEquals("test1value", client.get("test1"));
    assertFalse(client.add("test1", 5, "ignoredvalue").get());
    // Should return the original value
    assertEquals("test1value", client.get("test1"));
  }

  @Test
  public void testAddWithTranscoder() throws Exception {
    Transcoder<String> t = new TestTranscoder();
    assertNull(client.get("test1", t));
    assertTrue(client.set("test1", 5, "test1value", t).get());
    assertEquals("test1value", client.get("test1", t));
    assertFalse(client.add("test1", 5, "ignoredvalue", t).get());
    // Should return the original value
    assertEquals("test1value", client.get("test1", t));
  }

  @Test
  public void testAddNotSerializable() throws Exception {
    try {
      client.add("t1", 5, new Object());
      fail("expected illegal argument exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Non-serializable object, cause=java.lang.Object", e.getMessage());
    }
  }

  @Test
  public void testSetNotSerializable() throws Exception {
    try {
      client.set("t1", 5, new Object());
      fail("expected illegal argument exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Non-serializable object, cause=java.lang.Object", e.getMessage());
    }
  }

  @Test
  public void testReplaceNotSerializable() throws Exception {
    try {
      client.replace("t1", 5, new Object());
      fail("expected illegal argument exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Non-serializable object, cause=java.lang.Object", e.getMessage());
    }
  }

  @Test
  public void testUpdate() throws Exception {
    assertNull(client.get("test1"));
    assertFalse(client.replace("test1", 5, "test1value").get());
    assertNull(client.get("test1"));
  }

  @Test
  public void testUpdateWithTranscoder() throws Exception {
    Transcoder<String> t = new TestTranscoder();
    assertNull(client.get("test1", t));
    assertFalse(client.replace("test1", 5, "test1value", t).get());
    assertNull(client.get("test1", t));
  }

  // Just to make sure the sequence is being handled correctly
  @Test
  public void testMixedSetsAndUpdates() throws Exception {
    int keySize = 100;
    List<String> keys = new ArrayList<>(keySize);
    List<Future<Boolean>> setFutures = new ArrayList<>(keySize);
    List<Future<Boolean>> addFutures = new ArrayList<>(keySize);

    for (int i = 0; i < keySize; i++) {
      String key = "k" + i;
      setFutures.add(client.set(key, 60, key));
      addFutures.add(client.add(key, 60, "a" + i));
      keys.add(key);
    }
    for (int i = 0; i < keySize; i++) {
      assertTrue(setFutures.get(i).get());
      assertFalse(addFutures.get(i).get());
    }

    Map<String, Object> m = client.getBulk(keys);
    assertEquals(keySize, m.size());
    for (Map.Entry<String, Object> me : m.entrySet()) {
      assertEquals(me.getKey(), me.getValue());
    }
  }

  @Test
  public void testGetBulk() throws Exception {
    Collection<String> keys = Arrays.asList("test1", "test2", "test3");
    assertEquals(0, client.getBulk(keys).size());
    assertTrue(client.set("test1", 5, "val1").get());
    assertTrue(client.set("test2", 5, "val2").get());
    Map<String, Object> vals = client.getBulk(keys);
    assertEquals(2, vals.size());
    assertEquals("val1", vals.get("test1"));
    assertEquals("val2", vals.get("test2"));
  }

  @Test
  public void testAsyncGetBulkWithTranscoderIterator() throws Exception {
    ArrayList<String> keys = new ArrayList<>();
    keys.add("test1");
    keys.add("test2");
    keys.add("test3");

    ArrayList<Transcoder<String>> tcs = new ArrayList<>(keys.size());
    for (String key : keys) {
      tcs.add(new TestWithKeyTranscoder(key));
    }

    // Any transcoders listed after list of keys should be
    // ignored.
    for (String key : keys) {
      tcs.add(new TestWithKeyTranscoder(key));
    }

    assertEquals(0, client.asyncGetBulk(keys, tcs.listIterator()).get().size());

    assertTrue(client.set(keys.get(0), 5, "val1", tcs.get(0)).get());
    assertTrue(client.set(keys.get(1), 5, "val2", tcs.get(1)).get());
    Future<Map<String, String>> vals = client.asyncGetBulk(keys, tcs.listIterator());
    assertEquals(2, vals.get().size());
    assertEquals("val1", vals.get().get(keys.get(0)));
    assertEquals("val2", vals.get().get(keys.get(1)));

    // Set with one transcoder with the proper key and get
    // with another transcoder with the wrong key.
    keys.add(0, "test4");
    Transcoder<String> encodeTranscoder = new TestWithKeyTranscoder(keys.get(0));
    assertTrue(client.set(keys.get(0), 5, "val4", encodeTranscoder).get());

    Transcoder<String> decodeTranscoder = new TestWithKeyTranscoder("not " + keys.get(0));
    tcs.add(0, decodeTranscoder);
    try {
      client.asyncGetBulk(keys, tcs.listIterator()).get();
      fail("Expected ComparisonFailure caused by key mismatch");
    } catch (AssertionFailedError e) {
      // pass
    }
  }

  @Test
  public void testGetsBulk() throws Exception {
    Collection<String> keys = Arrays.asList("test1", "test2", "test3");
    assertEquals(0, client.getsBulk(keys).size());
    assertTrue(client.set("test1", 5, "val1").get());
    assertTrue(client.set("test2", 5, "val2").get());
    Map<String, CASValue<Object>> vals = client.getsBulk(keys);
    assertEquals(2, vals.size());
    assertEquals("val1", vals.get("test1").getValue());
    assertEquals("val2", vals.get("test2").getValue());
    assertEquals(client.gets("test1").getCas(), vals.get("test1").getCas());
    assertEquals(client.gets("test2").getCas(), vals.get("test2").getCas());
  }

  @Test
  public void testAsyncGetsBulkWithTranscoderIterator() throws Exception {
    ArrayList<String> keys = new ArrayList<>();
    keys.add("test1");
    keys.add("test2");
    keys.add("test3");

    ArrayList<Transcoder<String>> tcs = new ArrayList<>(keys.size());
    for (String key : keys) {
      tcs.add(new TestWithKeyTranscoder(key));
    }

    // Any transcoders listed after list of keys should be
    // ignored.
    for (String key : keys) {
      tcs.add(new TestWithKeyTranscoder(key));
    }

    assertEquals(0, client.asyncGetBulk(keys, tcs.listIterator()).get().size());

    assertTrue(client.set(keys.get(0), 5, "val1", tcs.get(0)).get());
    assertTrue(client.set(keys.get(1), 5, "val2", tcs.get(1)).get());
    BulkFuture<Map<String, CASValue<String>>> vals = client.asyncGetsBulk(keys, tcs.listIterator());
    assertEquals(2, vals.get().size());
    CASValue<String> val1 = vals.get().get(keys.get(0));
    CASValue<String> val2 = vals.get().get(keys.get(1));
    assertEquals("val1", val1.getValue());
    assertEquals("val2", val2.getValue());
    assertEquals(client.gets(keys.get(0), tcs.get(0)).getCas(), val1.getCas());
    assertEquals(client.gets(keys.get(1), tcs.get(1)).getCas(), val2.getCas());

    // Set with one transcoder with the proper key and get
    // with another transcoder with the wrong key.
    keys.add(0, "test4");
    Transcoder<String> encodeTranscoder = new TestWithKeyTranscoder(keys.get(0));
    assertTrue(client.set(keys.get(0), 5, "val4", encodeTranscoder).get());

    Transcoder<String> decodeTranscoder = new TestWithKeyTranscoder("not " + keys.get(0));
    tcs.add(0, decodeTranscoder);
    try {
      client.asyncGetsBulk(keys, tcs.listIterator()).get();
      fail("Expected ComparisonFailure caused by key mismatch");
    } catch (AssertionFailedError e) {
      // pass
    }
  }

  @Test
  public void testAvailableServers() {
    if (USE_ZK) {
      return; // We don't know the server address priori
    }
    client.getVersions();
    assertEquals(new ArrayList<>(
                    Collections.singleton(getExpectedVersionSource())),
            stringify(client.getAvailableServers()));
  }

  @Test
  public void testUnavailableServers() {
    client.getVersions();
    assertEquals(Collections.emptyList(), client.getUnavailableServers());
  }

  protected abstract String getExpectedVersionSource();

  @Test
  public void testGetVersions() throws Exception {
    if (USE_ZK) {
      return; // We don't know the server address priori
    }
    Map<SocketAddress, String> vs = client.getVersions();
    assertEquals(1, vs.size());
    Map.Entry<SocketAddress, String> me = vs.entrySet().iterator().next();
    assertEquals(getExpectedVersionSource(), me.getKey().toString());
    assertNotNull(me.getValue());
  }

  @Test
  public void testNonexistentMutate() throws Exception {
    assertEquals(-1, client.incr("nonexistent", 1));
    assertEquals(-1, client.decr("nonexistent", 1));
  }

  @Test
  public void testMutateWithDefault() throws Exception {
    assertEquals(3, client.incr("mtest", 1, 3));
    assertEquals(4, client.incr("mtest", 1, 3));
    assertEquals(3, client.decr("mtest", 1, 9));
    assertEquals(9, client.decr("mtest2", 1, 9));
  }

  @Test
  public void testMutateWithDefaultAndExp() throws Exception {
    assertEquals(3, client.incr("mtest", 1, 3, 1));
    assertEquals(4, client.incr("mtest", 1, 3, 1));
    assertEquals(3, client.decr("mtest", 1, 9, 1));
    assertEquals(9, client.decr("mtest2", 1, 9, 1));
    Thread.sleep(2000);
    assertNull(client.get("mtest"));
  }

  @Test
  public void testAsyncIncrement() throws Exception {
    String k = "async-incr";
    assertTrue(client.set(k, 0, "5").get());
    Future<Long> f = client.asyncIncr(k, 1);
    assertEquals(6, (long) f.get());
  }

  @Test
  public void testAsyncIncrementNonExistent() throws Exception {
    String k = "async-incr-non-existent";
    Future<Long> f = client.asyncIncr(k, 1);
    assertEquals(-1, (long) f.get());
  }

  @Test
  public void testAsyncDecrement() throws Exception {
    String k = "async-decr";
    assertTrue(client.set(k, 0, "5").get());
    Future<Long> f = client.asyncDecr(k, 1);
    assertEquals(4, (long) f.get());
  }

  @Test
  public void testAsyncDecrementNonExistent() throws Exception {
    String k = "async-decr-non-existent";
    Future<Long> f = client.asyncDecr(k, 1);
    assertEquals(-1, (long) f.get());
  }

  @Test
  public void testConcurrentMutation() throws Throwable {
    int num = SyncThread.getDistinctResultCount(10, new Callable<Long>() {
      public Long call() throws Exception {
        return client.incr("mtest", 1, 11);
      }
    });
    assertEquals(10, num);
  }

  @Test
  public void testImmediateDelete() throws Exception {
    assertNull(client.get("test1"));
    assertTrue(client.set("test1", 5, "test1value").get());
    assertEquals("test1value", client.get("test1"));
    assertTrue(client.delete("test1").get());
    assertNull(client.get("test1"));
  }

  @Test
  public void testFlush() throws Exception {
    assertNull(client.get("test1"));
    assertTrue(client.set("test1", 5, "test1value").get());
    assertTrue(client.set("test2", 5, "test2value").get());
    assertEquals("test1value", client.get("test1"));
    assertEquals("test2value", client.get("test2"));
    assertTrue(client.flush().get());
    assertNull(client.get("test1"));
    assertNull(client.get("test2"));
  }

  @Test
  public void testGracefulShutdown() throws Exception {
    for (int i = 0; i < 1000; i++) {
      client.set("t" + i, 10, i);
    }
    assertTrue(client.shutdown(5, TimeUnit.SECONDS),
            "Couldn't shut down within five seconds");

    // Get a new client
    initClient();
    Collection<String> keys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      keys.add("t" + i);
    }
    Map<String, Object> m = client.getBulk(keys);
    assertEquals(1000, m.size());
    for (int i = 0; i < 1000; i++) {
      assertEquals(i, m.get("t" + i));
    }
  }

  @Test
  public void testSyncGetTimeouts() throws Exception {
    final String key = "timeoutTestKey";
    final String value = "timeoutTestValue";
    // Shutting down the default client to get one with a short timeout.
    assertTrue(client.shutdown(5, TimeUnit.SECONDS),
            "Couldn't shut down within five seconds");

    initClient(new DefaultConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 1;
      }
    });

    assertTrue(client.set(key, 0, value).get(1, TimeUnit.SECONDS));
    try {
      for (int i = 0; i < 1000000; i++) {
        client.get(key);
      }
      throw new Exception("Didn't get a timeout.");
    } catch (OperationTimeoutException e) {
      System.out.println("Got a timeout.");
    }
    if (value.equals(client.asyncGet(key).get(1, TimeUnit.SECONDS))) {
      System.out.println("Got the right value.");
    } else {
      throw new Exception("Didn't get the expected value.");
    }
  }

  @Test
  public void testGracefulShutdownTooSlow() throws Exception {
    final int size = 1000 * 1000;
    final byte[] data = new byte[size];
    new Random().nextBytes(data);

    // Used a transcoder without compression to quickly send operations to the server.
    final Transcoder<byte[]> tc = new Transcoder<byte[]>() {
      @Override
      public boolean asyncDecode(CachedData d) {
        return false;
      }

      @Override
      public CachedData encode(byte[] o) {
        return new CachedData(0, data, size);
      }

      @Override
      public byte[] decode(CachedData d) {
        return d.getData();
      }

      @Override
      public int getMaxSize() {
        return CachedData.MAX_SIZE;
      }
    };

    for (int i = 0; i < 100; i++) {
      client.set("t" + i, 10, data, tc);
    }
    assertFalse(client.shutdown(700, TimeUnit.MICROSECONDS),
            "Weird, shut down too fast");

    try {
      Map<SocketAddress, String> m = client.getVersions();
      fail("Expected failure, got " + m);
    } catch (IllegalStateException e) {
      assertEquals("Shutting down", e.getMessage());
    }

    // Get a new client
    initClient();
  }

  @Test
  public void testStupidlyLargeSetAndSizeOverride() throws Exception {
    Random r = new Random();
    SerializingTranscoder st = new SerializingTranscoder(Integer.MAX_VALUE);

    st.setCompressionThreshold(Integer.MAX_VALUE);

    byte data[] = new byte[10 * 1024 * 1024];
    r.nextBytes(data);

    try {
      client.set("bigassthing", 60, data, st).get();
      fail("Didn't fail setting bigass thing.");
    } catch (ExecutionException e) {
      e.printStackTrace();
      OperationException oe = (OperationException) e.getCause();
      // ensure compatibility about changing E2BIG
      assertTrue(OperationErrorType.CLIENT == oe.getType() ||
            OperationErrorType.SERVER == oe.getType());
    }

    // But I should still be able to do something.
    assertTrue(client.set("k", 5, "Blah").get());
    assertEquals("Blah", client.get("k"));
  }

  @Test
  public void testStupidlyLargeSet() throws Exception {
    Random r = new Random();
    SerializingTranscoder st = new SerializingTranscoder();
    st.setCompressionThreshold(Integer.MAX_VALUE);

    byte data[] = new byte[10 * 1024 * 1024];
    r.nextBytes(data);

    try {
      client.set("bigassthing", 60, data, st).get();
      fail("Didn't fail setting bigass thing.");
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot cache data larger than "
                      + CachedData.MAX_SIZE + " bytes "
                      + "(you tried to cache a " + data.length + " byte object)",
              e.getMessage());
    }

    // But I should still be able to do something.
    assertTrue(client.set("k", 5, "Blah").get());
    assertEquals("Blah", client.get("k"));
  }

  @Test
  public void testQueueAfterShutdown() throws Exception {
    client.shutdown();
    try {
      Object o = client.get("k");
      fail("Expected IllegalStateException, got " + o);
    } catch (IllegalStateException e) {
      // OK
    } finally {
      initClient(); // init for tearDown
    }
  }

  @Test
  public void testMultiReqAfterShutdown() throws Exception {
    client.shutdown();
    try {
      Map<String, ?> m = client.getBulk(Arrays.asList("k1", "k2", "k3"));
      fail("Expected IllegalStateException, got " + m);
    } catch (IllegalStateException e) {
      // OK
    } finally {
      initClient(); // init for tearDown
    }
  }

  @Test
  public void testBroadcastAfterShutdown() throws Exception {
    client.shutdown();
    try {
      Future<?> f = client.flush();
      fail("Expected IllegalStateException, got " + f.get());
    } catch (IllegalStateException e) {
      // OK
    } finally {
      initClient(); // init for tearDown
    }
  }

  @Test
  public void testABunchOfCancelledOperations() throws Exception {
    final String k = "bunchOCancel";
    Collection<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      futures.add(client.set(k, 5, "xval"));
      futures.add(client.asyncGet(k));
    }
    Future<Boolean> sf = client.set(k, 5, "myxval");
    Future<Object> gf = client.asyncGet(k);
    for (Future<?> f : futures) {
      f.cancel(true);
    }
    assertTrue(sf.get());
    assertEquals("myxval", gf.get());
  }

  @Test
  public void testUTF8Key() throws Exception {
    final String key = "junit.Здравствуйте." + System.currentTimeMillis();
    final String value = "Skiing rocks if you can find the time to go!";

    assertTrue(client.set(key, 6000, value).get());
    Object output = client.get(key);
    assertNotNull(output, "output is null");
    assertEquals(value, output, "output is not equal");
  }

  @Test
  public void testUTF8KeyDelete() throws Exception {
    final String key = "junit.Здравствуйте." + System.currentTimeMillis();
    final String value = "Skiing rocks if you can find the time to go!";

    assertTrue(client.set(key, 6000, value).get());
    assertTrue(client.delete(key).get());
    assertNull(client.get(key));
  }

  @Test
  public void testUTF8MultiGet() throws Exception {
    final String value = "Skiing rocks if you can find the time to go!";
    Collection<String> keys = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      final String key = "junit.Здравствуйте."
              + System.currentTimeMillis() + "." + i;
      assertTrue(client.set(key, 6000, value).get());
      keys.add(key);
    }

    Map<String, Object> vals = client.getBulk(keys);
    assertEquals(keys.size(), vals.size());
    for (Object o : vals.values()) {
      assertEquals(value, o);
    }
    assertTrue(keys.containsAll(vals.keySet()));
  }

  @Test
  public void testUTF8Value() throws Exception {
    final String key = "junit.plaintext." + System.currentTimeMillis();
    final String value = "Здравствуйте Здравствуйте Здравствуйте "
            + "Skiing rocks if you can find the time to go!";

    assertTrue(client.set(key, 6000, value).get());
    Object output = client.get(key);
    assertNotNull(output, "output is null");
    assertEquals(value, output, "output is not equal");
  }

  @Test
  public void testAppend() throws Exception {
    final String key = "append.key";
    assertTrue(client.set(key, 5, "test").get());
    assertTrue(client.append(0, key, "es").get());
    assertEquals("testes", client.get(key));
  }

  @Test
  public void testPrepend() throws Exception {
    final String key = "prepend.key";
    assertTrue(client.set(key, 5, "test").get());
    assertTrue(client.prepend(0, key, "es").get());
    assertEquals("estest", client.get(key));
  }

  @Test
  public void testAppendNoSuchKey() throws Exception {
    final String key = "append.missing";
    assertFalse(client.append(0, key, "es").get());
    assertNull(client.get(key));
  }

  @Test
  public void testPrependNoSuchKey() throws Exception {
    final String key = "prepend.missing";
    assertFalse(client.prepend(0, key, "es").get());
    assertNull(client.get(key));
  }

  private static class TestTranscoder implements Transcoder<String> {
    private static final int flags = 238885206;

    public String decode(CachedData d) {
      assert d.getFlags() == flags
              : "expected " + flags + " got " + d.getFlags();
      return new String(d.getData());
    }

    public CachedData encode(String o) {
      return new CachedData(flags, o.getBytes(), getMaxSize());
    }

    public int getMaxSize() {
      return CachedData.MAX_SIZE;
    }

    public boolean asyncDecode(CachedData d) {
      return false;
    }
  }

  private static class TestWithKeyTranscoder implements Transcoder<String> {
    private static final int flags = 238885207;

    private final String key;

    TestWithKeyTranscoder(String k) {
      key = k;
    }

    public String decode(CachedData d) {
      assert d.getFlags() == flags
              : "expected " + flags + " got " + d.getFlags();

      ByteBuffer bb = ByteBuffer.wrap(d.getData());

      int keyLength = bb.getInt();
      byte[] keyBytes = new byte[keyLength];
      bb.get(keyBytes);
      String k = new String(keyBytes);

      assertEquals(key, k);

      int valueLength = bb.getInt();
      byte[] valueBytes = new byte[valueLength];
      bb.get(valueBytes);

      return new String(valueBytes);
    }

    public CachedData encode(String o) {
      byte[] keyBytes = key.getBytes();
      byte[] valueBytes = o.getBytes();
      int length = 4 + keyBytes.length + 4 + valueBytes.length;
      byte[] bytes = new byte[length];

      ByteBuffer bb = ByteBuffer.wrap(bytes);
      bb.putInt(keyBytes.length).put(keyBytes);
      bb.putInt(valueBytes.length).put(valueBytes);

      return new CachedData(flags, bytes, getMaxSize());
    }

    public int getMaxSize() {
      return CachedData.MAX_SIZE;
    }

    public boolean asyncDecode(CachedData d) {
      return false;
    }
  }
}
