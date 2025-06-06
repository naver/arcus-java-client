package net.spy.memcached;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import net.spy.memcached.transcoders.IntegerTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jmock.Mockery;

import static net.spy.memcached.ExpectationsUtil.buildExpectations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import static org.jmock.AbstractExpectations.returnValue;

/**
 * Test the CacheMap.
 */
class CacheMapTest {

  private final static int EXP = 8175;
  private Mockery context;
  private MemcachedClientIF client;
  private Transcoder<Object> transcoder;
  private CacheMap cacheMap;

  @BeforeEach
  protected void setUp() throws Exception {
    transcoder = new SerializingTranscoder();
    context = new Mockery();
    client = context.mock(MemcachedClientIF.class);

    context.checking(buildExpectations(e -> {
      e.oneOf(client).getTranscoder();
      e.will(returnValue(transcoder));
    }));

    cacheMap = new CacheMap(client, EXP, "blah");
  }

  @AfterEach
  protected void tearDown() throws Exception {
    context.assertIsSatisfied();
  }

  private void expectGetAndReturn(String k, Object value) {
    context.checking(buildExpectations(e -> {
      e.oneOf(client).get(k, transcoder);
      e.will(returnValue(value));
    }));
  }

  @Test
  void testNoExpConstructor() throws Exception {
    context.checking(buildExpectations(e -> {
      e.oneOf(client).getTranscoder();
      e.will(returnValue(transcoder));
    }));

    CacheMap cm = new CacheMap(client, "blah");
    Field f = BaseCacheMap.class.getDeclaredField("exp");
    f.setAccessible(true);
    assertEquals(0, f.getInt(cm));
  }

  @Test
  void testBaseConstructor() throws Exception {
    BaseCacheMap<Integer> bcm = new BaseCacheMap<>(client,
            EXP, "base", new IntegerTranscoder());
    Field f = BaseCacheMap.class.getDeclaredField("exp");
    f.setAccessible(true);
    assertEquals(EXP, f.getInt(bcm));
  }

  @Test
  void testClear() {
    try {
      cacheMap.clear();
      fail("Expected unsupported operation exception");
    } catch (UnsupportedOperationException e) {
      // pass
    }
  }

  @Test
  void testGetPositive() {
    expectGetAndReturn("blaha", "something");
    assertEquals("something", cacheMap.get("a"));
  }

  @Test
  void testGetNegative() {
    expectGetAndReturn("blaha", null);
    assertNull(cacheMap.get("a"));
  }

  @Test
  void testGetNotString() {
    assertNull(cacheMap.get(new Object()));
  }

  @Test
  void testContainsPositive() {
    expectGetAndReturn("blaha", new Object());
    assertTrue(cacheMap.containsKey("a"));
  }

  @Test
  void testContainsNegative() {
    expectGetAndReturn("blaha", null);
    assertFalse(cacheMap.containsKey("a"));
  }

  @Test
  void testContainsValue() {
    assertFalse(cacheMap.containsValue("anything"));
  }

  @Test
  void testEntrySet() {
    assertEquals(0, cacheMap.entrySet().size());
  }

  @Test
  void testKeySet() {
    assertEquals(0, cacheMap.keySet().size());
  }

  @Test
  void testtIsEmpty() {
    assertFalse(cacheMap.isEmpty());
  }

  @Test
  void testPutAll() {
    context.checking(buildExpectations(e -> {
      e.oneOf(client).set("blaha", EXP, "vala");
      e.oneOf(client).set("blahb", EXP, "valb");
    }));

    Map<String, Object> m = new HashMap<>();
    m.put("a", "vala");
    m.put("b", "valb");

    cacheMap.putAll(m);
  }

  @Test
  void testSize() {
    assertEquals(0, cacheMap.size());
  }

  @Test
  void testValues() {
    assertEquals(0, cacheMap.values().size());
  }

  @Test
  void testRemove() {
    expectGetAndReturn("blaha", "olda");
    context.checking(buildExpectations(e -> {
      e.oneOf(client).delete("blaha");
    }));

    assertEquals("olda", cacheMap.remove("a"));
  }

  @Test
  void testRemoveNotString() {
    assertNull(cacheMap.remove(new Object()));
  }

  @Test
  void testPut() {
    expectGetAndReturn("blaha", "olda");
    context.checking(buildExpectations(e -> {
      e.oneOf(client).set("blaha", EXP, "newa");
    }));

    assertEquals("olda", cacheMap.put("a", "newa"));
  }

}
