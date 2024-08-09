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

import org.jmock.Expectations;
import org.jmock.Mockery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test the CacheMap.
 */
public class CacheMapTest {

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

    context.checking(
            new Expectations() {{
              oneOf(client).getTranscoder();
              will(returnValue(transcoder));
            }}
    );

    cacheMap = new CacheMap(client, EXP, "blah");
  }

  @AfterEach
  protected void tearDown() throws Exception {
    context.assertIsSatisfied();
  }

  private void expectGetAndReturn(String k, Object value) {
    context.checking(
            new Expectations() {{
              oneOf(client).get(k, transcoder);
              will(returnValue(value));
            }}
    );
  }

  @Test
  public void testNoExpConstructor() throws Exception {
    context.checking(
            new Expectations() {{
              oneOf(client).getTranscoder();
              will(returnValue(transcoder));
            }}
    );

    CacheMap cm = new CacheMap(client, "blah");
    Field f = BaseCacheMap.class.getDeclaredField("exp");
    f.setAccessible(true);
    assertEquals(0, f.getInt(cm));
  }

  @Test
  public void testBaseConstructor() throws Exception {
    BaseCacheMap<Integer> bcm = new BaseCacheMap<>(client,
            EXP, "base", new IntegerTranscoder());
    Field f = BaseCacheMap.class.getDeclaredField("exp");
    f.setAccessible(true);
    assertEquals(EXP, f.getInt(bcm));
  }

  @Test
  public void testClear() {
    try {
      cacheMap.clear();
      fail("Expected unsupported operation exception");
    } catch (UnsupportedOperationException e) {
      // pass
    }
  }

  @Test
  public void testGetPositive() {
    expectGetAndReturn("blaha", "something");
    assertEquals("something", cacheMap.get("a"));
  }

  @Test
  public void testGetNegative() {
    expectGetAndReturn("blaha", null);
    assertNull(cacheMap.get("a"));
  }

  @Test
  public void testGetNotString() {
    assertNull(cacheMap.get(new Object()));
  }

  @Test
  public void testContainsPositive() {
    expectGetAndReturn("blaha", new Object());
    assertTrue(cacheMap.containsKey("a"));
  }

  @Test
  public void testContainsNegative() {
    expectGetAndReturn("blaha", null);
    assertFalse(cacheMap.containsKey("a"));
  }

  @Test
  public void testContainsValue() {
    assertFalse(cacheMap.containsValue("anything"));
  }

  @Test
  public void testEntrySet() {
    assertEquals(0, cacheMap.entrySet().size());
  }

  @Test
  public void testKeySet() {
    assertEquals(0, cacheMap.keySet().size());
  }

  @Test
  public void testtIsEmpty() {
    assertFalse(cacheMap.isEmpty());
  }

  @Test
  public void testPutAll() {
    context.checking(
            new Expectations() {{
              oneOf(client).set("blaha", EXP, "vala");
              oneOf(client).set("blahb", EXP, "valb");
            }}
    );

    Map<String, Object> m = new HashMap<>();
    m.put("a", "vala");
    m.put("b", "valb");

    cacheMap.putAll(m);
  }

  @Test
  public void testSize() {
    assertEquals(0, cacheMap.size());
  }

  @Test
  public void testValues() {
    assertEquals(0, cacheMap.values().size());
  }

  @Test
  public void testRemove() {
    expectGetAndReturn("blaha", "olda");
    context.checking(
            new Expectations() {{
              oneOf(client).delete("blaha");
            }}
    );

    assertEquals("olda", cacheMap.remove("a"));
  }

  @Test
  public void testRemoveNotString() {
    assertNull(cacheMap.remove(new Object()));
  }

  @Test
  public void testPut() {
    expectGetAndReturn("blaha", "olda");
    context.checking(
            new Expectations() {{
              oneOf(client).set("blaha", EXP, "newa");
            }}
    );

    assertEquals("olda", cacheMap.put("a", "newa"));
  }

}
