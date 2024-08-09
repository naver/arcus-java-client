package net.spy.memcached;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import net.spy.memcached.transcoders.IntegerTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.jmock.Expectations;
import org.jmock.Mockery;

/**
 * Test the CacheMap.
 */
public class CacheMapTest extends TestCase {

  private final static int EXP = 8175;
  private Mockery context;
  private MemcachedClientIF client;
  private Transcoder<Object> transcoder;
  private CacheMap cacheMap;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
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

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
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

  public void testBaseConstructor() throws Exception {
    BaseCacheMap<Integer> bcm = new BaseCacheMap<>(client,
            EXP, "base", new IntegerTranscoder());
    Field f = BaseCacheMap.class.getDeclaredField("exp");
    f.setAccessible(true);
    assertEquals(EXP, f.getInt(bcm));
  }

  public void testClear() {
    try {
      cacheMap.clear();
      fail("Expected unsupported operation exception");
    } catch (UnsupportedOperationException e) {
      // pass
    }
  }

  public void testGetPositive() {
    expectGetAndReturn("blaha", "something");
    assertEquals("something", cacheMap.get("a"));
  }

  public void testGetNegative() {
    expectGetAndReturn("blaha", null);
    assertNull(cacheMap.get("a"));
  }

  public void testGetNotString() {
    assertNull(cacheMap.get(new Object()));
  }

  public void testContainsPositive() {
    expectGetAndReturn("blaha", new Object());
    assertTrue(cacheMap.containsKey("a"));
  }

  public void testContainsNegative() {
    expectGetAndReturn("blaha", null);
    assertFalse(cacheMap.containsKey("a"));
  }

  public void testContainsValue() {
    assertFalse(cacheMap.containsValue("anything"));
  }

  public void testEntrySet() {
    assertEquals(0, cacheMap.entrySet().size());
  }

  public void testKeySet() {
    assertEquals(0, cacheMap.keySet().size());
  }

  public void testtIsEmpty() {
    assertFalse(cacheMap.isEmpty());
  }

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

  public void testSize() {
    assertEquals(0, cacheMap.size());
  }

  public void testValues() {
    assertEquals(0, cacheMap.values().size());
  }

  public void testRemove() {
    expectGetAndReturn("blaha", "olda");
    context.checking(
            new Expectations() {{
              oneOf(client).delete("blaha");
            }}
    );

    assertEquals("olda", cacheMap.remove("a"));
  }

  public void testRemoveNotString() {
    assertNull(cacheMap.remove(new Object()));
  }

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
