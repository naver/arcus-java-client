package net.spy.memcached.transcoders;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import net.spy.memcached.CachedData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test the generic JSON serializing transcoder.
 */
class GenericJsonSerializingTranscoderTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private GenericJsonSerializingTranscoder tc;

  @BeforeEach
  void setUp() {
    tc = new GenericJsonSerializingTranscoder(mapper, "", CachedData.MAX_SIZE);
  }

  @Test
  void testValidCharacterSet() {
    tc.setCharset("KOI8");
    assertEquals("KOI8", tc.getCharset());
  }

  @Test
  void testInvalidCharacterSet() {
    try {
      tc.setCharset("Dustin's Kick Ass Character Set");
      fail("Expected a RuntimeException");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof UnsupportedEncodingException);
    }
  }

  @Test
  void testSetCompressionThreshold() {
    tc.setCompressionThreshold(1024);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 2000; i++) {
      sb.append('x');
    }
    String largeData = sb.toString();
    CachedData cd = tc.encode(largeData);
    assertEquals(largeData, tc.decode(cd));
  }

  @Test
  void testConstructorWithMaxSize() {
    assertEquals(CachedData.MAX_SIZE, tc.getMaxSize());
  }

  @Test
  void testStrings() {
    String s1 = "This is a simple test string.";
    CachedData cd = tc.encode(s1);
    assertEquals(0, cd.getFlags());
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  void testUTF8String() {
    String s1 = "\u2013\u00f3\u2013\u00a5\u2014\u00c4\u2013\u221e\u2013"
        + "\u2264\u2014\u00c5\u2014\u00c7\u2013\u2264\u2014\u00c9\u2013"
        + "\u03c0, \u2013\u00ba\u2013\u220f\u2014\u00c4.";
    CachedData cd = tc.encode(s1);
    assertEquals(0, cd.getFlags());
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  void testEmptyString() {
    String emptyStr = "";
    CachedData cd = tc.encode(emptyStr);
    assertEquals(emptyStr, tc.decode(cd));
  }

  @Test
  void testLong() {
    assertEquals(923L, tc.decode(tc.encode(923L)));
    assertEquals(Long.MIN_VALUE, tc.decode(tc.encode(Long.MIN_VALUE)));
    assertEquals(1L, tc.decode(tc.encode(1L)));
    assertEquals(23852L, tc.decode(tc.encode(23852L)));
    assertEquals(0L, tc.decode(tc.encode(0L)));
    assertEquals(-1L, tc.decode(tc.encode(-1L)));
    assertEquals(-23835L, tc.decode(tc.encode(-23835L)));
    assertEquals(Long.MAX_VALUE, tc.decode(tc.encode(Long.MAX_VALUE)));
  }

  @Test
  void testInt() {
    assertEquals(923, tc.decode(tc.encode(923)));
    assertEquals(Integer.MIN_VALUE, tc.decode(tc.encode(Integer.MIN_VALUE)));
    assertEquals(83526, tc.decode(tc.encode(83526)));
    assertEquals(1, tc.decode(tc.encode(1)));
    assertEquals(0, tc.decode(tc.encode(0)));
    assertEquals(-1, tc.decode(tc.encode(-1)));
    assertEquals(-238526, tc.decode(tc.encode(-238526)));
    assertEquals(Integer.MAX_VALUE, tc.decode(tc.encode(Integer.MAX_VALUE)));
  }

  @Test
  void testShort() {
    assertEquals((short) 923, tc.decode(tc.encode((short) 923)));
  }

  @Test
  void testChar() {
    assertEquals('c', tc.decode(tc.encode('c')));
  }

  @Test
  void testBoolean() {
    assertTrue((Boolean) tc.decode(tc.encode(true)));
    assertFalse((Boolean) tc.decode(tc.encode(false)));
  }

  @Test
  void testByte() {
    assertEquals((byte) -127, tc.decode(tc.encode((byte) -127)));
  }

  @Test
  void testFloat() {
    assertEquals(0f, tc.decode(tc.encode(0f)));
    assertEquals(Float.MIN_VALUE, tc.decode(tc.encode(Float.MIN_VALUE)));
    assertEquals(Float.MAX_VALUE, tc.decode(tc.encode(Float.MAX_VALUE)));
    assertEquals(3.14f, tc.decode(tc.encode(3.14f)));
    assertEquals(-3.14f, tc.decode(tc.encode(-3.14f)));
    assertEquals(Float.NaN, tc.decode(tc.encode(Float.NaN)));
    assertEquals(Float.POSITIVE_INFINITY, tc.decode(tc.encode(Float.POSITIVE_INFINITY)));
    assertEquals(Float.NEGATIVE_INFINITY, tc.decode(tc.encode(Float.NEGATIVE_INFINITY)));
  }

  @Test
  void testDouble() {
    assertEquals(0d, tc.decode(tc.encode(0d)));
    assertEquals(Double.MIN_VALUE, tc.decode(tc.encode(Double.MIN_VALUE)));
    assertEquals(Double.MAX_VALUE, tc.decode(tc.encode(Double.MAX_VALUE)));
    assertEquals(3.14d, tc.decode(tc.encode(3.14d)));
    assertEquals(-3.14d, tc.decode(tc.encode(-3.14d)));
    assertEquals(Double.NaN, tc.decode(tc.encode(Double.NaN)));
    assertEquals(Double.POSITIVE_INFINITY, tc.decode(tc.encode(Double.POSITIVE_INFINITY)));
    assertEquals(Double.NEGATIVE_INFINITY, tc.decode(tc.encode(Double.NEGATIVE_INFINITY)));
  }

  @Test
  void testByteArray() {
    byte[] a = {'a', 'b', 'c'};
    CachedData cd = tc.encode(a);
    assertEquals(TranscoderUtils.SPECIAL_BYTEARRAY, cd.getFlags());
    assertArrayEquals(a, (byte[]) tc.decode(cd));
  }

  @Test
  void testStringArray() {
    String[] a = {"a", "b", "c"};
    CachedData cd = tc.encode(a);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());
    assertArrayEquals(a, (String[]) tc.decode(cd));
  }

  @Test
  void testStringBuilder() {
    StringBuilder sb = new StringBuilder("test");
    StringBuilder sb2 = (StringBuilder) tc.decode(tc.encode(sb));
    assertEquals(sb.toString(), sb2.toString());
  }

  @Test
  void testStringBuffer() {
    StringBuffer sb = new StringBuffer("test");
    StringBuffer sb2 = (StringBuffer) tc.decode(tc.encode(sb));
    assertEquals(sb.toString(), sb2.toString());
  }

  @Test
  void testDate() {
    Date d = new Date();
    CachedData cd = tc.encode(d);
    assertEquals(d, tc.decode(cd));
  }

  @Test
  void testSomethingBigger() {
    Collection<Date> dates = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      dates.add(new Date());
    }
    CachedData d = tc.encode(dates);
    assertEquals(dates, tc.decode(d));
  }

  @Test
  void testJsonObject() {
    TestPojo pojo = new TestPojo("test", 123);

    CachedData cd = tc.encode(pojo);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());
    assertTrue(new String(cd.getData()).contains("@class"));

    TestPojo decoded = (TestPojo) tc.decode(cd);
    assertEquals(pojo.getName(), decoded.getName());
    assertEquals(pojo.getValue(), decoded.getValue());
  }

  @Test
  void testObjectMapperWithoutDefaultTyping() {
    ObjectMapper objectMapper = new ObjectMapper();
    GenericJsonSerializingTranscoder tcWithoutDefaultTyping
        = new GenericJsonSerializingTranscoder(objectMapper, null, CachedData.MAX_SIZE);
    TestPojo pojo = new TestPojo("test", 123);

    CachedData cd = tcWithoutDefaultTyping.encode(pojo);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());
    assertFalse(new String(cd.getData()).contains("@class"));

    assertThrows(ClassCastException.class, () -> {
      TestPojo decoded = (TestPojo) tcWithoutDefaultTyping.decode(cd);
    });
  }

  @Test
  void testNestedPojo() {
    NestedPojo nested = new NestedPojo("outer", new TestPojo("inner", 789));

    CachedData cd = tc.encode(nested);
    NestedPojo decoded = (NestedPojo) tc.decode(cd);
    assertEquals(nested.getOuterName(), decoded.getOuterName());
    assertEquals(nested.getInner().getName(), decoded.getInner().getName());
    assertEquals(nested.getInner().getValue(), decoded.getInner().getValue());
  }

  @Test
  void testJsonList() {
    List<String> list = Arrays.asList("item1", "item2", "item3");

    CachedData cd = tc.encode(list);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());

    @SuppressWarnings("unchecked")
    List<String> decoded = (List<String>) tc.decode(cd);
    assertEquals(list, decoded);
  }

  @Test
  void testJsonArray() {
    String[] array = {"item1", "item2", "item3"};

    CachedData cd = tc.encode(array);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());

    String[] decoded = (String[]) tc.decode(cd);
    assertEquals(Arrays.asList(array), Arrays.asList(decoded));
  }

  @Test
  void testJsonMap() {
    Map<String, Integer> map = new HashMap<>();
    map.put("key1", 1);
    map.put("key2", 2);

    CachedData cd = tc.encode(map);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());

    @SuppressWarnings("unchecked")
    Map<String, Integer> decoded = (Map<String, Integer>) tc.decode(cd);
    assertEquals(map, decoded);
  }

  @Test
  void testCompressedStringNotSmaller() {
    String s1 = "This is a test simple string that will not be compressed.";
    // Reduce the compression threshold so it'll attempt to compress it.
    tc.setCompressionThreshold(8);
    CachedData cd = tc.encode(s1);
    // This should *not* be compressed because it is too small
    assertEquals(0, cd.getFlags());
    assertArrayEquals(s1.getBytes(), cd.getData());
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  void testCompressedString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append('a');
    }
    String s1 = sb.toString();
    tc.setCompressionThreshold(8);
    CachedData cd = tc.encode(s1);
    assertEquals(TranscoderUtils.COMPRESSED, cd.getFlags());
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  void testCompressedObject() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append('x');
    }
    TestPojo largePojo = new TestPojo(sb.toString(), 123);
    tc.setCompressionThreshold(8);

    CachedData cd = tc.encode(largePojo);
    assertEquals(TranscoderUtils.SERIALIZED | TranscoderUtils.COMPRESSED, cd.getFlags());

    TestPojo decoded = (TestPojo) tc.decode(cd);
    assertEquals(largePojo.getName(), decoded.getName());
    assertEquals(largePojo.getValue(), decoded.getValue());
  }

  @Test
  void testEncodeNull() {
    assertThrows(NullPointerException.class, () -> tc.encode(null));
  }

  @Test
  void testDecodeEmpty() {
    assertEquals("", tc.decode(new CachedData(0, new byte[0], tc.getMaxSize())));
  }

  @Test
  void testUndecodeable() {
    CachedData cd = new CachedData(
        Integer.MAX_VALUE &
            ~(TranscoderUtils.COMPRESSED | TranscoderUtils.SERIALIZED),
        new TranscoderUtils(false).encodeInt(Integer.MAX_VALUE),
        tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Test
  void testUndecodeableSerialized() {
    CachedData cd = new CachedData(
        TranscoderUtils.SERIALIZED,
        "invalid json data".getBytes(),
        tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Test
  void testUndecodeableCompressed() {
    CachedData cd = new CachedData(
        TranscoderUtils.COMPRESSED,
        "invalid compressed data".getBytes(),
        tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Test
  void testEnum() {
    CachedData data = tc.encode(SampleEnum.ONE);
    assertEquals(TranscoderUtils.SERIALIZED, data.getFlags());
    Object decoded = tc.decode(data);
    assertEquals(SampleEnum.ONE, decoded);
  }

  @Test
  void testCustomTypePropertyName() {
    GenericJsonSerializingTranscoder custom =
        new GenericJsonSerializingTranscoder(new ObjectMapper(), "type", CachedData.MAX_SIZE);
    TestPojo p = new TestPojo("abc", 1);
    CachedData cd = custom.encode(p);
    String json = new String(cd.getData(), StandardCharsets.UTF_8);
    assertTrue(json.contains("\"type\""));
    assertFalse(json.contains("\"@class\""));
    TestPojo decoded = (TestPojo) custom.decode(cd);
    assertEquals(p.getName(), decoded.getName());
  }

  @Test
  void testPolymorphicList() {
    List<Object> list = new ArrayList<>();
    list.add("str");
    list.add(42);
    list.add(new TestPojo("x", 7));
    list.add(Arrays.asList("nested", 5));
    CachedData cd = tc.encode(list);
    @SuppressWarnings("unchecked")
    List<Object> decoded = (List<Object>) tc.decode(cd);
    assertEquals(list.size(), decoded.size());
    assertEquals("str", decoded.get(0));
    assertEquals(42, decoded.get(1));
    assertInstanceOf(TestPojo.class, decoded.get(2));
  }

  @Test
  void testMapWithMixedTypesAndNull() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("a", 1);
    map.put("b", null);
    map.put("c", new TestPojo("v", 3));
    map.put("d", Arrays.asList(null, "z"));
    CachedData cd = tc.encode(map);
    @SuppressWarnings("unchecked")
    Map<String, Object> decoded = (Map<String, Object>) tc.decode(cd);
    assertTrue(decoded.containsKey("b"));
    assertNull(decoded.get("b"));
    assertInstanceOf(TestPojo.class, decoded.get("c"));
  }

  @Test
  void testObjectReferencePreservesConcreteType() {
    Object obj = new TestPojo("keep", 9);
    CachedData cd = tc.encode(obj);
    Object decoded = tc.decode(cd);
    assertInstanceOf(TestPojo.class, decoded);
    assertEquals("keep", ((TestPojo) decoded).getName());
  }

  @Test
  void testBigDecimalAndBigInteger() {
    BigDecimal bd = new BigDecimal("12345.6789");
    BigInteger bi = new BigInteger("9876543210123456789");
    assertEquals(bd, tc.decode(tc.encode(bd)));
    assertEquals(bi, tc.decode(tc.encode(bi)));
  }

  @Test
  void testEmptyCollectionAndMap() {
    List<Object> emptyList = Collections.emptyList();
    Map<String, Object> emptyMap = Collections.emptyMap();
    assertEquals(emptyList, tc.decode(tc.encode(emptyList)));
    assertEquals(emptyMap, tc.decode(tc.encode(emptyMap)));
  }

  @Test
  void testPojoArray() {
    TestPojo[] arr = {
        new TestPojo("a", 1),
        new TestPojo("b", 2)
    };
    CachedData cd = tc.encode(arr);
    TestPojo[] decoded = (TestPojo[]) tc.decode(cd);
    assertEquals(arr.length, decoded.length);
    assertEquals(arr[1].getName(), decoded[1].getName());
  }

  @Test
  void testArrayWithNullElements() {
    String[] arr = new String[] {"x", null, "y"};
    CachedData cd = tc.encode(arr);
    String[] decoded = (String[]) tc.decode(cd);
    assertNull(decoded[1]);
  }

  @Test
  void testCorruptedSerializedCompressedData() {
    CachedData cd = tc.encode(new TestPojo("x", 1));
    CachedData fake = new CachedData(
        TranscoderUtils.SERIALIZED | TranscoderUtils.COMPRESSED,
        cd.getData(),
        cd.getData().length);
    TestPojo decoded = (TestPojo) tc.decode(fake);
    assertNull(decoded);
  }

  @Test
  void testVeryLargeByteArrayCompression() {
    tc.setCompressionThreshold(32);
    byte[] large = new byte[10_000];
    Arrays.fill(large, (byte) 1);
    CachedData cd = tc.encode(large);
    assertNotEquals(0, cd.getFlags() & TranscoderUtils.COMPRESSED);
    assertArrayEquals(large, (byte[]) tc.decode(cd));
  }

  @Test
  void testConcurrentEncodeDecode() throws Exception {
    ExecutorService es = Executors.newFixedThreadPool(8);
    List<Callable<Boolean>> tasks = new ArrayList<>();
    IntStream.range(0, 100).forEach(i -> tasks.add(() -> {
      TestPojo p = new TestPojo("n" + i, i);
      CachedData cd = tc.encode(p);
      TestPojo d = (TestPojo) tc.decode(cd);
      return p.getName().equals(d.getName()) && p.getValue() == d.getValue();
    }));
    List<Future<Boolean>> results = es.invokeAll(tasks, 30, TimeUnit.SECONDS);
    for (Future<Boolean> f : results) {
      assertTrue(f.get());
    }
    es.shutdown();
  }

  public static class TestPojo {
    private String name;
    private int value;

    public TestPojo() {
    }

    public TestPojo(String name, int value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }

  public static class NestedPojo {
    private String outerName;
    private TestPojo inner;

    public NestedPojo() {
    }

    public NestedPojo(String outerName, TestPojo inner) {
      this.outerName = outerName;
      this.inner = inner;
    }

    public String getOuterName() {
      return outerName;
    }

    public TestPojo getInner() {
      return inner;
    }
  }

  public enum SampleEnum {
    ONE("one"),
    TWO("two"),
    THREE("three");

    private final String text;

    SampleEnum(String text) {
      this.text = text;
    }

    public String getText() {
      return text;
    }
  }

}
