package net.spy.memcached.transcoders;
/*
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.spy.memcached.CachedData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
*/
/**
 * Test the JSON serializing transcoder.
 */
/*
class JsonSerializingTranscoderTest {

  private ObjectMapper mapper;
  private TranscoderUtils tu;

  @BeforeEach
  void setUp() {
    mapper = new ObjectMapper();
    tu = new TranscoderUtils(true);
  }

  @Test
  void testValidCharacterSet() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    tc.setCharset("KOI8");
    assertEquals("KOI8", tc.getCharset());
  }

  @Test
  void testInvalidCharacterSet() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    try {
      tc.setCharset("Dustin's Kick Ass Character Set");
      fail("Expected a RuntimeException");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof UnsupportedEncodingException);
    }
  }

  @Test
  void testSetCompressionThreshold() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
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
    JsonSerializingTranscoder<String> customTranscoder =
            new JsonSerializingTranscoder<>(1024, String.class);
    assertEquals(1024, customTranscoder.getMaxSize());
  }

  @Test
  void testStrings() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    String s1 = "This is a simple test string.";
    CachedData cd = tc.encode(s1);
    assertEquals(0, cd.getFlags());
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  void testUTF8String() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    String s1 = "\u2013\u00f3\u2013\u00a5\u2014\u00c4\u2013\u221e\u2013"
            + "\u2264\u2014\u00c5\u2014\u00c7\u2013\u2264\u2014\u00c9\u2013"
            + "\u03c0, \u2013\u00ba\u2013\u220f\u2014\u00c4.";
    CachedData cd = tc.encode(s1);
    assertEquals(0, cd.getFlags());
    assertEquals(s1, tc.decode(cd));
  }

  @Test
  void testEmptyString() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    String emptyStr = "";
    CachedData cd = tc.encode(emptyStr);
    assertEquals(emptyStr, tc.decode(cd));
  }

  @Test
  void testLong() {
    JsonSerializingTranscoder<Long> tc = new JsonSerializingTranscoder<>(Long.class);
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
    JsonSerializingTranscoder<Integer> tc = new JsonSerializingTranscoder<>(Integer.class);
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
    JsonSerializingTranscoder<Short> tc = new JsonSerializingTranscoder<>(Short.class);
    assertEquals((short) 923, tc.decode(tc.encode((short) 923)));
  }

  @Test
  void testChar() {
    JsonSerializingTranscoder<Character> tc = new JsonSerializingTranscoder<>(Character.class);
    assertEquals('c', tc.decode(tc.encode('c')));
  }

  @Test
  void testBoolean() {
    JsonSerializingTranscoder<Boolean> tc = new JsonSerializingTranscoder<>(Boolean.class);
    assertTrue(tc.decode(tc.encode(true)));
    assertFalse(tc.decode(tc.encode(false)));
  }

  @Test
  void testByte() {
    JsonSerializingTranscoder<Byte> tc = new JsonSerializingTranscoder<>(Byte.class);
    assertEquals((byte) -127, tc.decode(tc.encode((byte) -127)));
  }

  @Test
  void testFloat() {
    JsonSerializingTranscoder<Float> tc = new JsonSerializingTranscoder<>(Float.class);
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
    JsonSerializingTranscoder<Double> tc = new JsonSerializingTranscoder<>(Double.class);
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
    JsonSerializingTranscoder<byte[]> tc = new JsonSerializingTranscoder<>(byte[].class);
    byte[] a = {'a', 'b', 'c'};
    CachedData cd = tc.encode(a);
    assertArrayEquals(a, tc.decode(cd));
  }

  @Test
  void testStringBuilder() {
    JsonSerializingTranscoder<StringBuilder> tc =
            new JsonSerializingTranscoder<>(StringBuilder.class);
    StringBuilder sb = new StringBuilder("test");
    StringBuilder sb2 = tc.decode(tc.encode(sb));
    assertEquals(sb.toString(), sb2.toString());
  }

  @Test
  void testStringBuffer() {
    JsonSerializingTranscoder<StringBuffer> tc =
            new JsonSerializingTranscoder<>(StringBuffer.class);
    StringBuffer sb = new StringBuffer("test");
    StringBuffer sb2 = tc.decode(tc.encode(sb));
    assertEquals(sb.toString(), sb2.toString());
  }

  @Test
  void testDate() {
    JsonSerializingTranscoder<Date> tc = new JsonSerializingTranscoder<>(Date.class);
    Date d = new Date();
    CachedData cd = tc.encode(d);
    assertEquals(d, tc.decode(cd));
  }

  @Test
  void testSomethingBigger() {
    JavaType collectionType = mapper.getTypeFactory()
            .constructCollectionType(Collection.class, Date.class);
    JsonSerializingTranscoder<Collection<Date>> tc =
            new JsonSerializingTranscoder<>(collectionType);
    Collection<Date> dates = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      dates.add(new Date());
    }
    CachedData d = tc.encode(dates);
    assertEquals(dates, tc.decode(d));
  }

  @Test
  void testJsonObject() {
    JsonSerializingTranscoder<TestPojo> tc = new JsonSerializingTranscoder<>(TestPojo.class);
    TestPojo pojo = new TestPojo("test", 123);
    
    CachedData cd = tc.encode(pojo);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());
    
    TestPojo decoded = tc.decode(cd);
    assertEquals(pojo.getName(), decoded.getName());
    assertEquals(pojo.getValue(), decoded.getValue());
  }

  @Test
  void testNestedPojo() {
    JsonSerializingTranscoder<NestedPojo> tc = new JsonSerializingTranscoder<>(NestedPojo.class);
    NestedPojo nested = new NestedPojo("outer", new TestPojo("inner", 789));
    
    CachedData cd = tc.encode(nested);
    NestedPojo decoded = tc.decode(cd);
    assertEquals(nested.getOuterName(), decoded.getOuterName());
    assertEquals(nested.getInner().getName(), decoded.getInner().getName());
    assertEquals(nested.getInner().getValue(), decoded.getInner().getValue());
  }

  @Test
  void testJsonList() {
    JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, String.class);
    JsonSerializingTranscoder<List<String>> tc = new JsonSerializingTranscoder<>(listType);
    List<String> list = Arrays.asList("item1", "item2", "item3");
    
    CachedData cd = tc.encode(list);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());
    
    List<String> decoded = tc.decode(cd);
    assertEquals(list, decoded);
  }

  @Test
  void testJsonArray() {
    JsonSerializingTranscoder<String[]> tc = new JsonSerializingTranscoder<>(String[].class);
    String[] array = {"item1", "item2", "item3"};
    
    CachedData cd = tc.encode(array);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());
    
    String[] decoded = tc.decode(cd);
    assertEquals(Arrays.asList(array), Arrays.asList(decoded));
  }

  @Test
  void testJsonMap() {
    JavaType mapType = mapper.getTypeFactory()
            .constructMapType(Map.class, String.class, Integer.class);
    JsonSerializingTranscoder<Map<String, Integer>> tc = new JsonSerializingTranscoder<>(mapType);
    Map<String, Integer> map = new HashMap<>();
    map.put("key1", 1);
    map.put("key2", 2);
    
    CachedData cd = tc.encode(map);
    assertEquals(TranscoderUtils.SERIALIZED, cd.getFlags());
    
    Map<String, Integer> decoded = tc.decode(cd);
    assertEquals(map, decoded);
  }

  @Test
  void testCompressedStringNotSmaller() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
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
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
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
    JsonSerializingTranscoder<TestPojo> tc = new JsonSerializingTranscoder<>(TestPojo.class);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append('x');
    }
    TestPojo largePojo = new TestPojo(sb.toString(), 123);
    tc.setCompressionThreshold(8);

    CachedData cd = tc.encode(largePojo);
    assertEquals(TranscoderUtils.SERIALIZED | TranscoderUtils.COMPRESSED, cd.getFlags());

    TestPojo decoded = tc.decode(cd);
    assertEquals(largePojo.getName(), decoded.getName());
    assertEquals(largePojo.getValue(), decoded.getValue());
  }

  @Test
  void testEncodeNull() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    assertThrows(NullPointerException.class, () -> tc.encode(null));
  }

  @Test
  void testDecodeEmpty() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    assertEquals("", tc.decode(new CachedData(0, new byte[0], tc.getMaxSize())));
  }

  @Test
  void testUndecodeable() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    CachedData cd = new CachedData(
            Integer.MAX_VALUE &
                    ~(TranscoderUtils.COMPRESSED | TranscoderUtils.SERIALIZED),
            tu.encodeInt(Integer.MAX_VALUE),
            tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Test
  void testUndecodeableSerialized() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    CachedData cd = new CachedData(
            TranscoderUtils.SERIALIZED,
            "invalid json data".getBytes(),
            tc.getMaxSize());
    assertNull(tc.decode(cd));
  }

  @Test
  void testUndecodeableCompressed() {
    JsonSerializingTranscoder<String> tc = new JsonSerializingTranscoder<>(String.class);
    CachedData cd = new CachedData(
            TranscoderUtils.COMPRESSED,
            "invalid compressed data".getBytes(),
            tc.getMaxSize());
    assertNull(tc.decode(cd));
  }


  public static class TestPojo {
    private String name;
    private int value;

    public TestPojo() {}

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

    public NestedPojo() {}

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
}
*/
