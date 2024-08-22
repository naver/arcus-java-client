package net.spy.memcached.transcoders;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import net.spy.memcached.CachedData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base tests of the base serializing transcoder stuff.
 */
public class BaseSerializingTranscoderTest {

  private Exposer ex;

  @BeforeEach
  protected void setUp() throws Exception {
    ex = new Exposer();
  }

  @Test
  public void testValidCharacterSet() {
    ex.setCharset("KOI8");
  }

  @Test
  public void testInvalidCharacterSet() {
    try {
      ex.setCharset("Dustin's Kick Ass Character Set");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof UnsupportedEncodingException);
    }
  }

  @Test
  public void testCompressNull() {
    try {
      ex.compress(null);
      fail("Expected an assertion error");
    } catch (NullPointerException e) {
      // pass
    }
  }

  @Test
  public void testDecodeStringNull() {
    assertNull(ex.decodeString(null));
  }

  @Test
  public void testDeserializeNull() {
    assertNull(ex.deserialize(null));
  }

  @Test
  public void testEncodeStringNull() {
    try {
      ex.encodeString(null);
      fail("Expected an assertion error");
    } catch (NullPointerException e) {
      // pass
    }
  }

  @Test
  public void testSerializeNull() {
    try {
      ex.serialize(null);
      fail("Expected an assertion error");
    } catch (NullPointerException e) {
      // pass
    }
  }

  @Test
  public void testDecompressNull() {
    assertNull(ex.decompress(null));
  }

  @Test
  public void testUndeserializable() throws Exception {
    byte[] data = {
      -84, -19, 0, 5, 115, 114, 0, 4, 84, 101, 115, 116, 2, 61, 102,
      -87, -28, 17, 52, 30, 2, 0, 1, 73, 0, 9, 115, 111, 109, 101,
      116, 104, 105, 110, 103, 120, 112, 0, 0, 0, 5
    };
    assertNull(ex.deserialize(data));
  }

  @Test
  public void testDeserializable() throws Exception {
    byte[] data = {-84, -19, 0, 5, 116, 0, 5, 104, 101, 108, 108, 111};
    assertEquals("hello", ex.deserialize(data));
  }

  @Test
  public void testBadCharsetDecode() {
    ex.overrideCharsetSet("Some Crap");
    try {
      ex.encodeString("Woo!");
      fail("Expected runtime exception");
    } catch (RuntimeException e) {
      assertSame(UnsupportedEncodingException.class,
              e.getCause().getClass());
    }
  }

  @Test
  public void testBadCharsetEncode() {
    ex.overrideCharsetSet("Some Crap");
    try {
      ex.decodeString("Woo!".getBytes());
      fail("Expected runtime exception");
    } catch (RuntimeException e) {
      assertSame(UnsupportedEncodingException.class,
              e.getCause().getClass());
    }
  }

  @Test
  public void testDifferentClassLoaderMakeDifferentValue() throws Exception {
    // load CustomEntry class with custom class loader.
    CustomClassLoader customClassLoader = new CustomClassLoader();
    customClassLoader.findClass("net.spy.memcached.transcoders." +
            "BaseSerializingTranscoderTest$CustomEntry");
    CustomEntry customEntry = new CustomEntry(1L, "one");

    // serialize and deserialize with default class loader.
    SerializingTranscoder tcWithDefaultClassLoader = new SerializingTranscoder(CachedData.MAX_SIZE);
    byte[] bytes1 = tcWithDefaultClassLoader.serialize(customEntry);
    Object deserialized1 = tcWithDefaultClassLoader.deserialize(bytes1);

    // serialize and deserialize with custom class loader.
    SerializingTranscoder tcWithCustomClassLoader
            = new SerializingTranscoder(CachedData.MAX_SIZE, customClassLoader);
    byte[] bytes2 = tcWithCustomClassLoader.serialize(customEntry);
    Object deserialized2 = tcWithCustomClassLoader.deserialize(bytes2);

    assertArrayEquals(bytes1, bytes2);
    assertThrows(ClassCastException.class, () -> deserialized1.getClass().cast(deserialized2));
    assertThrows(ClassCastException.class, () -> deserialized2.getClass().cast(deserialized1));
    assertEquals(deserialized1.getClass().getName(), deserialized2.getClass().getName());
    assertEquals(deserialized1.getClass(), CustomEntry.class);
    assertEquals(deserialized1.getClass().getClassLoader(), this.getClass().getClassLoader());
    assertEquals(deserialized2.getClass().getClassLoader(), customClassLoader);
    assertNotSame(this.getClass().getClassLoader(), customClassLoader);
  }

  public static class CustomClassLoader extends ClassLoader {
    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      String resourceName = name.replace('.', '/') + ".class";

      try (InputStream input = getClass().getClassLoader().getResourceAsStream(resourceName)) {
        if (input == null) {
          throw new ClassNotFoundException("Could not find resource: " + resourceName);
        }
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        int nextValue;
        while ((nextValue = input.read()) != -1) {
          byteStream.write(nextValue);
        }
        byte[] classData = byteStream.toByteArray();
        return defineClass(name, classData, 0, classData.length);
      } catch (Exception e) {
        throw new ClassNotFoundException("Could not load class " + name, e);
      }
    }
  }

  public static class CustomEntry implements Serializable {
    private static final long serialVersionUID = 1000101L;
    private final Long key;
    private final String value;

    public CustomEntry(Long key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  // Expose the protected methods so I can test them.
  static class Exposer extends BaseSerializingTranscoder {

    public Exposer() {
      super(CachedData.MAX_SIZE);
    }

    public void overrideCharsetSet(String to) {
      charset = to;
    }

    @Override
    public byte[] compress(byte[] in) {
      return super.compress(in);
    }

    @Override
    public String decodeString(byte[] data) {
      return super.decodeString(data);
    }

    @Override
    public byte[] decompress(byte[] in) {
      return super.decompress(in);
    }

    @Override
    public Object deserialize(byte[] in) {
      return super.deserialize(in);
    }

    @Override
    public byte[] encodeString(String in) {
      return super.encodeString(in);
    }

    @Override
    public byte[] serialize(Object o) {
      return super.serialize(o);
    }

  }
}
