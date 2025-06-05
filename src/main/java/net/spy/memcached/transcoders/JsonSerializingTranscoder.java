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
package net.spy.memcached.transcoders;

import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Objects;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.SpyObject;

/**
 * Transcoder that serializes and deserializes objects to and from JSON.
 * @param <T> the type of objects to be transcoded
 */
public class JsonSerializingTranscoder<T> extends SpyObject implements Transcoder<T> {

  // General flags
  static final int SERIALIZED = 1;
  static final int COMPRESSED = 2;

  // Special flags for specially handled types.
  protected static final int SPECIAL_MASK = 0xff00;
  static final int SPECIAL_BOOLEAN = (1 << 8);
  static final int SPECIAL_INT = (2 << 8);
  static final int SPECIAL_LONG = (3 << 8);
  static final int SPECIAL_DATE = (4 << 8);
  static final int SPECIAL_BYTE = (5 << 8);
  static final int SPECIAL_FLOAT = (6 << 8);
  static final int SPECIAL_DOUBLE = (7 << 8);
  static final int SPECIAL_BYTEARRAY = (8 << 8);
  private static final String DEFAULT_CHARSET = "UTF-8";

  private final ObjectMapper objectMapper;
  private final JavaType javaType;
  private final int maxSize;
  private final CompressionUtils cu;
  protected final TranscoderUtils tu;
  protected String charset;

  public JsonSerializingTranscoder(Class<T> clazz) {
    this(CachedData.MAX_SIZE, clazz);
  }

  public JsonSerializingTranscoder(JavaType javaType) {
    this(CachedData.MAX_SIZE, javaType);
  }

  public JsonSerializingTranscoder(int max, Class<T> clazz) {
    this(max, createDefaultObjectMapper().getTypeFactory().constructType(clazz));
  }

  public JsonSerializingTranscoder(int max, JavaType javaType) {
    this.maxSize = max;
    this.objectMapper = createDefaultObjectMapper();
    this.javaType = Objects.requireNonNull(javaType, "JavaType must not be null");
    this.cu = new CompressionUtils();
    this.tu = new TranscoderUtils(true);
    this.charset = DEFAULT_CHARSET;
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }

  /**
   * Set the compression threshold to the given number of bytes.  This
   * transcoder will attempt to compress any data being stored that's larger
   * than this.
   *
   * @param threshold the number of bytes
   */
  public void setCompressionThreshold(int threshold) {
    cu.setCompressionThreshold(threshold);
  }

  /**
   * Set the character set for string value transcoding (defaults to UTF-8).
   */
  public void setCharset(String to) {
    // Validate the character set.
    try {
      new String(new byte[97], to);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    charset = to;
  }

  @SuppressWarnings("unchecked")
  public T decode(CachedData d) {
    byte[] data = d.getData();
    if (data == null) {
      return null; // No data to decode
    }

    if (isCompressed(d.getFlags())) {
      data = cu.decompress(d.getData());
    }

    Object rv = null;
    int flags = d.getFlags() & SPECIAL_MASK;
    if (isSerialized(d.getFlags())) {
      rv = deserialize(data);
    } else if (flags != 0) {
      switch (flags) {
        case SPECIAL_BOOLEAN:
          rv = tu.decodeBoolean(data);
          break;
        case SPECIAL_INT:
          rv = tu.decodeInt(data);
          break;
        case SPECIAL_LONG:
          rv = tu.decodeLong(data);
          break;
        case SPECIAL_DATE:
          rv = new Date(tu.decodeLong(data));
          break;
        case SPECIAL_BYTE:
          rv = tu.decodeByte(data);
          break;
        case SPECIAL_FLOAT:
          rv = Float.intBitsToFloat(tu.decodeInt(data));
          break;
        case SPECIAL_DOUBLE:
          rv = Double.longBitsToDouble(tu.decodeLong(data));
          break;
        case SPECIAL_BYTEARRAY:
          rv = data;
          break;
        default:
          getLogger().warn("Undecodeable with flags %x", flags);
      }
    } else {
      rv = decodeString(data);
    }
    return (T) rv;
  }

  @Override
  public CachedData encode(T o) {
    if (o == null) {
      throw new NullPointerException("Can't encode null");
    }

    byte[] b = null;
    int flags = 0;

    if (o instanceof String) {
      b = encodeString((String) o);
    } else if (o instanceof Long) {
      b = tu.encodeLong((Long) o);
      flags |= SPECIAL_LONG;
    } else if (o instanceof Integer) {
      b = tu.encodeInt((Integer) o);
      flags |= SPECIAL_INT;
    } else if (o instanceof Boolean) {
      b = tu.encodeBoolean((Boolean) o);
      flags |= SPECIAL_BOOLEAN;
    } else if (o instanceof Date) {
      b = tu.encodeLong(((Date) o).getTime());
      flags |= SPECIAL_DATE;
    } else if (o instanceof Byte) {
      b = tu.encodeByte((Byte) o);
      flags |= SPECIAL_BYTE;
    } else if (o instanceof Float) {
      b = tu.encodeInt(Float.floatToRawIntBits((Float) o));
      flags |= SPECIAL_FLOAT;
    } else if (o instanceof Double) {
      b = tu.encodeLong(Double.doubleToRawLongBits((Double) o));
      flags |= SPECIAL_DOUBLE;
    } else if (o instanceof byte[]) {
      b = (byte[]) o;
      flags |= SPECIAL_BYTEARRAY;
    } else {
      b = serialize(o);
      flags |= SERIALIZED;
    }
    assert b != null;
    if (isCompressionCandidate(b)) {
      byte[] compressed = cu.compress(b);
      if (compressed.length < b.length) {
        getLogger().debug("Compressed %s from %d to %d",
                o.getClass().getName(), b.length, compressed.length);
        b = compressed;
        flags |= COMPRESSED;
      } else {
        getLogger().info(
                "Compression increased the size of %s from %d to %d",
                o.getClass().getName(), b.length, compressed.length);
      }
    }
    return new CachedData(flags, b, getMaxSize());
  }

  protected T deserialize(byte[] data) {
    T rv = null;
    try {
      rv = objectMapper.readValue(data, javaType);
    } catch (DatabindException e) {
      getLogger().warn("Caught DatabindException decoding %d bytes of data",
              data == null ? 0 : data.length, e);
    } catch (IOException e) {
      getLogger().warn("Caught IOException decoding %d bytes of data",
              data == null ? 0 : data.length, e);
    }
    return rv;
  }

  protected byte[] serialize(T o) {
    try {
      return objectMapper.writeValueAsBytes(o);
    } catch (IOException e) {
      throw new IllegalArgumentException("Non-serializable object, cause=" + e.getMessage(), e);
    }
  }

  /**
   * Decode the string with the current character set.
   */
  protected String decodeString(byte[] data) {
    String rv = null;
    try {
      if (data != null) {
        rv = new String(data, charset);
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return rv;
  }

  /**
   * Encode a string into the current character set.
   */
  protected byte[] encodeString(String in) {
    byte[] rv = null;
    try {
      rv = in.getBytes(charset);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return rv;
  }

  /**
   * Check if the data should be compressed based on its length and the compression threshold.
   *
   * @param data the data to check
   * @return true if the data should be compressed, false otherwise
   */
  protected boolean isCompressionCandidate(byte[] data) {
    return cu.isCompressionCandidate(data);
  }

  /**
   * Check if the data is compressed based on the flags.
   *
   * @param flags the flags to check
   * @return true if the data is compressed, false otherwise
   */
  protected boolean isCompressed(int flags) {
    return (flags & COMPRESSED) != 0;
  }

  /**
   * Check if the data is compressed based on the flags.
   *
   * @param flags the flags to check
   * @return true if the data is compressed, false otherwise
   */
  protected boolean isSerialized(int flags) {
    return (flags & SERIALIZED) != 0;
  }

  private static ObjectMapper createDefaultObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.activateDefaultTyping(
            mapper.getPolymorphicTypeValidator(),
            ObjectMapper.DefaultTyping.NON_FINAL);
    return mapper;
  }
}
