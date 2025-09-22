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

/*
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.SpyObject;

import static net.spy.memcached.transcoders.TranscoderUtils.COMPRESSED;
import static net.spy.memcached.transcoders.TranscoderUtils.SERIALIZED;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_BOOLEAN;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_BYTE;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_BYTEARRAY;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_DATE;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_DOUBLE;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_FLOAT;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_INT;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_LONG;
import static net.spy.memcached.transcoders.TranscoderUtils.SPECIAL_MASK;
*/

/**
 * Transcoder that serializes and deserializes objects to and from JSON.
 * @param <T> the type of objects to be transcoded
 */
/*
public class JsonSerializingTranscoder<T> extends SpyObject implements Transcoder<T> {

  private final ObjectMapper objectMapper;
  private final JavaType javaType;
  private final int maxSize;
  private final CompressionUtils cu;
  private final TranscoderUtils tu;

  public JsonSerializingTranscoder(Class<T> clazz) {
    this(CachedData.MAX_SIZE, clazz);
  }

  public JsonSerializingTranscoder(JavaType javaType) {
    this(CachedData.MAX_SIZE, javaType);
  }

  public JsonSerializingTranscoder(int max, Class<T> clazz) {
    this.maxSize = max;
    this.objectMapper = new ObjectMapper();
    this.javaType = objectMapper.getTypeFactory().constructType(clazz);
    this.cu = new CompressionUtils();
    this.tu = new TranscoderUtils(true);
  }

  public JsonSerializingTranscoder(int max, JavaType javaType) {
    this.maxSize = max;
    this.objectMapper = new ObjectMapper();
    this.javaType = Objects.requireNonNull(javaType, "JavaType must not be null");
    this.cu = new CompressionUtils();
    this.tu = new TranscoderUtils(true);
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }
*/
  /**
   * Set the compression threshold to the given number of bytes.  This
   * transcoder will attempt to compress any data being stored that's larger
   * than this.
   *
   * @param threshold the number of bytes
   */
/*
  public void setCompressionThreshold(int threshold) {
    cu.setCompressionThreshold(threshold);
  }
*/
  /**
   * Set the character set for string value transcoding (defaults to UTF-8).
   */
/*
  public void setCharset(String to) {
    tu.setCharset(to);
  }

  public String getCharset() {
    return tu.getCharset();
  }

  @SuppressWarnings("unchecked")
  public T decode(CachedData d) {
    byte[] data = d.getData();
    if (data == null) {
      return null; // No data to decode
    }

    if ((d.getFlags() & COMPRESSED) != 0) {
      data = cu.decompress(data);
    }

    Object rv = null;
    int flags = d.getFlags() & SPECIAL_MASK;
    if ((d.getFlags() & SERIALIZED) != 0) {
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
          getLogger().warn("Unable to decode: Unknown flag %x", flags);
      }
    } else {
      rv = tu.decodeString(data);
    }
    return (T) rv;
  }

  @Override
  public CachedData encode(T o) {
    if (o == null) {
      throw new NullPointerException("Can't encode null");
    }

    byte[] b;
    int flags = 0;

    if (o instanceof String) {
      b = tu.encodeString((String) o);
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
    if (cu.isCompressionCandidate(b)) {
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

  private T deserialize(byte[] data) {
    try {
      return objectMapper.readValue(data, javaType);
    } catch (DatabindException e) {
      getLogger().warn("Caught DatabindException decoding %d bytes of data",
              data == null ? 0 : data.length, e);
    } catch (IOException e) {
      getLogger().warn("Caught IOException decoding %d bytes of data",
              data == null ? 0 : data.length, e);
    }
    return null;
  }

  private byte[] serialize(T o) {
    try {
      return objectMapper.writeValueAsBytes(o);
    } catch (IOException e) {
      throw new IllegalArgumentException("Non-serializable object, cause=" + e.getMessage(), e);
    }
  }
}
*/
