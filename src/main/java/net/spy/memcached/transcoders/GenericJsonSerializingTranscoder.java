package net.spy.memcached.transcoders;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

import java.io.IOException;
import java.util.Date;

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

/**
 * Transcoder that serializes and deserializes objects to and from JSON.
 */
public class GenericJsonSerializingTranscoder extends SpyObject implements Transcoder<Object> {

  private final ObjectMapper objectMapper;
  private final int maxSize;
  private final CompressionUtils cu;
  private final TranscoderUtils tu;

  /**
   * Construct a new GenericJsonSerializingTranscoder
   *
   * @param objectMapper         the object mapper to use. This transcoder enables
   *                             polymorphic typing to preserve concrete types.
   *                             Jackson polymorphic deserialization can be vulnerable
   *                             for any untrusted JSON input if default typing is
   *                             too permissive without proper validation.
   *                             It is recommended to configure a restrictive
   *                             {@code BasicPolymorphicTypeValidator}
   * @param typeHintPropertyName the property name to use for type hints.
   *                             If {@code null} is given, do not set DefaultTyping of ObjectMapper.
   *                             If empty String is given, set DefaultTyping of ObjectMapper with
   *                             default type property name ("@class").
   *                             Otherwise, set DefaultTyping of ObjectMapper with given String
   *                             to write type info into JSON.
   * @param max                  the maximum size of a serialized object
   */
  public GenericJsonSerializingTranscoder(ObjectMapper objectMapper, String typeHintPropertyName,
                                          int max) {
    this(objectMapper, max);

    if (typeHintPropertyName != null) {
      @SuppressWarnings("deprecation")
      StdTypeResolverBuilder typer = new ObjectMapper.DefaultTypeResolverBuilder(
          ObjectMapper.DefaultTyping.EVERYTHING, this.objectMapper.getPolymorphicTypeValidator());
      typer = typer.init(JsonTypeInfo.Id.CLASS, null);
      typer = typer.inclusion(JsonTypeInfo.As.PROPERTY);
      typer = typer.typeProperty(typeHintPropertyName);
      this.objectMapper.setDefaultTyping(typer);
    }
  }

  public GenericJsonSerializingTranscoder(ObjectMapper objectMapper, int max) {
    if (objectMapper == null) {
      throw new IllegalArgumentException("ObjectMapper must not be null");
    }
    this.objectMapper = objectMapper;
    this.maxSize = max;
    this.cu = new CompressionUtils();
    this.tu = new TranscoderUtils(true);
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
    tu.setCharset(to);
  }

  public String getCharset() {
    return tu.getCharset();
  }

  @Override
  public Object decode(CachedData d) {
    byte[] data = d.getData();
    if (data == null) {
      return null; // No data to decode
    }

    if ((d.getFlags() & COMPRESSED) != 0) {
      data = cu.decompress(data);
    }

    Object rv = null;
    int flags = d.getFlags() & SPECIAL_MASK;
    if ((d.getFlags() & SERIALIZED) != 0 && data != null) {
      rv = deserialize(data);
    } else if (flags != 0 && data != null) {
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
    return rv;
  }

  @Override
  public CachedData encode(Object o) {
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

  private Object deserialize(byte[] data) {
    try {
      return objectMapper.readValue(data, Object.class);
    } catch (DatabindException e) {
      getLogger().warn("Caught DatabindException decoding %d bytes of data",
          data == null ? 0 : data.length, e);
    } catch (IOException e) {
      getLogger().warn("Caught IOException decoding %d bytes of data",
          data == null ? 0 : data.length, e);
    }
    return null;
  }

  private byte[] serialize(Object o) {
    try {
      return objectMapper.writeValueAsBytes(o);
    } catch (IOException e) {
      throw new IllegalArgumentException("Non-serializable object, cause=" + e.getMessage(), e);
    }
  }
}

