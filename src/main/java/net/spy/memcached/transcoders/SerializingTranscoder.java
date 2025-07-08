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
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.transcoders;

import java.util.Date;

import net.spy.memcached.CachedData;

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
 * Transcoder that serializes and compresses objects.
 */
public class SerializingTranscoder extends BaseSerializingTranscoder
        implements Transcoder<Object> {

  /**
   * Maximum element size allowed by memcached collections.
   * The cache server's default setting of max_element_bytes is 16KB
   * and it can be changed up to 32KB
   */
  public static final int MAX_COLLECTION_ELEMENT_SIZE = 32 * 1024;

  private final boolean isCollection;
  private final boolean forceJDKSerializeForCollection;

  /**
   * Get a serializing transcoder with the default max data size.
   */
  public SerializingTranscoder() {
    this(CachedData.MAX_SIZE, null, false, false);
  }

  public SerializingTranscoder(int max) {
    this(max, null, false, false);
  }

  public SerializingTranscoder(int max, ClassLoader cl) {
    this(max, cl, false, false);
  }

  /**
   * Constructor with full customization.
   * Use static factory methods forKV() or forCollection() for default settings,
   * or Builder for custom configurations.
   */
  protected SerializingTranscoder(int max, ClassLoader cl, boolean isCollection,
                               boolean forceJDKSerializeForCollection) {
    super(max, cl);
    this.isCollection = isCollection;
    this.forceJDKSerializeForCollection = forceJDKSerializeForCollection;
  }

  /**
   * Factory method for general key-value usage.
   */
  public static Builder forKV() {
    return new Builder().forKV();
  }

  /**
   * Factory method for collection item usage.
   */
  public static Builder forCollection() {
    return new Builder().forCollection();
  }

  public Object decode(CachedData d) {
    byte[] data = d.getData();

    // Skip decompression for collections
    if (!isCollection && (d.getFlags() & COMPRESSED) != 0) {
      data = decompress(data);
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

  public CachedData encode(Object o) {
    byte[] b;
    int flags = 0;

    if (isCollection && forceJDKSerializeForCollection) {
      b = serialize(o);
      flags |= SERIALIZED;
      return new CachedData(flags, b, getMaxSize());
    }

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
    if (!isCollection && isCompressionCandidate(b)) {
      byte[] compressed = compress(b);
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

  /**
   * Builder for constructing SerializingTranscoder instances with custom settings.
   */
  public static final class Builder {
    private int max;
    private ClassLoader cl;
    private boolean isCollection;
    private boolean forceJDKSerializeForCollection;

    private Builder() {}

    Builder forKV() {
      this.max = CachedData.MAX_SIZE;
      this.cl = null;
      this.isCollection = false;
      this.forceJDKSerializeForCollection = false;
      return this;
    }

    Builder forCollection() {
      this.max = MAX_COLLECTION_ELEMENT_SIZE;
      this.cl = null;
      this.isCollection = true;
      this.forceJDKSerializeForCollection = false;
      return this;
    }

    public Builder maxSize(int max) {
      this.max = max;
      return this;
    }

    public Builder classLoader(ClassLoader cl) {
      this.cl = cl;
      return this;
    }

    /**
     * By default, this transcoder uses Java serialization only if the type is a user-defined class.
     * This mechanism may cause malfunction if you store Object type values
     * into an Arcus collection item and the values are actually
     * different types like String, Integer.
     * In this case, you should enable {@code forceJDKSerializeForCollection},
     * which enforces Java serialization for all values regardless of their actual type.
     * This option is only available for collection transcoders.
     */
    public Builder forceJDKSerializationForCollection() {
      if (!isCollection) {
        throw new IllegalStateException("forceJDKSerializationForCollection can only be " +
                "used with collection transcoders");
      }
      this.forceJDKSerializeForCollection = true;
      return this;
    }

    public SerializingTranscoder build() {
      return new SerializingTranscoder(max, cl, isCollection, forceJDKSerializeForCollection);
    }
  }
}
