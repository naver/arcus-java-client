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
import net.spy.memcached.collection.ElementValueType;

/**
 * Transcoder that serializes and compresses objects.
 */
public class SerializingTranscoder extends BaseSerializingTranscoder
        implements Transcoder<Object> {

  // General flags
  static final int SERIALIZED = 1;
  static final int COMPRESSED = 2;

  /**
   * Maximum element size allowed by memcached collections.
   * The cache server's default setting of max_element_bytes is 16KB
   * and it can be changed up to 32KB
   */
  public static final int MAX_COLLECTION_ELEMENT_SIZE = 32 * 1024;

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

  private final boolean isCollection;
  private final boolean optimize;

  protected final TranscoderUtils tu = new TranscoderUtils(true);

  // Constructors for various use cases
  public SerializingTranscoder() {
    this(CachedData.MAX_SIZE, null, false, false);
  }

  public SerializingTranscoder(int max) {
    this(max, null, false, false);
  }

  public SerializingTranscoder(ClassLoader cl) {
    this(CachedData.MAX_SIZE, cl, false, false);
  }

  public SerializingTranscoder(int max, ClassLoader cl) {
    this(max, cl, false, false);
  }

  public SerializingTranscoder(boolean isCollection) {
    this(isCollection ? MAX_COLLECTION_ELEMENT_SIZE : CachedData.MAX_SIZE,
            null, isCollection, isCollection);
  }

  public SerializingTranscoder(int max, boolean isCollection) {
    this(max, null, isCollection, isCollection);
  }

  public SerializingTranscoder(ClassLoader cl, boolean isCollection) {
    this(isCollection ? MAX_COLLECTION_ELEMENT_SIZE : CachedData.MAX_SIZE,
            cl, isCollection, isCollection);
  }

  public SerializingTranscoder(int max, ClassLoader cl, boolean isCollection) {
    this(max, cl, isCollection, isCollection);
  }

  /**
   * Constructor with full customization.
   */
  public SerializingTranscoder(int max, ClassLoader cl, boolean isCollection, boolean optimize) {
    super(max, cl);
    this.isCollection = isCollection;
    this.optimize = optimize;

    if (isCollection && max > MAX_COLLECTION_ELEMENT_SIZE) {
      throw new IllegalArgumentException("The maximum size cannot exceed " +
              MAX_COLLECTION_ELEMENT_SIZE + " in collection mode due to element size limitations.");
    }
  }

  /**
   * Factory method for general key-value usage.
   */  public static SerializingTranscoder forKV() {
    return new SerializingTranscoder(CachedData.MAX_SIZE, null, false, false);
  }

  /**
   * Factory method for collection item usage.
   */
  public static SerializingTranscoder forCollection() {
    return new SerializingTranscoder(MAX_COLLECTION_ELEMENT_SIZE, null, true, true);
  }

  /**
   * Determines the appropriate flags based on the element type.
   */
  public static int examineFlags(ElementValueType type) {
    int flags = 0;
    if (type == ElementValueType.STRING) {
      // string type has no flags.
    } else if (type == ElementValueType.LONG) {
      flags |= SPECIAL_LONG;
    } else if (type == ElementValueType.INTEGER) {
      flags |= SPECIAL_INT;
    } else if (type == ElementValueType.BOOLEAN) {
      flags |= SPECIAL_BOOLEAN;
    } else if (type == ElementValueType.DATE) {
      flags |= SPECIAL_DATE;
    } else if (type == ElementValueType.BYTE) {
      flags |= SPECIAL_BYTE;
    } else if (type == ElementValueType.FLOAT) {
      flags |= SPECIAL_FLOAT;
    } else if (type == ElementValueType.DOUBLE) {
      flags |= SPECIAL_DOUBLE;
    } else if (type == ElementValueType.BYTEARRAY) {
      flags |= SPECIAL_BYTEARRAY;
    } else {
      flags |= SERIALIZED;
    }
    return flags;
  }

  public Object decode(CachedData d) {
    byte[] data = d.getData();

    // Skip decompression for collections
    if (!isCollection && (d.getFlags() & COMPRESSED) != 0) {
      data = decompress(data);
    }

    Object rv = null;
    if ((d.getFlags() & COMPRESSED) != 0) {
      data = decompress(d.getData());
    }
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
      rv = decodeString(data);
    }
    return rv;
  }

  public CachedData encode(Object o) {
    byte[] b;
    int flags = 0;

    if (isCollection && !optimize) {
      b = serialize(o);
      flags |= SERIALIZED;
      return new CachedData(flags, b, getMaxSize());
    }

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
  public static class Builder {
    private int max = CachedData.MAX_SIZE;
    private ClassLoader cl = null;
    private boolean isCollection = false;
    private boolean optimize = false;

    public Builder setMaxSize(int max) {
      this.max = max;
      return this;
    }

    public Builder setClassLoader(ClassLoader cl) {
      this.cl = cl;
      return this;
    }

    public Builder forCollection() {
      this.isCollection = true;
      this.max = MAX_COLLECTION_ELEMENT_SIZE;
      return this;
    }

    public Builder enableOptimization() {
      this.optimize = true;
      return this;
    }

    /**
     * By default, this transcoder uses Java serialization only if the type is a user-defined class.
     * This mechanism may cause malfunction if you store Object type values
     * into an Arcus collection item and the values are actually
     * different types like String, Integer.
     * In this case, you should disable optimization.
     */
    public Builder disableOptimization() {
      this.optimize = false;
      return this;
    }

    public SerializingTranscoder build() {
      return new SerializingTranscoder(max, cl, isCollection, optimize);
    }
  }
}
