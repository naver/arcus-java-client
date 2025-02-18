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

import java.util.Date;

import net.spy.memcached.CachedData;
import net.spy.memcached.collection.ElementValueType;

/**
 * Transcoder that serialized and compresses objects for collection elements.
 */
public class CollectionTranscoder extends SerializingTranscoder implements
        Transcoder<Object> {

  /**
   * Maximum element size allowed by memcached collections.
   * The cache server's default setting of max_element_bytes is 16KB
   * and it can be changed up to 32KB
   */
  public static final int MAX_ELEMENT_BYTES = 32 * 1024;

  private boolean optimize = true;

  /**
   * Get a serializing transcoder with the default max data size.
   */
  public CollectionTranscoder() {
    this(MAX_ELEMENT_BYTES);
  }

  /**
   * Get a serializing transcoder that specifies the max data size.
   */
  public CollectionTranscoder(int max) {
    super(max);
  }

  public CollectionTranscoder(int max, ClassLoader cl) {
    super(max, cl);
  }

  private CollectionTranscoder(int max, ClassLoader cl, boolean optimize) {
    super(max, cl);
    this.optimize = optimize;
  }

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

  @Override
  public Object decode(CachedData d) {
    byte[] data = d.getData();
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
          getLogger().warn("Undecodeable with flags %x", flags);
      }
    } else {
      rv = decodeString(data);
    }
    return rv;
  }

  @Override
  public CachedData encode(Object o) {
    byte[] b;
    int flags = 0;

    if (!optimize) {
      flags |= SERIALIZED;
      b = serialize(o);
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
    return new CachedData(flags, b, getMaxSize());
  }

  public static class Builder {
    private int max = MAX_ELEMENT_BYTES;
    private boolean optimize = true;
    private ClassLoader cl;

    public Builder setMaxElementBytes(int max) {
      this.max = max;
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

    public Builder setClassLoader(ClassLoader cl) {
      this.cl = cl;
      return this;
    }

    public CollectionTranscoder build() {
      return new CollectionTranscoder(max, cl, optimize);
    }
  }
}
