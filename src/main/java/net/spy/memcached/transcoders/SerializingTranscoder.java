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

/**
 * Transcoder that serializes and compresses objects.
 */
public class SerializingTranscoder extends BaseSerializingTranscoder
        implements Transcoder<Object> {

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

  protected final TranscoderUtils tu = new TranscoderUtils(true);

  /**
   * Get a serializing transcoder with the default max data size.
   */
  public SerializingTranscoder() {
    this(CachedData.MAX_SIZE, null);
  }

  /**
   * Get a serializing transcoder that specifies the max data size.
   */
  public SerializingTranscoder(int max) {
    super(max, null);
  }

  /**
   * Get a serializing transcoder that specifies the max data size and classloader.
   */
  public SerializingTranscoder(int max, ClassLoader cl) {
    super(max, cl);
  }

  @Override
  public boolean asyncDecode(CachedData d) {
    if ((d.getFlags() & COMPRESSED) != 0
            || (d.getFlags() & SERIALIZED) != 0) {
      return true;
    }
    return super.asyncDecode(d);
  }

  public Object decode(CachedData d) {
    byte[] data = d.getData();
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
          getLogger().warn("Undecodeable with flags %x", flags);
      }
    } else {
      rv = decodeString(data);
    }
    return rv;
  }

  public CachedData encode(Object o) {
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

}
