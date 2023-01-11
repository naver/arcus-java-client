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

/**
 * Transcoder that provides compatibility with Greg Whalin's memcached client.
 */
public class WhalinTranscoder extends BaseSerializingTranscoder
        implements Transcoder<Object> {

  static final int SPECIAL_BYTE = 1;
  static final int SPECIAL_BOOLEAN = 8192;
  static final int SPECIAL_INT = 4;
  static final int SPECIAL_LONG = 16384;
  static final int SPECIAL_CHARACTER = 16;
  static final int SPECIAL_STRING = 32;
  static final int SPECIAL_STRINGBUFFER = 64;
  static final int SPECIAL_FLOAT = 128;
  static final int SPECIAL_SHORT = 256;
  static final int SPECIAL_DOUBLE = 512;
  static final int SPECIAL_DATE = 1024;
  static final int SPECIAL_STRINGBUILDER = 2048;
  static final int SPECIAL_BYTEARRAY = 4096;
  static final int COMPRESSED = 2;
  static final int SERIALIZED = 8;

  private final TranscoderUtils tu = new TranscoderUtils(false);

  public WhalinTranscoder() {
    super(CachedData.MAX_SIZE);
  }

  public Object decode(CachedData d) {
    byte[] data = d.getData();
    Object rv = null;
    if ((d.getFlags() & COMPRESSED) != 0) {
      data = decompress(d.getData());
    }
    if ((d.getFlags() & SERIALIZED) != 0) {
      rv = deserialize(data);
    } else {
      int f = d.getFlags() & ~COMPRESSED;
      switch (f) {
        case SPECIAL_BOOLEAN:
          rv = this.decodeBoolean(data);
          break;
        case SPECIAL_INT:
          rv = tu.decodeInt(data);
          break;
        case SPECIAL_SHORT:
          rv = (short) tu.decodeInt(data);
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
        case SPECIAL_STRING:
          rv = decodeString(data);
          break;
        case SPECIAL_STRINGBUFFER:
          rv = new StringBuffer(decodeString(data));
          break;
        case SPECIAL_STRINGBUILDER:
          rv = new StringBuilder(decodeString(data));
          break;
        case SPECIAL_CHARACTER:
          rv = decodeCharacter(data);
          break;
        default:
          getLogger().warn("Cannot handle data with flags %x", f);
      }
    }
    return rv;
  }

  public CachedData encode(Object o) {
    byte[] b = null;
    int flags = 0;
    if (o instanceof String) {
      b = encodeString((String) o);
      flags |= SPECIAL_STRING;
    } else if (o instanceof StringBuffer) {
      flags |= SPECIAL_STRINGBUFFER;
      b = encodeString(String.valueOf(o));
    } else if (o instanceof StringBuilder) {
      flags |= SPECIAL_STRINGBUILDER;
      b = encodeString(String.valueOf(o));
    } else if (o instanceof Long) {
      b = tu.encodeLong((Long) o);
      flags |= SPECIAL_LONG;
    } else if (o instanceof Integer) {
      b = tu.encodeInt((Integer) o);
      flags |= SPECIAL_INT;
    } else if (o instanceof Short) {
      b = tu.encodeInt((Short) o);
      flags |= SPECIAL_SHORT;
    } else if (o instanceof Boolean) {
      b = this.encodeBoolean((Boolean) o);
      flags |= SPECIAL_BOOLEAN;
    } else if (o instanceof Date) {
      b = tu.encodeLong(((Date) o).getTime());
      flags |= SPECIAL_DATE;
    } else if (o instanceof Byte) {
      b = tu.encodeByte((Byte) o);
      flags |= SPECIAL_BYTE;
    } else if (o instanceof Float) {
      b = tu.encodeInt(Float.floatToIntBits((Float) o));
      flags |= SPECIAL_FLOAT;
    } else if (o instanceof Double) {
      b = tu.encodeLong(Double.doubleToLongBits((Double) o));
      flags |= SPECIAL_DOUBLE;
    } else if (o instanceof byte[]) {
      b = (byte[]) o;
      flags |= SPECIAL_BYTEARRAY;
    } else if (o instanceof Character) {
      b = tu.encodeInt((Character) o);
      flags |= SPECIAL_CHARACTER;
    } else {
      b = serialize(o);
      flags |= SERIALIZED;
    }
    assert b != null;
    if (b.length > compressionThreshold) {
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

  protected Character decodeCharacter(byte[] b) {
    return Character.valueOf((char) tu.decodeInt(b));
  }

  public byte[] encodeBoolean(boolean b) {
    byte[] rv = new byte[1];
    rv[0] = (byte) (b ? 1 : 0);
    return rv;
  }

  public boolean decodeBoolean(byte[] in) {
    assert in.length == 1 : "Wrong length for a boolean";
    return in[0] == 1;
  }


}
