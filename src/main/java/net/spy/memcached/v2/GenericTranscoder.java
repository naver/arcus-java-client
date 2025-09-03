package net.spy.memcached.v2;

import java.util.Date;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

public class GenericTranscoder<T> extends BaseSerializingTranscoder implements Transcoder<T> {

  // TODO: support max collection element bytes
  public static final int MAX_ELEMENT_BYTES = 1024 * 1024;

  // General flags
  static final int SERIALIZED = 1;

  protected static final int SPECIAL_MASK = 0xff00;
  static final int SPECIAL_BOOLEAN = (1 << 8);
  static final int SPECIAL_INT = (2 << 8);
  static final int SPECIAL_LONG = (3 << 8);
  static final int SPECIAL_DATE = (4 << 8);
  static final int SPECIAL_BYTE = (5 << 8);
  static final int SPECIAL_FLOAT = (6 << 8);
  static final int SPECIAL_DOUBLE = (7 << 8);
  static final int SPECIAL_BYTEARRAY = (8 << 8);

  public GenericTranscoder() {
    super(MAX_ELEMENT_BYTES);
  }

  @Override
  public CachedData encode(T o) {
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
    return new CachedData(flags, b, getMaxSize());
  }

  @Override
  @SuppressWarnings("unchecked")
  public T decode(CachedData d) {
    byte[] data = d.getData();
    T rv = null;
    int flags = d.getFlags() & SPECIAL_MASK;
    if ((d.getFlags() & SERIALIZED) != 0 && data != null) {
      rv = (T) deserialize(data);
    } else if (flags != 0 && data != null) {
      switch (flags) {
        case SPECIAL_BOOLEAN:
          rv = (T) Boolean.valueOf(tu.decodeBoolean(data));
          break;
        case SPECIAL_INT:
          rv = (T) Integer.valueOf(tu.decodeInt(data));
          break;
        case SPECIAL_LONG:
          rv = (T) Long.valueOf(tu.decodeLong(data));
          break;
        case SPECIAL_DATE:
          rv = (T) new Date(tu.decodeLong(data));
          break;
        case SPECIAL_BYTE:
          rv = (T) Byte.valueOf(tu.decodeByte(data));
          break;
        case SPECIAL_FLOAT:
          rv = (T) Float.valueOf(Float.intBitsToFloat(tu.decodeInt(data)));
          break;
        case SPECIAL_DOUBLE:
          rv = (T) Double.valueOf(Double.longBitsToDouble(tu.decodeLong(data)));
          break;
        case SPECIAL_BYTEARRAY:
          rv = (T) data;
          break;
        default:
          getLogger().warn("Undecodeable with flags %x", flags);
      }
    } else if (data != null) {
      rv = (T) tu.decodeString(data);
    }
    return rv;
  }
}
