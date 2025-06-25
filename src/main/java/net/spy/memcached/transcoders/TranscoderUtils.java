// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.transcoders;

import java.io.UnsupportedEncodingException;
import net.spy.memcached.collection.ElementValueType;

/**
 * Utility class for transcoding Java types.
 */
public final class TranscoderUtils {

  // General flags
  public static final int SERIALIZED = 1;
  public static final int COMPRESSED = 2;

  // Special flags for specially handled types.
  public static final int SPECIAL_MASK = 0xff00;
  public static final int SPECIAL_BOOLEAN = (1 << 8);
  public static final int SPECIAL_INT = (2 << 8);
  public static final int SPECIAL_LONG = (3 << 8);
  public static final int SPECIAL_DATE = (4 << 8);
  public static final int SPECIAL_BYTE = (5 << 8);
  public static final int SPECIAL_FLOAT = (6 << 8);
  public static final int SPECIAL_DOUBLE = (7 << 8);
  public static final int SPECIAL_BYTEARRAY = (8 << 8);

  private static final String DEFAULT_CHARSET = "UTF-8";
  private final boolean packZeros;
  private String charset = DEFAULT_CHARSET;

  /**
   * Get an instance of TranscoderUtils.
   *
   * @param pack if true, remove all zero bytes from the MSB of the packed num
   */
  public TranscoderUtils(boolean pack) {
    super();
    packZeros = pack;
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

  public String getCharset() {
    return charset;
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

  public byte[] encodeNum(long l, int maxBytes) {
    byte[] rv = new byte[maxBytes];
    for (int i = 0; i < rv.length; i++) {
      int pos = rv.length - i - 1;
      rv[pos] = (byte) ((l >> (8 * i)) & 0xff);
    }
    if (packZeros) {
      int firstNon0 = 0;
      for (; firstNon0 < rv.length && rv[firstNon0] == 0; firstNon0++) {
        // Just looking for what we can reduce
      }
      if (firstNon0 > 0) {
        byte[] tmp = new byte[rv.length - firstNon0];
        System.arraycopy(rv, firstNon0, tmp, 0, rv.length - firstNon0);
        rv = tmp;
      }
    }
    return rv;
  }

  public byte[] encodeLong(long l) {
    return encodeNum(l, 8);
  }

  public long decodeLong(byte[] b) {
    long rv = 0;
    for (byte i : b) {
      rv = (rv << 8) | (i < 0 ? 256 + i : i);
    }
    return rv;
  }

  public byte[] encodeInt(int in) {
    return encodeNum(in, 4);
  }

  public int decodeInt(byte[] in) {
    assert in.length <= 4
            : "Too long to be an int (" + in.length + ") bytes";
    return (int) decodeLong(in);
  }

  public byte[] encodeByte(byte in) {
    return new byte[]{in};
  }

  public byte decodeByte(byte[] in) {
    assert in.length <= 1 : "Too long for a byte";
    byte rv = 0;
    if (in.length == 1) {
      rv = in[0];
    }
    return rv;
  }

  public byte[] encodeBoolean(boolean b) {
    byte[] rv = new byte[1];
    rv[0] = (byte) (b ? '1' : '0');
    return rv;
  }

  public boolean decodeBoolean(byte[] in) {
    assert in.length == 1 : "Wrong length for a boolean";
    return in[0] == '1';
  }

  /**
   * Encode the string into bytes using the given character set.
   */
  public byte[] encodeString(String in) {
    byte[] rv = null;
    try {
      rv = in.getBytes(charset);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return rv;
  }

  /**
   * Decode the string with the current character set.
   */
  public String decodeString(byte[] data) {
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
}
