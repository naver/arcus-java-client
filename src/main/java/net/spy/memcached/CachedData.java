// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached;

import java.util.Arrays;

/**
 * Cached data with its attributes.
 */
public final class CachedData {

  /**
   * Maximum data size allowed by memcached.
   */
  public static final int MAX_SIZE = 1024 * 1024;

  private final int flags;
  private final byte[] data;
  private byte[] eFlag;

  /**
   * Get a CachedData instance for the given flags and byte array.
   *
   * @param flags       the flags
   * @param data        the data
   * @param max_size    the maximum allowable size.
   */
  public CachedData(int flags, byte[] data, int max_size) {
    super();
    if (data.length > max_size) {
      throw new IllegalArgumentException(
              "Cannot cache data larger than " + max_size
                      + " bytes (you tried to cache a "
                      + data.length + " byte object)");
    }
    this.flags = flags;
    this.data = data;
  }

  /**
   * Get a CachedData instance for the given flags and byte array.
   *
   * @param flag        the flags
   * @param data        the data
   * @param eFlag       the eFlag
   * @param max_size    the maximum allowable size.
   */
  public CachedData(int flag, byte[] data, byte[] eFlag, int max_size) {
    this(flag, data, max_size);
    this.eFlag = eFlag;
  }

  /**
   * Get the stored data.
   */
  public byte[] getData() {
    return data;
  }

  /**
   * Get the flags stored along with this value.
   */
  public int getFlags() {
    return flags;
  }

  public byte[] getEFlag() {
    return eFlag;
  }

  @Override
  public String toString() {
    return "{CachedData flags=" + flags +
            " data=" + Arrays.toString(data) +
            " eFlag=" + Arrays.toString(eFlag) + " }";
  }
}
