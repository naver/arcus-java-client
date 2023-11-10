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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.SpyObject;

/**
 * Base class for any transcoders that may want to work with serialized or
 * compressed data.
 */
public abstract class BaseSerializingTranscoder extends SpyObject {
  private static final String DEFAULT_CHARSET = "UTF-8";
  protected String charset = DEFAULT_CHARSET;
  private final Compressor compressor;
  private final int maxSize;

  /**
   * Initialize a serializing transcoder with the given maximum data size.
   */
  public BaseSerializingTranscoder(int max) {
    this(max, new GzipCompressor());
  }

  /**
   * Initialize a serializing transcoder with the given maximum data size and compressor.
   */
  public BaseSerializingTranscoder(int max, Compressor compressor) {
    super();
    this.compressor = compressor;
    maxSize = max;
  }

  public boolean asyncDecode(CachedData d) {
    return false;
  }

  /**
   * Set the compression threshold to the given number of bytes.  This
   * transcoder will attempt to compress any data being stored that's larger
   * than this.
   *
   * @param to the number of bytes
   */
  public void setCompressionThreshold(int to) {
    if (compressor != null) {
      compressor.setCompressionThreshold(to);
    }
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

  protected CachedData doCompress(byte[] before, int flags, Class<?> type) {
    if (compressor == null) {
      return new CachedData(flags, before, getMaxSize());
    }
    if (before.length > compressor.getCompressionThreshold()) {
      byte[] compressed = compressor.compress(before);
      if (compressed.length < before.length) {
        getLogger().debug("Compressed %s from %d to %d", type.getName(),
                before.length, compressed.length);
        before = compressed;
        flags |= SerializingTranscoder.COMPRESSED;
      } else if (compressed.length > before.length) {
        getLogger().info("Compression increased the size of %s from %d to %d",
                type.getName(), before.length, compressed.length);
      } else {
        getLogger().info("Compression makes same length of %s : %d",
                type.getName(), before.length);
      }
    }
    return new CachedData(flags, before, getMaxSize());
  }

  protected byte[] doDecompress(CachedData cachedData) {
    if (compressor == null) {
      return cachedData.getData();
    }
    return compressor.decompress(cachedData.getData());
  }

  /**
   * Get the bytes representing the given serialized object.
   */
  protected byte[] serialize(Object o) {
    if (o == null) {
      throw new NullPointerException("Can't serialize null");
    }
    byte[] rv = null;
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream os = new ObjectOutputStream(bos);
      os.writeObject(o);
      os.close();
      bos.close();
      rv = bos.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Non-serializable object, cause=" + e.getMessage(), e);
    }
    return rv;
  }

  /**
   * Get the object represented by the given serialized bytes.
   */
  protected Object deserialize(byte[] in) {
    Object rv = null;
    try {
      if (in != null) {
        ByteArrayInputStream bis = new ByteArrayInputStream(in);
        ObjectInputStream is = new ObjectInputStream(bis);
        rv = is.readObject();
        is.close();
        bis.close();
      }
    } catch (IOException e) {
      getLogger().warn("Caught IOException decoding %d bytes of data",
              in == null ? 0 : in.length, e);
    } catch (ClassNotFoundException e) {
      getLogger().warn("Caught CNFE decoding %d bytes of data",
              in == null ? 0 : in.length, e);
    }
    return rv;
  }

  /**
   * Decode the string with the current character set.
   */
  protected String decodeString(byte[] data) {
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

  /**
   * Encode a string into the current character set.
   */
  protected byte[] encodeString(String in) {
    byte[] rv = null;
    try {
      rv = in.getBytes(charset);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return rv;
  }

  public int getMaxSize() {
    return maxSize;
  }

}
