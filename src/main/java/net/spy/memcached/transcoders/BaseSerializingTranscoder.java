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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Proxy;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.CloseUtil;
import net.spy.memcached.compat.SpyObject;

/**
 * Base class for any transcoders that may want to work with serialized or
 * compressed data.
 */
public abstract class BaseSerializingTranscoder extends SpyObject {

  /**
   * Default compression threshold value.
   */
  public static final int DEFAULT_COMPRESSION_THRESHOLD = 16384;

  private static final String DEFAULT_CHARSET = "UTF-8";

  protected int compressionThreshold = DEFAULT_COMPRESSION_THRESHOLD;
  protected String charset = DEFAULT_CHARSET;

  private final int maxSize;

  /*
   * This specifies which class loader to use for deserialization
   * when there are multiple class loaders.
   * If this is null, java default classloader will be used.
   */
  private final ClassLoader classLoader;

  /**
   * Initialize a serializing transcoder with the given maximum data size.
   */
  public BaseSerializingTranscoder(int max) {
    super();
    maxSize = max;
    classLoader = null;
  }

  public BaseSerializingTranscoder(int max, ClassLoader cl) {
    super();
    maxSize = max;
    classLoader = cl;
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
    compressionThreshold = to;
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
        ObjectInputStream is = new ClassLoaderObjectInputStream(bis, this.classLoader);
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
   * Compress the given array of bytes.
   */
  protected byte[] compress(byte[] in) {
    if (in == null) {
      throw new NullPointerException("Can't compress null");
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GZIPOutputStream gz = null;
    try {
      gz = new GZIPOutputStream(bos);
      gz.write(in);
    } catch (IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    } finally {
      CloseUtil.close(gz);
      CloseUtil.close(bos);
    }
    byte[] rv = bos.toByteArray();
    getLogger().debug("Compressed %d bytes to %d", in.length, rv.length);
    return rv;
  }

  /**
   * Decompress the given array of bytes.
   *
   * @return null if the bytes cannot be decompressed
   */
  protected byte[] decompress(byte[] in) {
    ByteArrayOutputStream bos = null;
    if (in != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(in);
      bos = new ByteArrayOutputStream();
      GZIPInputStream gis;
      try {
        gis = new GZIPInputStream(bis);

        byte[] buf = new byte[8192];
        int r = -1;
        while ((r = gis.read(buf)) > 0) {
          bos.write(buf, 0, r);
        }
      } catch (IOException e) {
        getLogger().warn("Failed to decompress data", e);
        bos = null;
      }
    }
    return bos == null ? null : bos.toByteArray();
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

  private static final class ClassLoaderObjectInputStream extends ObjectInputStream {
    private final ClassLoader classLoader;

    private ClassLoaderObjectInputStream(InputStream in,
                                         ClassLoader classLoader) throws IOException {
      super(in);
      this.classLoader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass classDesc)
            throws IOException, ClassNotFoundException {
      if (this.classLoader != null) {
        try {
          return Class.forName(classDesc.getName(), false, this.classLoader);
        } catch (ClassNotFoundException e) {
          return super.resolveClass(classDesc);
        }
      } else {
        return super.resolveClass(classDesc);
      }
    }


    @Override
    @SuppressWarnings("deprecation") // for java 17 and above
    protected Class<?> resolveProxyClass(String[] interfaces)
            throws IOException, ClassNotFoundException {
      if (this.classLoader != null) {
        Class<?>[] resolvedInterfaces = new Class<?>[interfaces.length];

        for (int i = 0; i < interfaces.length; ++i) {
          resolvedInterfaces[i] = Class.forName(interfaces[i], false, this.classLoader);
        }

        return Proxy.getProxyClass(this.classLoader, resolvedInterfaces);
      } else {
        return super.resolveProxyClass(interfaces);
      }
    }
  }
}
