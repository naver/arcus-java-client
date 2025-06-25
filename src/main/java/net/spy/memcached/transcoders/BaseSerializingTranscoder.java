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
import java.lang.reflect.Proxy;

import net.spy.memcached.compat.SpyObject;

/**
 * Base class for any transcoders that may want to work with serialized or
 * compressed data.
 */
public abstract class BaseSerializingTranscoder extends SpyObject {

  private final int maxSize;

  /*
   * This specifies which class loader to use for deserialization
   * when there are multiple class loaders.
   * If this is null, java default classloader will be used.
   */
  private final ClassLoader classLoader;

  private final CompressionUtils cu = new CompressionUtils();
  protected final TranscoderUtils tu;

  /**
   * Initialize a serializing transcoder with the given maximum data size.
   */
  public BaseSerializingTranscoder(int max) {
    this(max, null, true);
  }

  public BaseSerializingTranscoder(int max, ClassLoader cl) {
    this(max, cl, true);
  }

  public BaseSerializingTranscoder(int max, ClassLoader cl, boolean pack) {
    super();
    this.maxSize = max;
    this.classLoader = cl;
    this.tu = new TranscoderUtils(pack);
  }

  /**
   * Set the compression threshold to the given number of bytes.  This
   * transcoder will attempt to compress any data being stored that's larger
   * than this.
   *
   * @param threshold the number of bytes
   */
  public void setCompressionThreshold(int threshold) {
    cu.setCompressionThreshold(threshold);
  }

  public String getCharset() {
    return tu.getCharset();
  }

  /**
   * Set the character set for string value transcoding (defaults to UTF-8).
   */
  public void setCharset(String to) {
    tu.setCharset(to);
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
   *
   * @param in the data to compress
   * @return the compressed data
   * @throws NullPointerException if the input is null
   */
  protected byte[] compress(byte[] in) {
    return cu.compress(in);
  }

  /**
   * Decompress the given array of bytes.
   *
   * @param in the compressed byte array, or null
   * @return the decompressed byte array, or null if input is null or decompression fails
   */
  protected byte[] decompress(byte[] in) {
    return cu.decompress(in);
  }

  /**
   * Check if the data should be compressed based on its length and the compression threshold.
   *
   * @param data the data to check
   * @return true if the data should be compressed, false otherwise
   */
  protected boolean isCompressionCandidate(byte[] data) {
    return cu.isCompressionCandidate(data);
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
