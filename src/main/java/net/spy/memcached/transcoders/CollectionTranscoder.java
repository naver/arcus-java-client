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

import net.spy.memcached.CachedData;

/**
 * @deprecated Use {@link SerializingTranscoder#forCollection()} instead.
 */
@Deprecated
public class CollectionTranscoder extends SerializingTranscoder {

  /**
   * Get a serializing transcoder with the default max data size.
   */
  public CollectionTranscoder() {
    super(true);
  }

  /**
   * Get a serializing transcoder that specifies the max data size.
   */
  public CollectionTranscoder(int max) {
    super(max, true);
  }

  /**
   * Get a serializing transcoder that specifies the max data size and class loader.
   */
  public CollectionTranscoder(int max, ClassLoader cl) {
    super(max, cl, true);
  }

  private CollectionTranscoder(int max, ClassLoader cl, boolean forceJDKSerializeForCollection) {
    super(max, cl, true, forceJDKSerializeForCollection);
  }

  @Override
  public CachedData encode(Object o) {
    return super.encode(o);
  }

  @Override
  public Object decode(CachedData d) {
    return super.decode(d);
  }

  /**
   * @deprecated Use {@link SerializingTranscoder.Builder} with {@code forCollection()} instead.
   */
  @Deprecated
  public static class Builder {
    private int max = MAX_COLLECTION_ELEMENT_SIZE;
    private boolean forceJDKSerializeForCollection = false;
    private ClassLoader cl;

    public Builder setMaxElementBytes(int max) {
      this.max = max;
      return this;
    }

    public Builder enableForceJDKSerialization() {
      this.forceJDKSerializeForCollection = true;
      return this;
    }

    public Builder setClassLoader(ClassLoader cl) {
      this.cl = cl;
      return this;
    }

    public CollectionTranscoder build() {
      return new CollectionTranscoder(max, cl, forceJDKSerializeForCollection);
    }
  }
}
