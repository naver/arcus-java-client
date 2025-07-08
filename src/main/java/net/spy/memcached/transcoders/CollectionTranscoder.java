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

/**
 * @deprecated Use {@link SerializingTranscoder#forCollection()} instead.
 */
@Deprecated
public class CollectionTranscoder extends SerializingTranscoder {

  public CollectionTranscoder() {
    super(MAX_COLLECTION_ELEMENT_SIZE, null, true, false);
  }

  public CollectionTranscoder(int max) {
    super(max, null, true, false);
  }

  public CollectionTranscoder(int max, ClassLoader cl) {
    super(max, cl, true, false);
  }

  private CollectionTranscoder(int max, ClassLoader cl, boolean forceJDKSerializeForCollection) {
    super(max, cl, true, forceJDKSerializeForCollection);
  }

  /**
   * @deprecated Use {@link SerializingTranscoder.Builder} with {@code forCollection()} instead.
   */
  @Deprecated
  public static class Builder {
    private int max = MAX_COLLECTION_ELEMENT_SIZE;
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
      return new CollectionTranscoder(max, cl, !optimize);
    }
  }
}
