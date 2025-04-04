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
package net.spy.memcached.collection;

import java.util.Arrays;
import java.util.Objects;

public class BTreeElement<K, V> {
  private final K bkey;
  private final V value;
  private final byte[] eflag;

  public BTreeElement(K bkey, byte[] eflag, V value) {
    this.bkey = bkey;
    this.eflag = eflag;
    this.value = value;
  }

  public K getBkey() {
    return bkey;
  }

  public V getValue() {
    return value;
  }

  public byte[] getEflag() {
    return eflag;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BTreeElement)) {
      return false;
    }

    BTreeElement<?, ?> that = (BTreeElement<?, ?>) o;
    return Objects.equals(bkey, that.bkey) &&
            Objects.equals(value, that.value) &&
            Arrays.equals(eflag, that.eflag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bkey, value, Arrays.hashCode(eflag));
  }

}
