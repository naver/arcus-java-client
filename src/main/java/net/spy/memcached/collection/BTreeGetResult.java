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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import net.spy.memcached.CachedData;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.transcoders.Transcoder;

public class BTreeGetResult<K, V> {
  private final CollectionOperationStatus opStatus;
  private final SortedMap<K, BTreeElement<K, V>> elements;

  public BTreeGetResult(List<BTreeElement<K, CachedData>> elementList,
                        boolean reverse, Transcoder<V> transcoder,
                        CollectionOperationStatus opStatus) {
    this.opStatus = opStatus;

    if (elementList == null) {
      this.elements = null;
      return;
    }
    this.elements = new ByteArrayTreeMap<K, BTreeElement<K, V>>(
            reverse ? Collections.reverseOrder() : null);
    for (BTreeElement<K, CachedData> elem : elementList) {
      elements.put(elem.getBkey(), new BTreeElement<K, V>(elem.getBkey(), elem.getEflag(),
              transcoder.decode(elem.getValue())));
    }
  }

  public Map<K, BTreeElement<K, V>> getElements() {
    return elements;
  }

  public CollectionOperationStatus getCollectionResponse() {
    return opStatus;
  }
}
