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

import java.util.List;

import net.spy.memcached.MemcachedNode;

public class BTreeSMGetWithLongTypeBkey<T> extends BTreeSMGetImpl<T> {

  public BTreeSMGetWithLongTypeBkey(MemcachedNode node, List<String> keyList,
                                    long from, long to,
                                    ElementFlagFilter eFlagFilter,
                                    int count, SMGetMode smgetMode) {
    super(node, keyList, from + ".." + to, eFlagFilter, count, smgetMode);
  }

  private BTreeSMGetWithLongTypeBkey(MemcachedNode node, List<String> keyList,
                                     String range,
                                     ElementFlagFilter eFlagFilter,
                                     int count, SMGetMode smgetMode) {
    super(node, keyList, range, eFlagFilter, count, smgetMode);
  }

  @Override
  public Long getSubkey() {
    return (Long) subkey;
  }

  @Override
  protected Object decodeSubkey(String subkey) {
    return Long.parseLong(subkey);
  }

  @Override
  public BTreeSMGet<T> clone(MemcachedNode node, List<String> keyList) {
    return new BTreeSMGetWithLongTypeBkey<T>(node, keyList,
        range, eFlagFilter, count, smgetMode);
  }
}
