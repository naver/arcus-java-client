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

public class BTreeGetBulkWithLongTypeBkey<T> extends BTreeGetBulkImpl<T> {

  public BTreeGetBulkWithLongTypeBkey(MemcachedNode node, List<String> keyList,
                                      long from, long to,
                                      ElementFlagFilter eFlagFilter,
                                      int offset, int count) {
    super(node, keyList, from + ".." + to, eFlagFilter, offset, count);
  }

  private BTreeGetBulkWithLongTypeBkey(MemcachedNode node, List<String> keyList,
                                       String range,
                                       ElementFlagFilter eFlagFilter,
                                       int offset, int count) {
    super(node, keyList, range, eFlagFilter, offset, count);
  }

  public Long getBkey() {
    return (Long) bkey;
  }

  protected Object decodeBkey(String bkey) {
    return Long.parseLong(bkey);
  }

  @Override
  public BTreeGetBulk<T> clone(MemcachedNode node, List<String> keyList) {
    return new BTreeGetBulkWithLongTypeBkey<>(node, keyList,
            range, eFlagFilter, offset, count);
  }
}
