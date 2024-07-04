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
import net.spy.memcached.util.BTreeUtil;

public class BTreeGetBulkWithByteTypeBkey<T> extends BTreeGetBulkImpl<T> {

  public BTreeGetBulkWithByteTypeBkey(MemcachedNode node, List<String> keyList,
                                      byte[] from, byte[] to,
                                      ElementFlagFilter eFlagFilter,
                                      int offset, int count) {
    super(node, keyList, BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to),
        eFlagFilter, offset, count);
  }

  private BTreeGetBulkWithByteTypeBkey(MemcachedNode node, List<String> keyList,
                                       String range,
                                       ElementFlagFilter eFlagFilter,
                                       int offset, int count) {
    super(node, keyList, range, eFlagFilter, offset, count);
  }

  public byte[] getBkey() {
    return (byte[]) bkey;
  }

  protected Object decodeBkey(String bkey) {
    return BTreeUtil.hexStringToByteArrays(bkey.substring(2));
  }

  @Override
  public BTreeGetBulk<T> clone(MemcachedNode node, List<String> keyList) {
    return new BTreeGetBulkWithByteTypeBkey<>(node, keyList,
            range, eFlagFilter, offset, count);
  }
}
