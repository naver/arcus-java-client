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
        BTreeUtil.compareByteArraysInLexOrder(from, to) > 0, eFlagFilter, offset, count);
  }

  public byte[] getSubkey() {
    return (byte[]) subkey;
  }

  protected Object decodeSubkey(String subkey) {
    return BTreeUtil.hexStringToByteArrays(subkey.substring(2));
  }
}
