/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
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

public class BTreeSMGetWithByteTypeBkeyOld<T> extends BTreeSMGetImpl<T> {

  public BTreeSMGetWithByteTypeBkeyOld(MemcachedNode node, List<String> keyList,
                                       byte[] from, byte[] to,
                                       ElementFlagFilter eFlagFilter,
                                       int offset, int count) {
    super(node, keyList, BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to),
        eFlagFilter, offset, count);
  }

  private BTreeSMGetWithByteTypeBkeyOld(MemcachedNode node, List<String> keyList,
                                        String range,
                                        ElementFlagFilter eFlagFilter,
                                        int offset, int count) {
    super(node, keyList, range, eFlagFilter, offset, count);
  }

  @Override
  public byte[] getSubkey() {
    return (byte[]) subkey;
  }

  @Override
  protected Object decodeSubkey(String subkey) {
    return BTreeUtil.hexStringToByteArrays(subkey.substring(2));
  }

  @Override
  public BTreeSMGet<T> clone(MemcachedNode node, List<String> keyList) {
    return new BTreeSMGetWithByteTypeBkeyOld<>(node, keyList,
            range, eFlagFilter, offset, count);
  }
}
