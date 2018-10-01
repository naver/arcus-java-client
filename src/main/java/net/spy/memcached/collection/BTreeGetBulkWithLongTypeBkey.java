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

import net.spy.memcached.util.BTreeUtil;

public class BTreeGetBulkWithLongTypeBkey<T> extends BTreeGetBulkImpl<T> {

  public BTreeGetBulkWithLongTypeBkey(List<String> keyList, long from, long to,
                                      ElementFlagFilter eFlagFilter, int offset, int count) {
    super(keyList, from, to, eFlagFilter, offset, count);
  }

  /* ENABLE_MIGRATION if */
  public BTreeGetBulkWithLongTypeBkey(List<String> keyList, String range,
                                      ElementFlagFilter eFlagFilter, int offset, int count, boolean reverse) {
    super(keyList, range, eFlagFilter, offset, count, reverse);
  }
  /* ENABLE_MIGRATION end */

  public Long getSubkey() {
    return (Long) subkey;
  }

  public void decodeItemHeader(String itemHeader) {
    String[] splited = itemHeader.split(" ");

    if (splited.length == 3) {
      // ELEMENT <bkey> <bytes>
      this.subkey = Long.parseLong(splited[1]);
      this.dataLength = Integer.parseInt(splited[2]);
      this.eflag = null;
    } else if (splited.length == 4) {
      // ELEMENT <bkey> <eflag> <bytes>
      this.subkey = Long.parseLong(splited[1]);
      this.eflag = BTreeUtil.hexStringToByteArrays(splited[2].substring(2));
      this.dataLength = Integer.parseInt(splited[3]);
    }
  }

  @Override
  public void decodeKeyHeader(String keyHeader) {
    String[] splited = keyHeader.split(" ");
    this.key = splited[1];
    if (splited.length == 5) {
      this.flag = Integer.valueOf(splited[3]);
    }
  }
}