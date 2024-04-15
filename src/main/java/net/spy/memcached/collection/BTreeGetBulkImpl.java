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

import net.spy.memcached.KeyUtil;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.util.BTreeUtil;

public abstract class BTreeGetBulkImpl<T> implements BTreeGetBulk<T> {

  private static final String command = "bop mget";

  private final MemcachedNode node;
  private String str;

  private String keySeparator;
  private String spaceSeparatedKeys;

  private final List<String> keyList;
  protected final String range;
  protected final ElementFlagFilter eFlagFilter;
  protected final int offset;
  protected final int count;

  protected Object subkey;
  private int dataLength;
  private byte[] eflag = null;

  protected BTreeGetBulkImpl(MemcachedNode node, List<String> keyList,
                             String range, ElementFlagFilter eFlagFilter,
                             int offset, int count) {
    this.node = node;
    this.keyList = keyList;
    this.range = range;
    this.eFlagFilter = eFlagFilter;
    this.offset = offset;
    this.count = count;
  }

  public void setKeySeparator(String keySeparator) {
    this.keySeparator = keySeparator;
  }

  public String getSpaceSeparatedKeys() {
    if (spaceSeparatedKeys != null) {
      return spaceSeparatedKeys;
    }

    StringBuilder sb = new StringBuilder();
    int numkeys = keyList.size();
    for (int i = 0; i < numkeys; i++) {
      sb.append(keyList.get(i));
      if ((i + 1) < numkeys) {
        sb.append(keySeparator);
      }
    }
    spaceSeparatedKeys = sb.toString();
    return spaceSeparatedKeys;
  }

  public MemcachedNode getMemcachedNode() {
    return node;
  }

  public List<String> getKeyList() {
    return keyList;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }

    /*
     *
     * bop mget <lenkeys> <numkeys> <bkey or "bkey range"> [<eflag_filter>]
     * [<offset>] <count>\r\n<"comma separated keys">\r\n
     * <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
     *
     */

    StringBuilder b = new StringBuilder();

    b.append(KeyUtil.getKeyBytes(getSpaceSeparatedKeys()).length);
    b.append(" ").append(keyList.size());
    b.append(" ").append(range);

    if (eFlagFilter != null) {
      b.append(" ").append(eFlagFilter.toString());
    }

    if (offset > 0) {
      b.append(" ").append(offset);
    }

    b.append(" ").append(count);

    str = b.toString();
    return str;
  }

  public void decodeItemHeader(String[] splited) {
    if (splited.length == 3) {
      // ELEMENT <bkey> <bytes>
      this.subkey = decodeSubkey(splited[1]);
      this.dataLength = Integer.parseInt(splited[2]);
      this.eflag = null;
    } else if (splited.length == 4) {
      // ELEMENT <bkey> <eflag> <bytes>
      this.subkey = decodeSubkey(splited[1]);
      this.eflag = BTreeUtil.hexStringToByteArrays(splited[2].substring(2));
      this.dataLength = Integer.parseInt(splited[3]);
    }
  }

  public String getCommand() {
    return command;
  }

  public boolean headerReady(int spaceCount) {
    return spaceCount == BTreeGetBulk.headerCount;
  }

  public int getDataLength() {
    return dataLength;
  }

  public byte[] getEFlag() {
    return eflag;
  }

  protected abstract Object decodeSubkey(String subkey);
}
