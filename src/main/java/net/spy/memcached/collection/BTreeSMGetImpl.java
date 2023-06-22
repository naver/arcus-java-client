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

import net.spy.memcached.KeyUtil;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.util.BTreeUtil;

public abstract class BTreeSMGetImpl<T> implements BTreeSMGet<T> {
  private static final String command = "bop smget";

  private final MemcachedNode node;
  private String str;

  private String keySeparator;
  private String spaceSeparatedKeys;

  private final List<String> keyList;
  protected final String range;
  protected final ElementFlagFilter eFlagFilter;
  protected final int offset;
  protected final int count;
  protected final SMGetMode smgetMode;

  private String key;
  private int flags;
  protected Object subkey;
  private int dataLength;
  private byte[] eflag = null;

  public BTreeSMGetImpl(MemcachedNode node, List<String> keyList,
                        String range,
                        ElementFlagFilter eFlagFilter,
                        int count, SMGetMode smgetMode) {
    this(node, keyList, range, eFlagFilter, -1, count, smgetMode);
  }

  public BTreeSMGetImpl(MemcachedNode node, List<String> keyList,
                        String range,
                        ElementFlagFilter eFlagFilter,
                        int offset, int count) {
    this(node, keyList, range, eFlagFilter, offset, count, null);
  }

  private BTreeSMGetImpl(MemcachedNode node, List<String> keyList,
                         String range,
                         ElementFlagFilter eFlagFilter,
                         int offset, int count,
                         SMGetMode smgetMode) {
    this.node = node;
    this.keyList = keyList;
    this.range = range;
    this.eFlagFilter = eFlagFilter;
    this.offset = offset;
    this.count = count;
    this.smgetMode = smgetMode;
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
    StringBuilder b = new StringBuilder();
    b.append(KeyUtil.getKeyBytes(getSpaceSeparatedKeys()).length);
    b.append(" ").append(keyList.size());
    b.append(" ").append(range);
    if (eFlagFilter != null) {
      b.append(" ").append(eFlagFilter);
    }
    if (smgetMode != null) { // new smget
      b.append(" ").append(count);
      b.append(" ").append(smgetMode.getMode());
    } else { // old smget
      if (offset > 0) {
        b.append(" ").append(offset);
      }
      b.append(" ").append(count);
    }
    str = b.toString();
    return str;
  }

  public String getCommand() {
    return command;
  }

  public boolean headerReady(int spaceCount) {
    return headerCount == spaceCount;
  }

  public String getKey() {
    return key;
  }

  public int getFlags() {
    return flags;
  }

  public int getDataLength() {
    return dataLength;
  }

  public byte[] getEflag() {
    return eflag;
  }

  public void decodeItemHeader(String itemHeader) {
    String[] splited = itemHeader.split(" ");
    // <key> <flags> <bkey> [<eflag>] <bytes>

    this.key = splited[0];
    this.flags = Integer.parseInt(splited[1]);
    this.subkey = decodeSubkey(splited[2]);

    if (splited.length == 4) {
      // <key> <flags> <bkey> <bytes>
      this.eflag = null;
      this.dataLength = Integer.parseInt(splited[3]);
    } else if (splited.length == 5) {
      // <key> <flags> <bkey> <eflag> <bytes>
      this.eflag = BTreeUtil.hexStringToByteArrays(splited[3].substring(2));
      this.dataLength = Integer.parseInt(splited[4]);
    }
  }

  protected abstract Object decodeSubkey(String subkey);
}
