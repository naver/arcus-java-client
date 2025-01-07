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

public class BTreeGet extends CollectionGet {

  private static final String COMMAND = "bop get";
  protected int offset = -1;
  protected int count = -1;
  protected ElementFlagFilter elementFlagFilter;
  private boolean reverse = false;

  private BTreeGet(String range, ElementFlagFilter elementFlagFilter,
                   boolean delete, boolean dropIfEmpty) {
    this.range = range;
    this.delete = delete;
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
    this.eHeadCount = 2;
    this.eFlagIndex = 1;
  }

  public BTreeGet(long bkey, ElementFlagFilter elementFlagFilter,
                  boolean delete, boolean dropIfEmpty) {
    this(String.valueOf(bkey), elementFlagFilter, delete, dropIfEmpty);
  }

  public BTreeGet(byte[] bkey, ElementFlagFilter elementFlagFilter,
                  boolean delete, boolean dropIfEmpty) {
    this(BTreeUtil.toHex(bkey), elementFlagFilter, delete, dropIfEmpty);
  }

  private BTreeGet(String range, boolean reverse, ElementFlagFilter elementFlagFilter,
                   int offset, int count, boolean delete, boolean dropIfEmpty) {
    this(range, elementFlagFilter, delete, dropIfEmpty);
    this.offset = offset;
    this.count = count;
    this.reverse = reverse;
  }

  public BTreeGet(long from, long to, ElementFlagFilter elementFlagFilter,
                  int offset, int count,
                  boolean delete, boolean dropIfEmpty) {
    this(from + ".." + to, from > to, elementFlagFilter,
        offset, count, delete, dropIfEmpty);
  }

  public BTreeGet(byte[] from, byte[] to, ElementFlagFilter elementFlagFilter,
                  int offset, int count,
                  boolean delete, boolean dropIfEmpty) {
    this(BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to),
            BTreeUtil.compareByteArraysInLexOrder(from, to) > 0,
            elementFlagFilter, offset, count, delete, dropIfEmpty);
  }

  public boolean isReversed() {
    return reverse;
  }

  public String getRange() {
    return range;
  }

  public void setRange(String range) {
    this.range = range;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }

    StringBuilder b = new StringBuilder();
    b.append(range);

    if (elementFlagFilter != null) {
      b.append(" ").append(elementFlagFilter);
    }
    if (offset > 0) {
      b.append(" ").append(offset);
    }
    if (count > 0) {
      b.append(" ").append(count);
    }
    if (delete && dropIfEmpty) {
      b.append(" drop");
    }
    if (delete && !dropIfEmpty) {
      b.append(" delete");
    }

    str = b.toString();
    return str;
  }

  public String getCommand() {
    return COMMAND;
  }

  @Override
  public byte[] getAdditionalArgs() {
    return null;
  }

  @Override
  public void decodeElemHeader(List<String> tokens) {
    subkey = tokens.get(0);
    if (tokens.size() == 2) {
      dataLength = Integer.parseInt(tokens.get(1));
    } else {
      elementFlag = BTreeUtil.hexStringToByteArrays(tokens.get(1));
      dataLength = Integer.parseInt(tokens.get(2));
    }
  }
}
