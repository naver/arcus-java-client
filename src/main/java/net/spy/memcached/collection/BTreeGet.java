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

import net.spy.memcached.util.BTreeUtil;

public class BTreeGet extends CollectionGet {

  private static final String command = "bop get";

  protected int offset = -1;
  protected int count = -1;

  protected ElementFlagFilter elementFlagFilter;

  public BTreeGet(long bkey, boolean delete) {
    this.headerCount = 2;
    this.range = String.valueOf(bkey);
    this.delete = delete;
  }

  public BTreeGet(long bkey, boolean delete, boolean dropIfEmpty,
                  ElementFlagFilter elementFlagFilter) {
    this(bkey, delete);
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeGet(long from, long to, int offset, int count, boolean delete) {
    this.headerCount = 2;
    this.range = String.valueOf(from) + ".." + String.valueOf(to);
    this.offset = offset;
    this.count = count;
    this.delete = delete;
  }

  public BTreeGet(long from, long to, int offset, int count, boolean delete,
                  boolean dropIfEmpty, ElementFlagFilter elementFlagFilter) {
    this(from, to, offset, count, delete);
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeGet(byte[] from, byte[] to, int offset, int count, boolean delete,
                  boolean dropIfEmpty, ElementFlagFilter elementFlagFilter) {
    this.headerCount = 2;
    this.range = BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to);
    this.offset = offset;
    this.count = count;
    this.delete = delete;
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeGet(long bkey, boolean delete, boolean dropIfEmpty,
                  ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this(bkey, delete);
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = (ElementFlagFilter) elementMultiFlagsFilter;
  }

  public BTreeGet(long from, long to, int offset, int count, boolean delete, boolean dropIfEmpty,
                  ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this(from, to, offset, count, delete);
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = (ElementFlagFilter) elementMultiFlagsFilter;
  }

  public ElementFlagFilter getElementFlagFilter() {
    return elementFlagFilter;
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
      b.append(" ").append(elementFlagFilter.toString());
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
    return command;
  }

  public void resetHeaderCount(int count) {
    this.headerCount = count;
  }

  private int headerParseStep = 1;

  private boolean elementFlagExists = false;

  public boolean eachRecordParseCompleted() {
    if (elementFlagExists) {
      return headerParseStep == 1;
    } else {
      return true;
    }
  }

  @Override
  public byte[] getAddtionalArgs() {
    return null;
  }

  @Override
  public boolean headerReady(int spaceCount) {
    return spaceCount == 2 || spaceCount == 3;
  }

  public void decodeItemHeader(String itemHeader) {
    String[] splited = itemHeader.split(" ");

    if (headerParseStep == 1) {
      // found bkey
      if (splited[0].startsWith("0x")) {
        this.subkey = splited[0].substring(2);
      } else {
        this.subkey = splited[0];
      }

      // found element flag.
      if (splited[1].startsWith("0x")) {
        this.elementFlagExists = true;
        this.elementFlag = BTreeUtil.hexStringToByteArrays(splited[1].substring(2));
      //this.headerCount++;
        headerParseStep = 2;
      } else {
        this.dataLength = Integer.parseInt(splited[1]);
      }
    } else {
      this.headerParseStep = 1;
      this.dataLength = Integer.parseInt(splited[1]);
    }
  }
}
