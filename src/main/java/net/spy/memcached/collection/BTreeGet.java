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

  private boolean isFirstParsing = true;
  private boolean elementFlagExists = false;


  private BTreeGet(String range,
                   boolean delete, boolean dropIfEmpty,
                   ElementFlagFilter elementFlagFilter) {
    this.headerCount = 2;
    this.range = range;
    this.delete = delete;
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeGet(long bkey,
                  boolean delete, boolean dropIfEmpty,
                  ElementFlagFilter elementFlagFilter) {
    this(String.valueOf(bkey), delete, dropIfEmpty, elementFlagFilter);
  }

  public BTreeGet(byte[] bkey,
                  boolean delete, boolean dropIfEmpty,
                  ElementFlagFilter elementFlagFilter) {
    this(BTreeUtil.toHex(bkey), delete, dropIfEmpty, elementFlagFilter);
  }

  private BTreeGet(String range, int offset, int count,
                   boolean delete, boolean dropIfEmpty,
                   ElementFlagFilter elementFlagFilter) {
    this(range, delete, dropIfEmpty, elementFlagFilter);
    this.offset = offset;
    this.count = count;
  }

  public BTreeGet(long from, long to, int offset, int count,
                  boolean delete, boolean dropIfEmpty,
                  ElementFlagFilter elementFlagFilter) {
    this(from + ".." + to,
        offset, count, delete, dropIfEmpty, elementFlagFilter);
  }

  public BTreeGet(byte[] from, byte[] to, int offset, int count,
                  boolean delete, boolean dropIfEmpty,
                  ElementFlagFilter elementFlagFilter) {
    this(BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to),
        offset, count, delete, dropIfEmpty, elementFlagFilter);
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
    return command;
  }

  public boolean eachRecordParseCompleted() {
    if (elementFlagExists) {
      // isFirstParsing true means item header with eFlag was parsed completely
      return isFirstParsing;
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
    // non-eFlag header has spaceCount 2 and eFlag header has spaceCount 3
    return spaceCount == 2 || spaceCount == 3;
  }

  @Override
  public void decodeItemHeader(String itemHeader) {
    String[] splited = itemHeader.split(" ");

    if (isFirstParsing) {
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
        // need second parsing because of EFlag field
        isFirstParsing = false;
      } else {
        this.dataLength = Integer.parseInt(splited[1]);
      }
    } else {
      this.dataLength = Integer.parseInt(splited[1]);
      // revert true for next B+Tree element parsing
      this.isFirstParsing = true;
    }
  }
}
