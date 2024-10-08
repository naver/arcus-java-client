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

import net.spy.memcached.util.BTreeUtil;

/**
 * Ascii protocol implementation for "bop gbp" (B+Tree get by position)
 *
 * <pre>{@code
 * bop gbp <key> <order> <position or "position range">\r\n
 * VALUE <flags> <count>\r\n
 * <bkey> [<eflag>] <bytes> <data>\r\n
 * END\r\n (CLIENT_ERROR, NOT_FOUND, UNREADABLE, TYPE_MISMATCH, NOT_FOUND_ELEMENT)
 * }</pre>
 */
public class BTreeGetByPosition extends CollectionGet {

  public static final int HEADER_EFLAG_POSITION = 1; // 0-based

  private static final String COMMAND = "bop gbp";

  private final BTreeOrder order;
  private final int posFrom;
  private final int posTo;
  private BKeyObject bkey;

  public BTreeGetByPosition(BTreeOrder order, int pos) {
    this.order = order;
    this.range = String.valueOf(pos);
    this.posFrom = pos;
    this.posTo = pos;
    this.eHeadCount = 2;
    this.eFlagIndex = 1;
  }

  public BTreeGetByPosition(BTreeOrder order, int posFrom, int posTo) {
    this.order = order;
    this.range = posFrom + ".." + posTo;
    this.posFrom = posFrom;
    this.posTo = posTo;
    this.eHeadCount = 2;
    this.eFlagIndex = 1;
  }

  public BTreeOrder getOrder() {
    return order;
  }

  public String getRange() {
    return range;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }
    StringBuilder b = new StringBuilder();
    b.append(order.getAscii());
    b.append(" ");
    b.append(range);

    str = b.toString();
    return str;
  }

  public String getCommand() {
    return COMMAND;
  }

  @Override
  public byte[] getAddtionalArgs() {
    return null;
  }

  /*
   * VALUE <flags> <count>\r\n
   * <bkey> [<eflag>] <bytes> <data>\r\n
   * END\r\n
   */
  @Override
  public void decodeElemHeader(List<String> tokens) {
    subkey = tokens.get(0);
    if (subkey.startsWith("0x")) {
      bkey =  new BKeyObject(BTreeUtil.hexStringToByteArrays(subkey.substring(2)));
    } else {
      bkey = new BKeyObject(Long.parseLong(subkey));
    }
    if (tokens.size() == 2) {
      dataLength = Integer.parseInt(tokens.get(1));
    } else {
      elementFlag = BTreeUtil.hexStringToByteArrays(tokens.get(1));
      dataLength = Integer.parseInt(tokens.get(2));
    }
  }

  public BKeyObject getBkey() {
    return bkey;
  }

  public int getPosFrom() {
    return posFrom;
  }

  public int getPosTo() {
    return posTo;
  }

  public boolean isReversed() {
    return posFrom > posTo;
  }
}
