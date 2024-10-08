/*
 * arcus-java-client : Arcus Java client
 * Copyright 2014 JaM2in Co., Ltd.
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
 * Ascii protocol implementation for "bop pwg" (B+Tree find position with get)
 *
 * <pre>{@code
 * bop pwg <key> <bkey> <order> [<count>]\r\n
 *
 * VALUE <position> <flags> <count> <index>\r\n
 * <bkey> [<eflag>] <bytes> <data>\r\n
 * END\r\n
 * }</pre>
 *
 * Failure Code
 * - NOT_FOUND
 * - NOT_FOUND_ELEMENT
 * - TYPE_MISMATCH
 * - BKEY_MISMATCH
 * - UNREADABLE
 * - CLIENT_ERROR
 * - SERVER_ERROR
 */
public class BTreeFindPositionWithGet extends CollectionGet {
  private static final String COMMAND = "bop pwg";

  private final BKeyObject bkeyObject;
  private final BTreeOrder order;
  private final int count;
  private BKeyObject bkey;

  public BTreeFindPositionWithGet(long longBKey, BTreeOrder order, int count) {
    this.bkeyObject = new BKeyObject(longBKey);
    this.order = order;
    this.count = count;
    this.eHeadCount = 2;
    this.eFlagIndex = 1;
  }

  public BTreeFindPositionWithGet(byte[] byteArrayBKey, BTreeOrder order, int count) {
    this.bkeyObject = new BKeyObject(byteArrayBKey);
    this.order = order;
    this.count = count;
    this.eHeadCount = 2;
    this.eFlagIndex = 1;
  }

  public String stringify() {
    if (str == null) {
      StringBuilder b = new StringBuilder();
      b.append(bkeyObject);
      b.append(" ");
      b.append(order.getAscii());
      if (count > 0) {
        b.append(" ");
        b.append(String.valueOf(count));
      }
      str = b.toString();
    }
    return str;
  }

  public String getCommand() {
    return COMMAND;
  }

  public BKeyObject getBkeyObject() {
    return bkeyObject;
  }

  public BTreeOrder getOrder() {
    return order;
  }

  public int getCount() {
    return count;
  }

  @Override
  public byte[] getAddtionalArgs() {
    return null;
  }

  /*
   * VALUE <position> <flags> <count> <index>\r\n
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
}
