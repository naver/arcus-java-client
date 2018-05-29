/*
 * arcus-java-client : Arcus Java client
 * Copyright 2014 JaM2in Co., Ltd.
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

/**
 * Ascii protocol implementation for "bop pwg" (B+Tree find position with get)
 *
 * bop pwg <key> <bkey> <order> [<count>]\r\n
 *
 * VALUE <position> <flags> <count> <index>\r\n
 * <bkey> [<eflag>] <bytes> <data>\r\n
 * END\r\n
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

  public static final int HEADER_EFLAG_POSITION = 1; // 0-based

  private static final String command = "bop pwg";

  private final BKeyObject bkeyObject;
  private final BTreeOrder order;
  private final int count;
  //private String str;

  private BKeyObject bkey;
  private byte[] eflag;
  private int bytes;

  public BTreeFindPositionWithGet(long longBKey, BTreeOrder order, int count) {
    this.bkeyObject = new BKeyObject(longBKey);
    this.order = order;
    this.count = count;
  }

  public BTreeFindPositionWithGet(byte[] byteArrayBKey, BTreeOrder order, int count) {
    this.bkeyObject = new BKeyObject(byteArrayBKey);
    this.order = order;
    this.count = count;
  }

  public String stringify() {
    if (str == null) {
      StringBuilder b = new StringBuilder();
      b.append(bkeyObject.getBKeyAsString());
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
    return command;
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
  public boolean headerReady(int spaceCount) {
    return spaceCount == 2;
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
  public void decodeItemHeader(String itemHeader) {
    String[] splited = itemHeader.split(" ");

    // <bkey>
    if (splited[0].startsWith("0x")) {
      this.bkey = new BKeyObject(splited[0].substring(2));
    } else {
      this.bkey = new BKeyObject(Long.parseLong(splited[0]));
    }
    if (splited[1].startsWith("0x")) {
      // <eflag> <bytes>
      this.eflag = BTreeUtil.hexStringToByteArrays(splited[1].substring(2));
      this.bytes = Integer.parseInt(splited[2]);
    } else {
      // <bytes> only
      this.bytes = Integer.parseInt(splited[1]);
    }

    this.dataLength = bytes;
  }

  public BKeyObject getBkey() {
    return bkey;
  }

  public byte[] getEflag() {
    return eflag;
  }

  public int getBytes() {
    return bytes;
  }
}
