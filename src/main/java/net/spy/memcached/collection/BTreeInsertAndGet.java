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
 * Ascii protocol implementation for store and get(trimmed) operations
 * <pre>{@code
 * - bop insert <key> <bkey> [<eflag>] <bytes> [create <attributes>] getrim\r\n<data>\r\n
 * - bop upsert <key> <bkey> [<eflag>] <bytes> [create <attributes>] getrim\r\n<data>\r\n
 * VALUE <flags> <count>\r\n
 * <bkey> [<eflag>] <bytes> <data>\r\n
 * TRIMMED\r\n
 * }</pre>
 *
 * @param <T> the expected class of the value
 */
public class BTreeInsertAndGet<T> extends CollectionGet {

  private final CollectionInsert<T> collection;
  private final boolean updateIfExist;
  private BKeyObject bKey;

  public BTreeInsertAndGet(long bkey, byte[] eFlag, T value, boolean updateIfExist,
                           CollectionAttributes attributesForCreate) {
    if (updateIfExist) {
      this.collection = new BTreeUpsert<T>(value, eFlag, RequestMode.GET_TRIM, attributesForCreate);
    } else {
      this.collection = new BTreeInsert<T>(value, eFlag, RequestMode.GET_TRIM, attributesForCreate);
    }
    this.updateIfExist = updateIfExist;
    this.bKey = new BKeyObject(bkey);
    this.eHeadCount = 2;
    this.eFlagIndex = 1;
  }

  public BTreeInsertAndGet(byte[] bkey, byte[] eFlag, T value, boolean updateIfExist,
                           CollectionAttributes attributesForCreate) {
    if (updateIfExist) {
      this.collection = new BTreeUpsert<T>(value, eFlag, RequestMode.GET_TRIM, attributesForCreate);
    } else {
      this.collection = new BTreeInsert<T>(value, eFlag, RequestMode.GET_TRIM, attributesForCreate);
    }
    this.updateIfExist = updateIfExist;
    this.bKey = new BKeyObject(bkey);
    this.eHeadCount = 2;
    this.eFlagIndex = 1;
  }

  @Override
  public void decodeElemHeader(List<String> tokens) {
    subkey = tokens.get(0);
    if (subkey.startsWith("0x")) {
      bKey =  new BKeyObject(BTreeUtil.hexStringToByteArrays(subkey.substring(2)));
    } else {
      bKey = new BKeyObject(Long.parseLong(subkey));
    }
    if (tokens.size() == 2) {
      dataLength = Integer.parseInt(tokens.get(1));
    } else {
      elementFlag = BTreeUtil.hexStringToByteArrays(tokens.get(1));
      dataLength = Integer.parseInt(tokens.get(2));
    }
  }

  @Override
  public byte[] getAddtionalArgs() {
    return null;
  }

  @Override
  public String stringify() {
    return collection.stringify();
  }

  @Override
  public String getCommand() {
    return collection.getCommand();
  }

  public T getValue() {
    return collection.getValue();
  }

  public String getElementFlagByHex() {
    return collection.getElementFlagByHex();
  }

  public BKeyObject getBkeyObject() {
    return bKey;
  }

  public void setFlags(int flags) {
    collection.setFlags(flags);
  }

  public boolean isUpdateIfExist() {
    return updateIfExist;
  }
}
