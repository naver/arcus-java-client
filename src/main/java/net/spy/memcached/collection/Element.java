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

import net.spy.memcached.util.BTreeUtil;

/**
 * Collection element
 *
 * @param <T> the expected class of the value
 */
public class Element<T> {
  private BKeyObject bKeyObject;
  private final T value;
  private final byte[] eflag;
  private final ElementFlagUpdate elementFlagUpdate;

  /**
   * Create an element
   *
   * @param bkey  key of element
   * @param value value of element
   * @param eflag flag of element (minimun length is 1. maximum length is 31)
   */
  public Element(byte[] bkey, T value, byte[] eflag) {
    this.bKeyObject = new BKeyObject(bkey);
    this.value = value;
    this.eflag = eflag;
    this.elementFlagUpdate = null;
  }

  public Element(long bkey, T value, byte[] eflag) {
    this.bKeyObject = new BKeyObject(bkey);
    this.value = value;
    this.eflag = eflag;
    this.elementFlagUpdate = null;
  }

  public Element(byte[] bkey, T value, ElementFlagUpdate elementFlagUpdate) {
    this.bKeyObject = new BKeyObject(bkey);
    this.value = value;
    this.eflag = null;
    this.elementFlagUpdate = elementFlagUpdate;
  }

  public Element(long bkey, T value, ElementFlagUpdate elementFlagUpdate) {
    this.bKeyObject = new BKeyObject(bkey);
    this.value = value;
    this.eflag = null;
    this.elementFlagUpdate = elementFlagUpdate;
  }

  /**
   * get value of element flag by hex.
   *
   * @return element flag by hex (e.g. 0x01)
   */
  public String getStringEFlag() {
    // convert to hex based on its real byte array
    if (eflag == null) {
      return "";
    }

    return BTreeUtil.toHex(eflag);
  }

  /**
   * get bkey
   *
   * @return bkey by byte[]
   */
  public byte[] getByteArrayBkey() {
    return bKeyObject.getByteArrayBKeyRaw();
  }

  /**
   * get bkey
   *
   * @return bkey
   */
  public long getLongBkey() {
    return bKeyObject.getLongBKey();
  }

  /**
   * if byte bkey exist return hex string and if not return type of string long bkey
   * @return type of hex byte bkey or type of String Long bkey
   */
  public String getStringBkey() {
    return bKeyObject.toString();
  }

  /**
   * get value
   *
   * @return value
   */
  public T getValue() {
    return value;
  }

  /**
   * get flag
   *
   * @return element flag
   */
  public byte[] getEFlag() {
    return eflag;
  }

  public ElementFlagUpdate getElementFlagUpdate() {
    return elementFlagUpdate;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ \"");
    sb.append(getStringBkey());
    sb.append("\" : { ");

    sb.append(" \"eflag\" : \"").append(getStringEFlag()).append("\"");
    sb.append(",");
    sb.append(" \"value\" : \"").append(value.toString()).append("\"");
    sb.append(" }");

    return sb.toString();
  }

}
