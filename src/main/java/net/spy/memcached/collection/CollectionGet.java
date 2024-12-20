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

public abstract class CollectionGet {

  protected boolean delete = false;
  protected boolean dropIfEmpty = false;

  protected String range;
  protected String str;
  protected String subkey;
  protected int dataLength;

  protected byte[] elementFlag;
  protected int eHeadCount;
  protected int eFlagIndex;

  public boolean isDelete() {
    return delete;
  }

  public void setDelete(boolean delete) {
    this.delete = delete;
  }

  public boolean isDropIfEmpty() {
    return dropIfEmpty;
  }

  public String getSubkey() {
    return subkey;
  }

  public int getDataLength() {
    return dataLength;
  }

  public byte[] getElementFlag() {
    return elementFlag;
  }

  public int getEFlagIndex() {
    return eFlagIndex;
  }

  public int getEHeadCount() {
    return eHeadCount;
  }

  public abstract byte[] getAdditionalArgs();

  public abstract String stringify();

  public abstract String getCommand();

  public abstract void decodeElemHeader(List<String> tokens);
}
