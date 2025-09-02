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

public abstract class CollectionInsert<T> {

  protected int flags = 0;
  protected T value;
  protected RequestMode requestMode;
  protected CollectionAttributes attribute;
  protected byte[] elementFlag;
  protected String str;

  protected CollectionInsert() {
  }

  protected CollectionInsert(CollectionType type, T value, byte[] elementFlag,
                          RequestMode requestMode, CollectionAttributes attr) {
    if (attr != null) { /* item creation option */
      CollectionCreate.checkOverflowAction(type, attr.getOverflowAction());
    }
    if (elementFlag != null) {
      if (elementFlag.length < 1 || elementFlag.length > ElementFlagFilter.MAX_EFLAG_LENGTH) {
        throw new IllegalArgumentException("Length of elementFlag must be between 1 and "
                + ElementFlagFilter.MAX_EFLAG_LENGTH + ".");
      }
    }

    this.value = value;
    this.elementFlag = elementFlag;
    this.requestMode = requestMode;
    this.attribute = attr;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }

    StringBuilder b = new StringBuilder();

    if (attribute != null) {
      b.append(CollectionCreate.makeCreateClause(attribute, flags));
    }

    // an optional request mode like noreply, pipe and getrim
    if (requestMode != null) {
      b.append((b.length() <= 0) ? "" : " ").append(requestMode.getAscii());
    }

    str = b.toString();
    return str;
  }

  public byte[] getElementFlag() {
    return elementFlag;
  }

  public String getElementFlagByHex() {
    if (elementFlag == null) {
      return "";
    }
    return BTreeUtil.toHex(elementFlag);
  }

  public void setFlags(int flags) {
    this.flags = flags;
  }

  public T getValue() {
    return value;
  }

  public String toString() {
    return (str != null) ? str : stringify();
  }

  public abstract String getCommand();

}
