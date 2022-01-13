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

public class BTreeCount extends CollectionCount {

  private static final String command = "bop count";

  protected final String range;

  protected final ElementFlagFilter elementFlagFilter;

  public BTreeCount(long from, long to, ElementFlagFilter elementFlagFilter) {
    this.range = BTreeUtil.toULong(from) + ".." + BTreeUtil.toULong(to);
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeCount(byte[] from, byte[] to, ElementFlagFilter elementFlagFilter) {
    this.range = BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to);
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeCount(long from, long to, ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this.range = BTreeUtil.toULong(from) + ".." + BTreeUtil.toULong(to);
    this.elementFlagFilter = (ElementFlagFilter) elementMultiFlagsFilter;
  }

  public BTreeCount(byte[] from, byte[] to, ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this.range = BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to);
    this.elementFlagFilter = (ElementFlagFilter) elementMultiFlagsFilter;
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

    str = b.toString();
    return str;
  }

  public String getCommand() {
    return command;
  }
}
