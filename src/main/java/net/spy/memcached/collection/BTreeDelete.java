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

public class BTreeDelete extends CollectionDelete {

  private static final String COMMAND = "bop delete";
  protected int count = -1;

  protected ElementFlagFilter elementFlagFilter;

  public BTreeDelete(long bkey, boolean noreply) {
    this.range = String.valueOf(bkey);
    this.noreply = noreply;
  }

  public BTreeDelete(long bkey, boolean noreply, boolean dropIfEmpty,
                     ElementFlagFilter elementFlagFilter) {
    this(bkey, noreply);
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeDelete(long from, long to, boolean noreply) {
    this.range = from + ".." + to;
    this.noreply = noreply;
  }

  public BTreeDelete(long from, long to, int count, boolean noreply) {
    this.range = from + ".." + to;
    this.count = count;
    this.noreply = noreply;
  }

  public BTreeDelete(long from, long to, int count, boolean noreply, boolean dropIfEmpty,
                     ElementFlagFilter elementFlagFilter) {
    this(from, to, count, noreply);
    this.dropIfEmpty = dropIfEmpty;
    this.noreply = noreply;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeDelete(byte[] bkey, boolean noreply, boolean dropIfEmpty,
                     ElementFlagFilter elementFlagFilter) {
    this.range = BTreeUtil.toHex(bkey);
    this.noreply = noreply;
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeDelete(byte[] from, byte[] to, int count, boolean noreply, boolean dropIfEmpty,
                     ElementFlagFilter elementFlagFilter) {
    this.range = BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to);
    this.count = count;
    this.noreply = noreply;
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementFlagFilter;
  }

  public BTreeDelete(long bkey, boolean noreply, boolean dropIfEmpty,
                     ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this(bkey, noreply);
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementMultiFlagsFilter;
  }

  public BTreeDelete(long from, long to, int count, boolean noreply, boolean dropIfEmpty,
                     ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this(from, to, count, noreply);
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementMultiFlagsFilter;
  }

  public BTreeDelete(byte[] bkey, boolean noreply, boolean dropIfEmpty,
                     ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this.range = BTreeUtil.toHex(bkey);
    this.noreply = noreply;
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementMultiFlagsFilter;
  }

  public BTreeDelete(byte[] from, byte[] to, int count, boolean noreply, boolean dropIfEmpty,
                     ElementMultiFlagsFilter elementMultiFlagsFilter) {
    this.range = BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to);
    this.count = count;
    this.noreply = noreply;
    this.dropIfEmpty = dropIfEmpty;
    this.elementFlagFilter = elementMultiFlagsFilter;
  }

  @Override
  public byte[] getAdditionalArgs() {
    return null;
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

    if (count >= 0) {
      b.append(" ").append(count);
    }

    if (dropIfEmpty) {
      b.append(" drop");
    }

    if (noreply) {
      b.append(" noreply");
    }

    str = b.toString();
    return str;
  }

  public String getCommand() {
    return COMMAND;
  }

}
