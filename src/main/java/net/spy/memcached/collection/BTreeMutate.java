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

import net.spy.memcached.ops.Mutator;
import net.spy.memcached.util.BTreeUtil;

public class BTreeMutate extends CollectionMutate {

  private final String command;

  protected final Mutator m;

  protected final int by;

  protected long initial = -1;

  protected byte[] elementFlag;

  public BTreeMutate(Mutator m, int by) {
    if (by <= 0) {
      throw new IllegalArgumentException("by must be positive value.");
    }

    if (Mutator.incr == m) {
      command = "bop incr";
    } else {
      command = "bop decr";
    }

    this.m = m;
    this.by = by;
  }

  public BTreeMutate(Mutator m, int by, long initial, byte[] eFlag) {
    if (by <= 0) {
      throw new IllegalArgumentException("by must be positive value.");
    }
    if (initial < 0) {
      throw new IllegalArgumentException("initial must be 0 or positive value.");
    }
    if (eFlag != null && (eFlag.length < 1 || eFlag.length > ElementFlagFilter.MAX_EFLAG_LENGTH)) {
      throw new IllegalArgumentException(
              "length of eFlag must be between 1 and " + ElementFlagFilter.MAX_EFLAG_LENGTH + ".");
    }

    if (Mutator.incr == m) {
      command = "bop incr";
    } else {
      command = "bop decr";
    }

    this.m = m;
    this.by = by;
    this.initial = initial;
    this.elementFlag = eFlag;
  }

  public String stringify() {
    if (str != null)
      return str;

    StringBuilder b = new StringBuilder();
    b.append(by);

    if (initial >= 0) b.append(" ").append(initial);
    if (elementFlag != null) b.append(" ").append(getElementFlagByHex());

    str = b.toString();
    return str;
  }

  public String getCommand() {
    return command;
  }

  public Mutator getMutator() {
    return this.m;
  }

  public String getElementFlagByHex() {
    return BTreeUtil.toHex(elementFlag);
  }
}
