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

public class SetGet extends CollectionGet {

  private static final String command = "sop get";

  protected int count;

  public SetGet(int count, boolean delete, boolean dropIfEmpty) {
    this.count = count;
    this.delete = delete;
    this.dropIfEmpty = dropIfEmpty;
    this.eHeadCount = 1;
    this.eFlagIndex = -1;
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
    b.append(count);
    if (delete && dropIfEmpty) {
      b.append(" drop");
    }
    if (delete && !dropIfEmpty) {
      b.append(" delete");
    }

    str = b.toString();
    return str;
  }

  public String getCommand() {
    return command;
  }

  @Override
  public void decodeElemHeader(List<String> tokens) {
    this.dataLength = Integer.parseInt(tokens.get(0));
  }
}
