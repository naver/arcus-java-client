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

public class ListDelete extends CollectionDelete {

  private static final String COMMAND = "lop delete";

  public ListDelete(int index, boolean dropIfEmpty, boolean noreply) {
    this.range = String.valueOf(index);
    this.dropIfEmpty = dropIfEmpty;
    this.noreply = noreply;
  }

  public ListDelete(int from, int to, boolean dropIfEmpty, boolean noreply) {
    this.range = from + ".." + to;
    this.dropIfEmpty = dropIfEmpty;
    this.noreply = noreply;
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
