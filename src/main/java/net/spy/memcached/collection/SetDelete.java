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

import net.spy.memcached.transcoders.Transcoder;

public class SetDelete<T> extends CollectionDelete {

  private static final String command = "sop delete";

  protected T value;
  protected byte[] additionalArgs;
  protected Transcoder<T> tc;

  public SetDelete(T value, boolean dropIfEmpty, boolean noreply, Transcoder<T> tc) {
    this.value = value;
    this.dropIfEmpty = dropIfEmpty;
    this.noreply = noreply;
    this.tc = tc;
    this.additionalArgs = tc.encode(value).getData();
  }

  public byte[] getAdditionalArgs() {
    return additionalArgs;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }

    StringBuilder b = new StringBuilder();
    b.append(additionalArgs.length);

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
    return command;
  }

}
