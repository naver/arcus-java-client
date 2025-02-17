/*
 * arcus-java-client : Arcus Java client
 * Copyright 2016 JaM2in Co., Ltd.
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

public class MapGet extends CollectionGet {

  private static final String command = "mop get";

  protected List<String> mkeyList;
  protected byte[] data;
  private String keySeparator;
  private String spaceSeparatedKeys;
  protected byte[] additionalArgs;

  public MapGet(List<String> mkeyList, boolean delete, boolean dropIfEmpty) {
    this.mkeyList = mkeyList;
    this.delete = delete;
    this.dropIfEmpty = dropIfEmpty;
    this.eHeadCount = 2;
    this.eFlagIndex = -1;
  }

  public void setKeySeparator(String keySeparator) {
    this.keySeparator = keySeparator;
  }

  @Override
  public byte[] getAdditionalArgs() {
    return additionalArgs;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }

    if (mkeyList.isEmpty()) {
      additionalArgs = null;
    } else {
      additionalArgs = getSpaceSeparatedMkeys().getBytes();
    }

    StringBuilder b = new StringBuilder();
    if (additionalArgs == null) {
      b.append("0");
    } else {
      b.append(additionalArgs.length);
    }
    b.append(" ").append(mkeyList.size());

    if (delete && dropIfEmpty) {
      b.append(" drop");
    }
    if (delete && !dropIfEmpty) {
      b.append(" delete");
    }

    str = b.toString();
    return str;
  }

  private String getSpaceSeparatedMkeys() {
    if (spaceSeparatedKeys != null) {
      return spaceSeparatedKeys;
    }

    StringBuilder sb = new StringBuilder();
    int numkeys = mkeyList.size();
    for (int i = 0; i < numkeys; i++) {
      sb.append(mkeyList.get(i));
      if ((i + 1) < numkeys) {
        sb.append(keySeparator);
      }
    }
    spaceSeparatedKeys = sb.toString();
    return spaceSeparatedKeys;
  }

  public String getCommand() {
    return command;
  }

  @Override
  public void decodeElemHeader(List<String> tokens) {
    this.subkey = tokens.get(0);
    this.dataLength = Integer.parseInt(tokens.get(1));
  }
}
