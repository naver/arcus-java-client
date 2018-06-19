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
  private String separatedKeys;
  private boolean spaceSeparate;
  protected byte[] additionalArgs;

  public MapGet(List<String> mkeyList, boolean delete, boolean spaceSeparate) {
    this.headerCount = 2;
    this.mkeyList = mkeyList;
    this.delete = delete;
    this.spaceSeparate = spaceSeparate;
    if (mkeyList.size() == 0) {
      this.additionalArgs = null;
    } else {
      this.additionalArgs = getSeparatedMkeys().getBytes();
    }
  }

  public MapGet(List<String> mkeyList, boolean delete, boolean spaceSeparate, boolean dropIfEmpty) {
    this(mkeyList, delete, spaceSeparate);
    this.dropIfEmpty = dropIfEmpty;
  }

  public String getSeparatedMkeys() {
    if (separatedKeys != null) {
      return separatedKeys;
    }

    String separator = null;
    if (spaceSeparate) {
      separator = " ";
    } else {
      separator = ",";
    }

    StringBuilder sb = new StringBuilder();
    int numkeys = mkeyList.size();
    for (int i = 0; i < numkeys; i++) {
      sb.append(mkeyList.get(i));
      if ((i + 1) < numkeys) {
        sb.append(separator);
      }
    }
    separatedKeys = sb.toString();
    return separatedKeys;
  }

  @Override
  public byte[] getAddtionalArgs() {
    return additionalArgs;
  }

  public String stringify() {
    if (str != null) return str;

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

  public String getCommand() {
    return command;
  }

  public void decodeItemHeader(String itemHeader) {
    String[] splited = itemHeader.split(" ");
    this.subkey = splited[0];
    this.dataLength = Integer.parseInt(splited[1]);
  }
}
