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

/**
 * Ascii protocol implementation for store and get(trimmed) operations
 * <pre>{@code
 * - bop insert <key> <bkey> [<eflag>] <bytes> [create <attributes>] getrim\r\n<data>\r\n
 * - bop upsert <key> <bkey> [<eflag>] <bytes> [create <attributes>] getrim\r\n<data>\r\n
 * VALUE <flags> <count>\r\n
 * <bkey> [<eflag>] <bytes> <data>\r\n
 * TRIMMED\r\n
 * }</pre>
 *
 * @param <T> the expected class of the value
 */
public class BTreeInsertAndGet<T> extends BTreeInsert<T> {

  // FIXME please refactor this :-( subclass-ing needed
  public enum Command {
    INSERT("bop insert"),
    UPSERT("bop upsert");

    private final String command;

    Command(String command) {
      this.command = command;
    }

    public String getCommand() {
      return command;
    }
  }

  public static final int HEADER_EFLAG_POSITION = 1; // 0-based

  private Command cmd;
  private BKeyObject bkeyObject;
  private int bytes;

  public BTreeInsertAndGet(Command cmd, long bkey, byte[] eFlag, T value,
                           CollectionAttributes attributesForCreate) {
    super(value, eFlag, attributesForCreate != null, RequestMode.GET_TRIM,
            attributesForCreate);
    this.cmd = cmd;
    this.bkeyObject = new BKeyObject(bkey);
  }

  public BTreeInsertAndGet(Command cmd, byte[] bkey, byte[] eFlag, T value,
                           CollectionAttributes attributesForCreate) {
    super(value, eFlag, attributesForCreate != null, RequestMode.GET_TRIM,
            attributesForCreate);
    this.cmd = cmd;
    this.bkeyObject = new BKeyObject(bkey);
  }

  public BKeyObject getBkeyObject() {
    return bkeyObject;
  }

  public boolean headerReady(int spaceCount) {
    return spaceCount == 2;
  }

  private static final int BKEY = 0;
  private static final int EFLAG_OR_BYTES = 1;
  private static final int BYTES = 2;

  public void decodeItemHeader(String itemHeader) {
    String[] splited = itemHeader.split(" ");
    boolean hasEFlag = false;

    // <bkey>
    if (splited[BKEY].startsWith("0x")) {
      this.bkeyObject = new BKeyObject(splited[0].substring(2));
    } else {
      this.bkeyObject = new BKeyObject(Long.parseLong(splited[0]));
    }

    // <eflag> or <bytes>
    if (splited[EFLAG_OR_BYTES].startsWith("0x")) {
      // <eflag>
      hasEFlag = true;
      this.elementFlag = BTreeUtil
              .hexStringToByteArrays(splited[EFLAG_OR_BYTES].substring(2));
    } else {
      this.bytes = Integer.parseInt(splited[EFLAG_OR_BYTES]);
    }

    // <bytes>
    if (hasEFlag) {
      this.bytes = Integer.parseInt(splited[BYTES]);
    }
  }

  public int getBytes() {
    return bytes;
  }

  public Command getCmd() {
    return cmd;
  }

  @Override
  public String getCommand() {
    return cmd.getCommand();
  }

}
