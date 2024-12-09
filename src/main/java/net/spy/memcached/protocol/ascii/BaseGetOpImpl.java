/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
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
package net.spy.memcached.protocol.ascii;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.StatusCode;

/**
 * Base class for get and gets handlers.
 */
abstract class BaseGetOpImpl extends OperationImpl {

  private static final OperationStatus END =
          new OperationStatus(true, "END", StatusCode.SUCCESS);
  private static final String RN_STRING = "\r\n";
  private final String cmd;
  private final Collection<String> keys;
  private String currentKey = null;
  private long casValue = 0;
  private int currentFlags = 0;
  private byte[] data = null;
  private int readOffset = 0;
  private byte lookingFor = '\0';
  /* ENABLE_MIGRATION if */
  private String notMyKeyLine = null;
  /* ENABLE_MIGRATION end */

  public BaseGetOpImpl(String c,
                       OperationCallback cb, Collection<String> k) {
    super(cb);
    cmd = c;
    keys = k;
    setOperationType(OperationType.READ);
  }

  /**
   * Get the keys this GetOperation is looking for.
   */
  public final Collection<String> getKeys() {
    return keys;
  }

  @Override
  public final void handleLine(String line) {
    /*
      VALUE <key> <flags> <bytes> [<cas unique>]\r\n
      <data block>\r\n
      ...
      END\r\n
    */
    if (line.equals("END")) {
      getLogger().debug("Get complete!");
      /* ENABLE_MIGRATION if */
      notMyKeyLine = null;
      if (needRedirect()) {
        transitionState(OperationState.REDIRECT);
        return;
      }
      /* ENABLE_MIGRATION end */
      getCallback().receivedStatus(END);
      transitionState(OperationState.COMPLETE);
      data = null;
    } else if (line.startsWith("VALUE ")) {
      getLogger().debug("Got line %s", line);
      String[] stuff = line.split(" ");
      assert stuff[0].equals("VALUE");
      currentKey = stuff[1];
      currentFlags = Integer.parseInt(stuff[2]);
      data = new byte[Integer.parseInt(stuff[3])];
      if (stuff.length > 4) {
        casValue = Long.parseLong(stuff[4]);
      }
      readOffset = 0;
      getLogger().debug("Set read type to data");
      setReadType(OperationReadType.DATA);
    /* ENABLE_MIGRATION if */
    } else if (hasNotMyKey(line)) {
      notMyKeyLine = line;
    } else if (notMyKeyLine != null) {
      addRedirectMultiKeyOperation(notMyKeyLine, line.trim());
    /* ENABLE_MIGRATION end */
    } else {
      assert false : "Unknown line type: " + line;
    }
  }

  @Override
  public final void handleRead(ByteBuffer bb) {
    assert currentKey != null;
    assert data != null;
    // This will be the case, because we'll clear them when it's not.
    assert readOffset <= data.length : "readOffset is " + readOffset
        + " data.length is " + data.length;

    getLogger().debug("readOffset: %d, length: %d", readOffset, data.length);
    // If we're not looking for termination, we're still looking for data
    if (lookingFor == '\0') {
      int toRead = data.length - readOffset;
      int available = bb.remaining();
      toRead = Math.min(toRead, available);
      getLogger().debug("Reading %d bytes", toRead);
      bb.get(data, readOffset, toRead);
      readOffset += toRead;
    }
    // Transition us into a ``looking for \r\n'' kind of state if we've
    // read enough and are still in a data state.
    if (readOffset == data.length && lookingFor == '\0') {
      // The callback is most likely a get callback.  If it's not, then
      // it's a gets callback.
      try {
        GetOperation.Callback gcb = (GetOperation.Callback) getCallback();
        gcb.gotData(currentKey, currentFlags, data);
      } catch (ClassCastException e) {
        GetsOperation.Callback gcb = (GetsOperation.Callback) getCallback();
        gcb.gotData(currentKey, currentFlags, casValue, data);
      }
      lookingFor = '\r';
    }
    // If we're looking for an ending byte, let's go find it.
    if (lookingFor != '\0' && bb.hasRemaining()) {
      do {
        byte tmp = bb.get();
        assert tmp == lookingFor : "Expecting " + lookingFor + ", got "
                + (char) tmp;
        switch (lookingFor) {
          case '\r':
            lookingFor = '\n';
            break;
          case '\n':
            lookingFor = '\0';
            break;
          default:
            assert false : "Looking for unexpected char: " + (char) lookingFor;
        }
      } while (lookingFor != '\0' && bb.hasRemaining());
      // Completed the read, reset stuff.
      if (lookingFor == '\0') {
        currentKey = null;
        data = null;
        readOffset = 0;
        currentFlags = 0;
        getLogger().debug("Setting read type back to line.");
        setReadType(OperationReadType.LINE);
      }
    }
  }

  @Override
  public final void initialize() {
    int size;
    StringBuilder commandBuilder = new StringBuilder();
    byte[] commandLine;
    ByteBuffer bb;

    String keysString = generateKeysString();

    if (cmd.equals("get") || cmd.equals("gets")) {
      // syntax: get <keys...>\r\n
      commandBuilder.append(cmd);
      commandBuilder.append(' ');
      commandBuilder.append(keysString);
      commandBuilder.append(RN_STRING);
    } else {
      assert (cmd.equals("mget") || cmd.equals("mgets"))
          : "Unknown Command " + cmd;
      // syntax: mget <lenKeys> <numkeys>\r\n<keys>\r\n
      int lenKeys = keysString.getBytes().length;
      int numKeys = keys.size();
      commandBuilder.append(cmd);
      commandBuilder.append(' ');
      commandBuilder.append(lenKeys);
      commandBuilder.append(' ');
      commandBuilder.append(numKeys);
      commandBuilder.append(RN_STRING);
      commandBuilder.append(keysString);
      commandBuilder.append(RN_STRING);
    }

    commandLine = commandBuilder.toString().getBytes();
    size = commandLine.length;

    bb = ByteBuffer.allocate(size);
    bb.put(commandLine);
    ((Buffer) bb).flip();
    setBuffer(bb);
  }

  private String generateKeysString() {
    StringBuilder keyString = new StringBuilder();
    Iterator<String> iterator = keys.iterator();

    // make keys line
    while (true) {
      keyString.append(iterator.next());
      if (iterator.hasNext()) {
        keyString.append(' ');
      } else {
        break;
      }
    }

    return keyString.toString();
  }

  @Override
  protected final void wasCancelled() {
    getCallback().receivedStatus(CANCELLED);
  }

  @Override
  public boolean isBulkOperation() {
    return keys.size() > 1;
  }

  @Override
  public boolean isPipeOperation() {
    return false;
  }

  @Override
  public boolean isIdempotentOperation() {
    return true;
  }

}
