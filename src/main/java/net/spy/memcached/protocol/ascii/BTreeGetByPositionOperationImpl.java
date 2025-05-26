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

import java.io.ByteArrayOutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.BTreeGetByPosition;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeGetByPositionOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

public final class BTreeGetByPositionOperationImpl extends OperationImpl implements
        BTreeGetByPositionOperation {

  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
          false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);

  private final String key;
  private final BTreeGetByPosition get;

  private int flags = 0;
  private int count = 0;
  private int pos = 0;
  private int posDiff = 0;
  private byte[] data = null;
  private int readOffset = 0;
  private byte lookingFor = '\0';
  private final List<String> tokens = new ArrayList<>();
  private int eHeadCount;
  private int eFlagIndex;

  public BTreeGetByPositionOperationImpl(String key, BTreeGetByPosition get,
                                         OperationCallback cb) {
    super(cb);
    this.key = key;
    this.get = get;
    this.eHeadCount = get.getEHeadCount();
    this.eFlagIndex = get.getEFlagIndex();
    setAPIType(APIType.BOP_GBP);
    setOperationType(OperationType.READ);
  }

  @Override
  public BTreeGetByPosition getGet() {
    return get;
  }

  @Override
  public void handleLine(String line) {
    getLogger().debug("Got line %s", line);

    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      addRedirectSingleKeyOperation(line, key);
      transitionState(OperationState.REDIRECT);
      return;
    }
    /* ENABLE_MIGRATION end */

    /*
      VALUE <flags> <count>\r\n
      <bkey> [<eflag>] <bytes> <data>\r\n
      [ ... ]
      END\r\n
     */
    if (line.startsWith("VALUE ")) {
      String[] stuff = line.split(" ");
      assert stuff.length == 3;
      assert "VALUE".equals(stuff[0]);

      flags = Integer.parseInt(stuff[1]);
      count = Integer.parseInt(stuff[2]);

      if (count > 0) {
        // position counter
        pos = get.isReversed() ? get.getPosTo() + count - 1 : get.getPosFrom();
        posDiff = get.isReversed() ? -1 : 1;

        // start to read actual data
        setReadType(OperationReadType.DATA);
      }
    } else {
      OperationStatus status = matchStatus(line, END, NOT_FOUND,
              UNREADABLE, TYPE_MISMATCH, NOT_FOUND_ELEMENT);
      getLogger().debug(status);
      getCallback().receivedStatus(status);
      transitionState(OperationState.COMPLETE);
    }
  }

  @Override
  public void handleRead(ByteBuffer bb) {
    // Decode a data header.
    if (lookingFor == '\0' && data == null) {
      while (bb.hasRemaining()) {
        byte b = bb.get();
        // Handle spaces to parse the header.
        if (b == ' ') {
          // btree: <bkey> [<eflag>] <bytes> <data>\r\n
          tokens.add(byteBuffer.toString());
          byteBuffer.reset();

          if (eFlagIndex >= 0) {
            if (tokens.size() == eFlagIndex + 1 && tokens.get(eFlagIndex).startsWith("0x")) {
              eHeadCount++;
            }
          }
          if (tokens.size() == eHeadCount) {
            get.decodeElemHeader(tokens);
            data = new byte[get.getDataLength()];
            tokens.clear();
            eHeadCount = get.getEHeadCount();
            break;
          }
          continue;
        }

        // Ready to finish.
        if (b == '\r') {
          continue;
        }

        // Finish the operation.
        if (b == '\n') {
          OperationStatus status = matchStatus(byteBuffer.toString(),
                  END, NOT_FOUND, UNREADABLE, TYPE_MISMATCH,
                  NOT_FOUND_ELEMENT);

          getLogger().debug("Get complete!");
          getCallback().receivedStatus(status);
          transitionState(OperationState.COMPLETE);
          data = null;
          break;
        }
        // Write to the result ByteBuffer
        byteBuffer.write(b);
      }
      return;
    }

    // Read data
    assert key != null;
    assert data != null;
    // This will be the case, because we'll clear them when it's not.
    assert readOffset <= data.length
            : "readOffset is " + readOffset + " data.length is " + data.length;

    getLogger().debug("readOffset: %d, length: %d", readOffset, data.length);

    if (lookingFor == '\0') {
      int toRead = data.length - readOffset;
      int available = bb.remaining();
      toRead = Math.min(toRead, available);

      getLogger().debug("Reading %d bytes", toRead);

      bb.get(data, readOffset, toRead);
      readOffset += toRead;
    }

    if (lookingFor == '\0' && readOffset == data.length) {
      // put an element data.
      BTreeGetByPositionOperation.Callback cb =
          (BTreeGetByPositionOperation.Callback) getCallback();
      cb.gotData(pos, flags, get.getBkey(), get.getElementFlag(), data);

      // next position.
      pos += posDiff;
      lookingFor = '\r';
    }

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
            assert false : "Looking for unexpected char: "
                    + (char) lookingFor;
        }
      } while (lookingFor != '\0' && bb.hasRemaining());

      if (lookingFor == '\0') {
        data = null;
        readOffset = 0;
      }
    }
  }

  @Override
  public void initialize() {
    String cmd = get.getCommand();
    String args = get.stringify();

    ByteBuffer bb = ByteBuffer.allocate(cmd.length() + KeyUtil.getKeyBytes(key).length
            + args.length() + 16);

    setArguments(bb, cmd, key, args);
    ((Buffer) bb).flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: %s",
              (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

}
