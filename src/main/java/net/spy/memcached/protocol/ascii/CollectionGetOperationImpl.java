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
import net.spy.memcached.collection.BTreeGet;
import net.spy.memcached.collection.CollectionGet;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ListGet;
import net.spy.memcached.collection.MapGet;
import net.spy.memcached.collection.SetGet;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to retrieve collection data in a memcached server.
 */
public final class CollectionGetOperationImpl extends OperationImpl
        implements CollectionGetOperation {

  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus DELETED = new CollectionOperationStatus(
          true, "DELETED", CollectionResponse.DELETED);
  private static final OperationStatus DELETED_DROPPED = new CollectionOperationStatus(
          true, "DELETED_DROPPED", CollectionResponse.DELETED_DROPPED);
  private static final OperationStatus TRIMMED = new CollectionOperationStatus(
          true, "TRIMMED", CollectionResponse.TRIMMED);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
          false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);
  private static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
          false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);

  private final String key;
  private final CollectionGet collectionGet;

  private int flags = 0;
  private int count = 0;
  private byte[] data = null;
  private int readOffset = 0;
  private byte lookingFor = '\0';
  private final List<String> tokens = new ArrayList<>();
  private int eHeadCount;
  private int eFlagIndex;

  public CollectionGetOperationImpl(String key, CollectionGet collectionGet,
                                    OperationCallback cb) {
    super(cb);
    this.key = key;
    this.collectionGet = collectionGet;
    this.eHeadCount = collectionGet.getEHeadCount();
    this.eFlagIndex = collectionGet.getEFlagIndex();
    if (this.collectionGet instanceof ListGet) {
      setAPIType(APIType.LOP_GET);
    } else if (this.collectionGet instanceof SetGet) {
      setAPIType(APIType.SOP_GET);
    } else if (this.collectionGet instanceof MapGet) {
      setAPIType(APIType.MOP_GET);
    } else if (this.collectionGet instanceof BTreeGet) {
      setAPIType(APIType.BOP_GET);
    }
    if (collectionGet.isDelete()) {
      setOperationType(OperationType.WRITE);
    } else {
      setOperationType(OperationType.READ);
    }
  }

  public void handleLine(String line) {
    /* ENABLE_REPLICATION if */
    if (hasSwitchedOver(line)) {
      receivedMoveOperations(line);
      return;
    }
    /* ENABLE_REPLICATION end */
    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      addRedirectSingleKeyOperation(line, key);
      transitionState(OperationState.REDIRECT);
      return;
    }
    /* ENABLE_MIGRATION end */
    /*
      VALUE <flag> <count>\r\n
      <collection_data>\r\n
      [ ... ]
      END|TRIMMED|DELETED|DELETED_DROPPED\r\n
     */
    if (line.startsWith("VALUE ")) {
      // Response header
      getLogger().debug("Got line %s", line);

      String[] stuff = line.split(" ");
      assert "VALUE".equals(stuff[0]);

      flags = Integer.parseInt(stuff[1]);
      count = Integer.parseInt(stuff[2]);

      setReadType(OperationReadType.DATA);
    } else {
      OperationStatus status = matchStatus(line, END, TRIMMED, DELETED,
              DELETED_DROPPED, NOT_FOUND, NOT_FOUND_ELEMENT,
              OUT_OF_RANGE, TYPE_MISMATCH, BKEY_MISMATCH, UNREADABLE);
      getLogger().debug(status);
      getCallback().receivedStatus(status);
      transitionState(OperationState.COMPLETE);
    }
  }

  @Override
  public final void handleRead(ByteBuffer bb) {
    // Decode a collection data header.
    if (lookingFor == '\0' && data == null) {
      while (bb.hasRemaining()) {
        byte b = bb.get();

        // Handle spaces.
        if (b == ' ') {
           /*
             btree: <bkey> [<eflag>] <bytes> <data>\r\n
             list:  <bytes> <data>\r\n
             set:   <bytes> <data>\r\n
             map:   <field> <bytes> <data>\r\n
           */
          tokens.add(byteBuffer.toString());
          byteBuffer.reset();

          if (eFlagIndex >= 0) {
            if (tokens.size() == eFlagIndex + 1 && tokens.get(eFlagIndex).startsWith("0x")) {
              eHeadCount++;
            }
          }
          if (tokens.size() == eHeadCount) {
            collectionGet.decodeElemHeader(tokens);
            data = new byte[collectionGet.getDataLength()];
            tokens.clear();
            eHeadCount = collectionGet.getEHeadCount();
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
                  END, TRIMMED, DELETED, DELETED_DROPPED, NOT_FOUND,
                  NOT_FOUND_ELEMENT, OUT_OF_RANGE, TYPE_MISMATCH,
                  BKEY_MISMATCH, UNREADABLE);

          getLogger().debug("Get complete!");
          getCallback().receivedStatus(status);
          transitionState(OperationState.COMPLETE);
          data = null;
          break;
        }
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
      CollectionGetOperation.Callback cb =
              (CollectionGetOperation.Callback) getCallback();
      cb.gotData(collectionGet.getSubkey(), flags, data, collectionGet.getElementFlag());
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

  public void initialize() {
    String cmd = collectionGet.getCommand();
    if (collectionGet instanceof MapGet) {
      MapGet mapGet = (MapGet) collectionGet;
      if (getHandlingNode() == null || getHandlingNode().enabledSpaceSeparate()) {
        mapGet.setKeySeparator(" ");
      } else {
        mapGet.setKeySeparator(",");
      }
    }
    String args = collectionGet.stringify();
    byte[] additionalArgs = collectionGet.getAddtionalArgs();

    ByteBuffer bb = ByteBuffer.allocate(
            KeyUtil.getKeyBytes(key).length + cmd.length() + args.length() +
                    (additionalArgs == null ? 0 : additionalArgs.length) + 16);

    setArguments(bb, cmd, key, args);
    if (additionalArgs != null) {
      bb.put(additionalArgs);
      bb.put(CRLF);
    }

    ((Buffer) bb).flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: %s",
              (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(GET_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public CollectionGet getGet() {
    return collectionGet;
  }

  @Override
  public boolean isBulkOperation() {
    return false;
  }

  @Override
  public boolean isPipeOperation() {
    return false;
  }

  @Override
  public boolean isIdempotentOperation() {
    return !collectionGet.isDelete() && !collectionGet.isDropIfEmpty();
  }

}
