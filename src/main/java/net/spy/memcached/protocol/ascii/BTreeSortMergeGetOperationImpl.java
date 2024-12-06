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
import java.util.Collection;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkey;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.util.BTreeUtil;

/**
 * Operation to retrieve b+tree data with multiple keys
 */
public final class BTreeSortMergeGetOperationImpl extends OperationImpl implements
        BTreeSortMergeGetOperation {

  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus DUPLICATED = new CollectionOperationStatus(
          true, "DUPLICATED", CollectionResponse.DUPLICATED);
  private static final OperationStatus TRIMMED = new CollectionOperationStatus(
          true, "TRIMMED", CollectionResponse.TRIMMED);
  private static final OperationStatus DUPLICATED_TRIMMED = new CollectionOperationStatus(
          true, "DUPLICATED_TRIMMED", CollectionResponse.DUPLICATED_TRIMMED);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);
  private static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
          false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);
  private static final OperationStatus ATTR_MISMATCH = new CollectionOperationStatus(
          false, "ATTR_MISMATCH", CollectionResponse.ATTR_MISMATCH);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);

  private final BTreeSMGet<?> smGet;

  private int flags = 0;
  private int lineCount = 0;
  private byte[] data = null;
  private int readOffset = 0;
  private byte lookingFor = '\0';
  private int spaceCount = 0;

  private ReadState readState = ReadState.VALUE;

  public BTreeSortMergeGetOperationImpl(BTreeSMGet<?> smGet,
                                        OperationCallback cb) {
    super(cb);
    this.smGet = smGet;
    setAPIType(APIType.BOP_SMGET);
    setOperationType(OperationType.READ);
  }

  public void handleLine(String line) {
    // Response header
    getLogger().debug("Got line %s", line);

    /*
      VALUE|ELEMENTS <ecount>\r\n
      <key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
      [ ... ]
      MISSED_KEYS <kcount>\r\n
      <key> [<cause>]\r\n
      [ ... ]
      TRIMMED_KEYS <kcount>\r\n
      <key> <bkey>\r\n
      [ ... ]
      END|DUPLICATED|TRIMMED|DUPLICATRED_TRIMMED\r\n
     */
    if (line.startsWith("VALUE ") ||
            line.startsWith("ELEMENTS ")) {
      readState = ReadState.VALUE;

      String[] stuff = line.split(" ");
      assert "VALUE".equals(stuff[0]) ||
              "ELEMENTS".equals(stuff[0]);

      lineCount = Integer.parseInt(stuff[1]);

      if (lineCount > 0) {
        setReadType(OperationReadType.DATA);
      }
    } else if (line.startsWith("MISSED_KEYS")) {
      readState = ReadState.MISSED_KEYS;

      String[] stuff = line.split(" ");
      assert "MISSED_KEYS".equals(stuff[0]);

      lineCount = Integer.parseInt(stuff[1]);

      if (lineCount > 0) {
        setReadType(OperationReadType.DATA);
      }
    } else if (line.startsWith("TRIMMED_KEYS")) {
      readState = ReadState.TRIMMED_KEYS;

      String[] stuff = line.split(" ");
      assert "TRIMMED_KEYS".equals(stuff[0]);

      lineCount = Integer.parseInt(stuff[1]);

      if (lineCount > 0) {
        setReadType(OperationReadType.DATA);
      }
    } else {
      OperationStatus status = matchStatus(line, END, TRIMMED,
              DUPLICATED, DUPLICATED_TRIMMED, OUT_OF_RANGE,
              ATTR_MISMATCH, TYPE_MISMATCH, BKEY_MISMATCH);
      getLogger().debug(status);
      getCallback().receivedStatus(status);
      transitionState(OperationState.COMPLETE);
    }
  }

  @Override
  public void handleRead(ByteBuffer bb) {
    switch (readState) {
      case VALUE:
        readValue(bb);
        break;
      case MISSED_KEYS:
        readMissedKeys(bb);
        break;
      case TRIMMED_KEYS:
        readTrimmedKeys(bb);
        break;
    }
  }

  private void readValue(ByteBuffer bb) {
    // Decode a collection data header.
    int count = 0;
    if (lookingFor == '\0' && data == null) {
      while (bb.remaining() > 0) {
        byte b = bb.get();

        // Handle spaces.
        if (b == ' ') {
          spaceCount++;
          if (smGet.headerReady(spaceCount)) {
            // Adjust space count if item header has a element flag.
            String itemHeader = byteBuffer.toString();
            String[] chunk = itemHeader.split(" ");
            if (chunk.length == BTreeSMGet.headerCount
                    && chunk[3].startsWith("0x")) {
              spaceCount--;
              byteBuffer.write(b);
              continue;
            }

            // <key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
            smGet.decodeItemHeader(itemHeader);
            data = new byte[smGet.getDataLength()];
            byteBuffer.reset();
            spaceCount = 0;
            break;
          }
        }

        // Ready to finish.
        if (b == '\r') {
          continue;
        }

        // Finish the operation.
        if (b == '\n') {
          String sep = byteBuffer.toString();
          if (sep.startsWith("MISSED_KEYS")) {
            readState = ReadState.MISSED_KEYS;
            byteBuffer.reset();
            spaceCount = 0;

            String[] stuff = sep.split(" ");
            lineCount = Integer.parseInt(stuff[1]);

            return;
          }

          OperationStatus status = matchStatus(byteBuffer.toString(),
                  END, TRIMMED, DUPLICATED, DUPLICATED_TRIMMED,
                  OUT_OF_RANGE, ATTR_MISMATCH, TYPE_MISMATCH,
                  BKEY_MISMATCH);

          getCallback().receivedStatus(status);
          //transitionState(OperationState.COMPLETE);
          data = null;
          break;
        }

        byteBuffer.write(b);
      }
      return;
    }

    // Read data
    // assert key != null;
    assert data != null;

    // This will be the case, because we'll clear them when it's not.
    assert readOffset <= data.length : "readOffset is " + readOffset
            + " data.length is " + data.length;

    getLogger()
            .debug("readOffset: %d, length: %d", readOffset, data.length);

    if (lookingFor == '\0') {
      int toRead = data.length - readOffset;
      int available = bb.remaining();
      toRead = Math.min(toRead, available);

      getLogger().debug("Reading %d bytes", toRead);

      bb.get(data, readOffset, toRead);
      readOffset += toRead;
    }

    if (lookingFor == '\0' && readOffset == data.length && count < lineCount) {
      BTreeSortMergeGetOperation.Callback cb = (BTreeSortMergeGetOperation.Callback) getCallback();
      cb.gotData(smGet.getKey(), smGet.getFlags(), smGet.getBkey(), smGet.getEflag(), data);
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

  private void readMissedKeys(ByteBuffer bb) {
    int count = 0;
    if (lookingFor == '\0' && data == null) {
      while (bb.remaining() > 0) {
        byte b = bb.get();

        // Ready to finish.
        if (b == '\r') {
          continue;
        }

        // Finish the operation.
        if (b == '\n') {
          String sep = byteBuffer.toString();
          if (sep.startsWith("TRIMMED_KEYS")) {
            readState = ReadState.TRIMMED_KEYS;
            byteBuffer.reset();
            spaceCount = 0;

            String[] stuff = sep.split(" ");
            lineCount = Integer.parseInt(stuff[1]);

            return;
          }

          OperationStatus status = matchStatus(byteBuffer.toString(),
                  END, TRIMMED, DUPLICATED, DUPLICATED_TRIMMED,
                  OUT_OF_RANGE, ATTR_MISMATCH, TYPE_MISMATCH,
                  BKEY_MISMATCH);

          if (status.isSuccess()) {
            /* ENABLE_MIGRATION if */
            if (needRedirect()) {
              transitionState(OperationState.REDIRECT);
              return;
            }
            /* ENABLE_MIGRATION end */
            getCallback().receivedStatus(status);
            transitionState(OperationState.COMPLETE);
            return;
          } else if (count < lineCount) {
            // <key> [<cause>]\r\n
            String line = byteBuffer.toString();
            String[] chunk = line.split(" ");
            if (chunk.length == 2) {
              ((BTreeSortMergeGetOperation.Callback) getCallback())
                      .gotMissedKey(chunk[0], matchStatus(chunk[1],
                              NOT_FOUND, UNREADABLE, OUT_OF_RANGE));
              /* ENABLE_MIGRATION if */
            } else if (hasNotMyKey(chunk[1])) {
              addRedirectMultiKeyOperation(getNotMyKey(line), chunk[0]);
              /* ENABLE_MIGRATION end */
            } else {
              ((BTreeSortMergeGetOperation.Callback) getCallback())
                      .gotMissedKey(chunk[0], new CollectionOperationStatus(false,
                              "UNDEFINED", CollectionResponse.UNDEFINED));
            }
            count++;
          } else {
            /* unexpected response */
            /* ENABLE_MIGRATION if */
            if (needRedirect()) {
              transitionState(OperationState.REDIRECT);
              return;
            }
            /* ENABLE_MIGRATION end */
            getCallback().receivedStatus(status);
            transitionState(OperationState.COMPLETE);
            return;
          }
          byteBuffer.reset();
        } else {
          byteBuffer.write(b);
        }
      }
    }
  }

  private void readTrimmedKeys(ByteBuffer bb) {
    int count = 0;
    if (lookingFor == '\0' && data == null) {
      while (bb.remaining() > 0) {
        byte b = bb.get();

        // Ready to finish.
        if (b == '\r') {
          continue;
        }

        // Finish the operation.
        if (b == '\n') {
          OperationStatus status = matchStatus(byteBuffer.toString(),
                  END, DUPLICATED, OUT_OF_RANGE, ATTR_MISMATCH,
                  TYPE_MISMATCH, BKEY_MISMATCH);

          if (status.isSuccess()) {
            /* ENABLE_MIGRATION if */
            if (needRedirect()) {
              transitionState(OperationState.REDIRECT);
              return;
            }
            /* ENABLE_MIGRATION end */
            getCallback().receivedStatus(status);
            transitionState(OperationState.COMPLETE);
            return;
          } else if (count < lineCount) {
            // <key> <bkey>\r\n
            String[] chunk = byteBuffer.toString().split(" ");
            if (smGet instanceof BTreeSMGetWithLongTypeBkey) {
              ((BTreeSortMergeGetOperation.Callback) getCallback())
                  .gotTrimmedKey(chunk[0], Long.parseLong(chunk[1]));
            } else if (smGet instanceof BTreeSMGetWithByteTypeBkey) {
              ((BTreeSortMergeGetOperation.Callback) getCallback())
                  .gotTrimmedKey(chunk[0],
                      BTreeUtil.hexStringToByteArrays(chunk[1].substring(2)));
            }
            count++;
          } else {
            /* unexpected response */
            /* ENABLE_MIGRATION if */
            if (needRedirect()) {
              transitionState(OperationState.REDIRECT);
              return;
            }
            /* ENABLE_MIGRATION end */
            getCallback().receivedStatus(status);
            transitionState(OperationState.COMPLETE);
            return;
          }
          byteBuffer.reset();
        } else {
          byteBuffer.write(b);
        }
      }
    }
  }

  public void initialize() {
    String cmd = smGet.getCommand();
    if (getHandlingNode() == null || getHandlingNode().enabledSpaceSeparate()) {
      smGet.setKeySeparator(" ");
    } else {
      smGet.setKeySeparator(",");
    }
    String args = smGet.stringify();

    ByteBuffer bb = ByteBuffer.allocate(cmd.length() + args.length()
            + KeyUtil.getKeyBytes(smGet.getSpaceSeparatedKeys()).length + 16);

    setArguments(bb, cmd, args);

    setArguments(bb, smGet.getSpaceSeparatedKeys());

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
    return smGet.getKeyList();
  }

  @Override
  public BTreeSMGet<?> getSMGet() {
    return smGet;
  }

  private enum ReadState {
    VALUE,
    MISSED_KEYS,
    TRIMMED_KEYS,
  }

  @Override
  public boolean isBulkOperation() {
    return true;
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
