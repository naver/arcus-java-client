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
package net.spy.memcached.protocol.ascii;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.BTreeGetByPosition;
import net.spy.memcached.collection.BTreeStoreAndGet;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeStoreAndGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

public class BTreeStoreAndGetOperationImpl extends OperationImpl implements
        BTreeStoreAndGetOperation {

  private static final int OVERHEAD = 32;

  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus CREATED_STORED = new CollectionOperationStatus(
          true, "CREATED_STORED", CollectionResponse.CREATED_STORED);
  private static final OperationStatus STORED = new CollectionOperationStatus(
          true, "STORED", CollectionResponse.STORED);
  private static final OperationStatus REPLACED = new CollectionOperationStatus(
          true, "REPLACED", CollectionResponse.REPLACED);
  private static final OperationStatus TRIMMED = new CollectionOperationStatus(
          true, "TRIMMED", CollectionResponse.TRIMMED);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus ELEMENT_EXISTS = new CollectionOperationStatus(
          false, "ELEMENT_EXISTS", CollectionResponse.ELEMENT_EXISTS);
  private static final OperationStatus OVERFLOWED = new CollectionOperationStatus(
          false, "OVERFLOWED", CollectionResponse.OVERFLOWED);
  private static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
          false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  private static final OperationStatus UNDEFINED_OPERATION = new CollectionOperationStatus(
          false, "UNDEFINED_OPERATION", CollectionResponse.UNDEFINED);

  private static final OperationStatus[] INSERT_AND_GET_STATUS_ON_LINE = {
          STORED, CREATED_STORED, NOT_FOUND, ELEMENT_EXISTS, OVERFLOWED,
          OUT_OF_RANGE, TYPE_MISMATCH, BKEY_MISMATCH};

  private static final OperationStatus[] UPSERT_AND_GET_STATUS_ON_LINE = {
          STORED, CREATED_STORED, REPLACED, NOT_FOUND, OVERFLOWED,
          OUT_OF_RANGE, TYPE_MISMATCH, BKEY_MISMATCH};

  private static final OperationStatus[] STORE_AND_GET_ON_DATA = {TRIMMED};

  protected final String key;
  protected final BTreeStoreAndGet<?> get;
  protected final byte[] dataToStore;

  protected int flags = 0;
  protected int count = 0;
  protected byte[] data = null;
  protected int readOffset = 0;
  protected byte lookingFor = '\0';
  protected int spaceCount = 0;

  private Boolean hasEFlag = null;

  public BTreeStoreAndGetOperationImpl(String key, BTreeStoreAndGet<?> get,
                                       byte[] dataToStore, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.get = get;
    this.dataToStore = dataToStore;
    if (get.getCmd() == BTreeStoreAndGet.Command.INSERT)
      setAPIType(APIType.BOP_INSERT);
    else if (get.getCmd() == BTreeStoreAndGet.Command.UPSERT)
      setAPIType(APIType.BOP_UPSERT);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public BTreeStoreAndGet<?> getGet() {
    return get;
  }

  @Override
  public void handleLine(String line) {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Got line %s", line);
    }

		/* ENABLE_REPLICATION if */
    if (line.equals("SWITCHOVER") || line.equals("REPL_SLAVE")) {
      receivedMoveOperations(line);
      return;
    }

		/* ENABLE_REPLICATION end */
    /* ENABLE_MIGRATION if */
    if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, true);
      return;
    }
		/* ENABLE_MIGRATION end */
    // VALUE <flags> <count>\r\n
    if (line.startsWith("VALUE ")) {
      String[] stuff = line.split(" ");
      assert stuff.length == 3;
      assert "VALUE".equals(stuff[0]);

      flags = Integer.parseInt(stuff[1]);
      count = Integer.parseInt(stuff[2]);

      if (count > 0) {
        // start to read actual data
        setReadType(OperationReadType.DATA);
      }
    } else {
      OperationStatus status = null;
      switch (get.getCmd()) {
        case INSERT:
          status = matchStatus(line, INSERT_AND_GET_STATUS_ON_LINE);
          break;
        case UPSERT:
          status = matchStatus(line, UPSERT_AND_GET_STATUS_ON_LINE);
          break;
        default:
          status = UNDEFINED_OPERATION;
      }
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(status);
      }
      getCallback().receivedStatus(status);
      transitionState(OperationState.COMPLETE);
      return;
    }
  }

  @Override
  public void handleRead(ByteBuffer bb) {
    // Decode a data header.
    if (lookingFor == '\0' && data == null) {
      for (int i = 0; bb.remaining() > 0; i++) {
        byte b = bb.get();
        // Handle spaces to parse the header.
        if (b == ' ') {
          // One-time check to find if this responses have eflags.
          if (hasEFlag == null && spaceCount == BTreeGetByPosition.HEADER_EFLAG_POSITION + 1) {
            String[] chunk = new String(byteBuffer.toByteArray())
                    .split(" ");
            if (chunk[BTreeGetByPosition.HEADER_EFLAG_POSITION].startsWith("0x")) {
              hasEFlag = true;
            } else {
              hasEFlag = false;
            }
          }

          spaceCount++;

          // Parse the value header.
          // FIXME this is not cool... please fix this :-(
          int spaceReduced = (hasEFlag != null && hasEFlag) ? 1 : 0;
          if (get.headerReady(spaceCount - spaceReduced)) {
            get.decodeItemHeader(new String(byteBuffer.toByteArray()));
            data = new byte[get.getBytes()];
            byteBuffer.reset();
            spaceCount = 0;
            hasEFlag = null;
            break;
          }
        }

        // Ready to finish.
        if (b == '\r') {
          continue;
        }

        // Finish the operation.
        if (b == '\n') {
          OperationStatus status = matchStatus(byteBuffer.toString(), STORE_AND_GET_ON_DATA);

          if (getLogger().isDebugEnabled()) {
            getLogger().debug("Get complete!");
          }
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

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("readOffset: %d, length: %d", readOffset, data.length);
    }

    if (lookingFor == '\0') {
      int toRead = data.length - readOffset;
      int available = bb.remaining();
      toRead = Math.min(toRead, available);

      if (getLogger().isDebugEnabled()) {
        getLogger().debug("Reading %d bytes", toRead);
      }

      bb.get(data, readOffset, toRead);
      readOffset += toRead;
    }

    if (lookingFor == '\0' && readOffset == data.length) {
      // put an element data.
      BTreeStoreAndGetOperation.Callback cb = (BTreeStoreAndGetOperation.Callback) getCallback();
      cb.gotData(key, flags, get.getBkeyObject(), get.getElementFlag(), data);

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
    String args = get.stringify();
    ByteBuffer bb = ByteBuffer.allocate(dataToStore.length
            + KeyUtil.getKeyBytes(key).length
            + KeyUtil.getKeyBytes(get.getBkeyObject().getBKeyAsString()).length
            + KeyUtil.getKeyBytes(get.getElementFlagByHex()).length
            + args.length()
            + OVERHEAD);
    setArguments(bb, get.getCommand(), key, get.getBkeyObject().getBKeyAsString(),
            get.getElementFlagByHex(), dataToStore.length, args);
    bb.put(dataToStore);
    bb.put(CRLF);
    bb.flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug(
              "Request in ascii protocol: "
                      + (new String(bb.array()))
                      .replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(GET_CANCELED);
  }

  @Override
  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

}
