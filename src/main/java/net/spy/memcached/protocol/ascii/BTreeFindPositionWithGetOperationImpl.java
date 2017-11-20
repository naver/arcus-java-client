/*
 * arcus-java-client : Arcus Java client
 * Copyright 2014 JaM2in Co., Ltd.
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
import net.spy.memcached.collection.BTreeFindPositionWithGet;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeFindPositionWithGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

public class BTreeFindPositionWithGetOperationImpl extends OperationImpl implements
        BTreeFindPositionWithGetOperation {

  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
          false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);

  protected final String key;
  protected final BTreeFindPositionWithGet get;

  protected int position = 0;
  protected int flags = 0;
  protected int count = 0;
  protected int index = 0;
  protected int pos = 0;
  protected int posDiff = 0;
  protected byte[] data = null;
  protected int readOffset = 0;
  protected byte lookingFor = '\0';
  protected int spaceCount = 0;

  private Boolean hasEFlag = null;

  public BTreeFindPositionWithGetOperationImpl(String key,
                                               BTreeFindPositionWithGet get, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.get = get;
    setAPIType(APIType.BOP_PWG);
    setOperationType(OperationType.READ);
  }

  @Override
  public BTreeFindPositionWithGet getGet() {
    return get;
  }

  @Override
  public void handleLine(String line) {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Got line %s", line);
    }

    if (line.startsWith("VALUE ")) {
      // VALUE <position> <flags> <count> <index>\r\n
      String[] stuff = line.split(" ");
      assert stuff.length == 5;

      position = Integer.parseInt(stuff[1]);
      flags = Integer.parseInt(stuff[2]);
      count = Integer.parseInt(stuff[3]);
      index = Integer.parseInt(stuff[4]);

      assert count > 0;
      // position counter
      pos = position - index;
      posDiff = 1;

      // start to read actual data
      setReadType(OperationReadType.DATA);
    /* ENABLE_MIGRATION if */
    } else if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, true);
    /* ENABLE_MIGRATION end */
    } else {
      OperationStatus status = matchStatus(line, END, NOT_FOUND, NOT_FOUND_ELEMENT,
              UNREADABLE, TYPE_MISMATCH, BKEY_MISMATCH);
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(status);
      }
      getCallback().receivedStatus(status);
      transitionState(OperationState.COMPLETE);
    }
  }

  @Override
  public void handleRead(ByteBuffer bb) {
    // Decode a data header.
    if (lookingFor == '\0' && data == null) {
      for (int i = 0; bb.remaining() > 0; i++) {
        byte b = bb.get();
        if (b == ' ') { // Handle spaces to parse the header.
          // One-time check to find if this responses have eflags.
          if (hasEFlag == null && spaceCount == 1) {
            String[] chunk = new String(byteBuffer.toByteArray()).split(" ");
            if (chunk[1].startsWith("0x")) {
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
            data = new byte[get.getDataLength()];
            byteBuffer.reset();
            spaceCount = 0;
            hasEFlag = null;
            break;
          }
        } else if (b == '\r') { // Ready to finish.
          continue;
        } else if (b == '\n') { // Finish the operation.
          OperationStatus status = matchStatus(byteBuffer.toString(), END,
                  NOT_FOUND, NOT_FOUND_ELEMENT, UNREADABLE, TYPE_MISMATCH, BKEY_MISMATCH);

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
      BTreeFindPositionWithGetOperation.Callback cb = (BTreeFindPositionWithGetOperation.Callback) getCallback();
      cb.gotData(key, flags, pos, get.getBkey(), get.getEflag(), data);

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

    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + cmd.length() + args.length() + 16);
    setArguments(bb, cmd, key, args);
    bb.flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: " + (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(GET_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }
}
