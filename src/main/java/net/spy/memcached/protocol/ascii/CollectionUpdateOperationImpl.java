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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.BTreeUpdate;
import net.spy.memcached.collection.MapUpdate;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.CollectionUpdate;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.CollectionUpdateOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to update collection data in a memcached server.
 */
public class CollectionUpdateOperationImpl extends OperationImpl implements
        CollectionUpdateOperation {

  private static final int OVERHEAD = 32;

  private static final OperationStatus STORE_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus UPDATED = new CollectionOperationStatus(
          true, "UPDATED", CollectionResponse.UPDATED);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
          false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);
  private static final OperationStatus NOTHING_TO_UPDATE = new CollectionOperationStatus(
          false, "NOTHING_TO_UPDATE", CollectionResponse.NOTHING_TO_UPDATE);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  private static final OperationStatus EFLAG_MISMATCH = new CollectionOperationStatus(
          false, "EFLAG_MISMATCH", CollectionResponse.EFLAG_MISMATCH);
  private static final OperationStatus SERVER_ERROR = new CollectionOperationStatus(
          false, "SERVER_ERROR", CollectionResponse.SERVER_ERROR);

  protected final String key;
  protected final String subkey; // e.g.) 0 or 0x00
  protected final CollectionUpdate<?> collectionUpdate;
  protected final byte[] data;

  public CollectionUpdateOperationImpl(String key, String subkey,
                                       CollectionUpdate<?> collectionUpdate, byte[] data,
                                       OperationCallback cb) {
    super(cb);
    this.key = key;
    this.subkey = subkey;
    this.collectionUpdate = collectionUpdate;
    this.data = data;
    if (this.collectionUpdate instanceof BTreeUpdate)
      setAPIType(APIType.BOP_UPDATE);
    else if (this.collectionUpdate instanceof MapUpdate)
      setAPIType(APIType.MOP_UPDATE);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING : "Read ``" + line
            + "'' when in " + getState() + " state";
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
    getCallback().receivedStatus(
            matchStatus(line, UPDATED, NOT_FOUND, NOT_FOUND_ELEMENT,
                    NOTHING_TO_UPDATE, TYPE_MISMATCH, BKEY_MISMATCH,
                    EFLAG_MISMATCH, SERVER_ERROR));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    String args = collectionUpdate.stringify();

    StringBuilder b = new StringBuilder();
    ElementFlagUpdate eflagUpdate = collectionUpdate.getElementFlagUpdate();
    if (eflagUpdate != null) {
      if (eflagUpdate.getElementFlagOffset() > -1 && eflagUpdate.getBitOp() != null) {
        b.append(eflagUpdate.getElementFlagOffset()).append(" ");
        b.append(eflagUpdate.getBitOp()).append(" ");
      }
      b.append(eflagUpdate.getElementFlagByHex());
    }
    String eflagStr = b.toString();

    ByteBuffer bb = ByteBuffer
            .allocate(((data != null) ? data.length : 0)
                    + KeyUtil.getKeyBytes(key).length
                    + KeyUtil.getKeyBytes(subkey).length
                    + eflagStr.length() + args.length()
                    + OVERHEAD);

    setArguments(bb, collectionUpdate.getCommand(), key, subkey,
            eflagStr, (data != null ? data.length : "-1"), args);

    if (data != null) {
      bb.put(data);
      bb.put(CRLF);
    }
    bb.flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug(
              "Request in ascii protocol: '"
                      + (new String(bb.array())).replaceAll("\\r\\n",
                      "\r\n") + "'");
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(STORE_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public String getSubKey() {
    return subkey;
  }

  public CollectionUpdate<?> getUpdate() {
    return collectionUpdate;
  }

  public byte[] getData() {
    return data;
  }

}
