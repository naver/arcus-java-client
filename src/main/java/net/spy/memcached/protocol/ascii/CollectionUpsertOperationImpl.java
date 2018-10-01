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
import net.spy.memcached.collection.BTreeUpsert;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.CollectionStore;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.CollectionUpsertOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to store collection data in a memcached server.
 */
public class CollectionUpsertOperationImpl extends OperationImpl implements
        CollectionUpsertOperation {

  private static final int OVERHEAD = 32;

  private static final OperationStatus STORE_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus CREATED_STORED = new CollectionOperationStatus(
          true, "CREATED_STORED", CollectionResponse.CREATED_STORED);
  private static final OperationStatus STORED = new CollectionOperationStatus(
          true, "STORED", CollectionResponse.STORED);
  private static final OperationStatus REPLACED = new CollectionOperationStatus(
          true, "REPLACED", CollectionResponse.REPLACED);
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

  protected final String key;
  protected final String subkey;
  protected final CollectionStore<?> collectionStore;
  protected final byte[] data;

  public CollectionUpsertOperationImpl(String key, String subkey,
                                       CollectionStore<?> collectionStore, byte[] data,
                                       OperationCallback cb) {
    super(cb);
    this.key = key;
    this.subkey = subkey;
    this.collectionStore = collectionStore;
    this.data = data;
    if (this.collectionStore instanceof BTreeUpsert)
      setAPIType(APIType.BOP_UPSERT);
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
            matchStatus(line, STORED, REPLACED, CREATED_STORED, NOT_FOUND,
                    ELEMENT_EXISTS, OVERFLOWED, OUT_OF_RANGE,
                    TYPE_MISMATCH, BKEY_MISMATCH));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    String args = collectionStore.stringify();
    ByteBuffer bb = ByteBuffer
            .allocate(data.length
                    + KeyUtil.getKeyBytes(key).length
                    + KeyUtil.getKeyBytes(subkey).length
                    + KeyUtil.getKeyBytes(collectionStore.getElementFlagByHex()).length
                    + args.length()
                    + OVERHEAD);
    setArguments(bb, collectionStore.getCommand(), key, subkey,
            collectionStore.getElementFlagByHex(), data.length, args);
    bb.put(data);
    bb.put(CRLF);
    bb.flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug(
              "Request in ascii protocol: "
                      + (new String(bb.array())).replaceAll("\\r\\n",
                      "\r\n"));
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

  public CollectionStore<?> getStore() {
    return collectionStore;
  }

  public byte[] getData() {
    return data;
  }

}
