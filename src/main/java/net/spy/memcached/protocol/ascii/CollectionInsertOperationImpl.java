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
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.BTreeInsert;
import net.spy.memcached.collection.BTreeUpsert;
import net.spy.memcached.collection.CollectionInsert;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ListInsert;
import net.spy.memcached.collection.MapInsert;
import net.spy.memcached.collection.MapUpsert;
import net.spy.memcached.collection.SetInsert;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionInsertOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to store collection data in a memcached server.
 */
public final class CollectionInsertOperationImpl extends OperationImpl
        implements CollectionInsertOperation {

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
  protected final String subkey;    // e.g.) 0 or 0x00
  protected final CollectionInsert<?> collectionInsert;
  protected final byte[] data;

  public CollectionInsertOperationImpl(String key, String subkey,
                                       CollectionInsert<?> collectionInsert, byte[] data,
                                       OperationCallback cb) {
    super(cb);
    this.key = key;
    this.subkey = subkey;
    this.collectionInsert = collectionInsert;
    this.data = data;
    if (this.collectionInsert instanceof ListInsert) {
      setAPIType(APIType.LOP_INSERT);
    } else if (this.collectionInsert instanceof SetInsert) {
      setAPIType(APIType.SOP_INSERT);
    } else if (this.collectionInsert instanceof MapInsert) {
      setAPIType(APIType.MOP_INSERT);
    } else if (this.collectionInsert instanceof MapUpsert) {
      setAPIType(APIType.MOP_UPSERT);
    } else if (this.collectionInsert instanceof BTreeInsert) {
      setAPIType(APIType.BOP_INSERT);
    } else if (this.collectionInsert instanceof BTreeUpsert) {
      setAPIType(APIType.BOP_UPSERT);
    }
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
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

    OperationStatus status = matchStatus(line, STORED, REPLACED, CREATED_STORED,
            NOT_FOUND, ELEMENT_EXISTS, OVERFLOWED, OUT_OF_RANGE,
            TYPE_MISMATCH, BKEY_MISMATCH);
    getCallback().receivedStatus(status);
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    String args = collectionInsert.stringify();
    ByteBuffer bb = ByteBuffer.allocate(data.length
            + KeyUtil.getKeyBytes(key).length
            + KeyUtil.getKeyBytes(subkey).length
            + KeyUtil.getKeyBytes(collectionInsert.getElementFlagByHex()).length
            + args.length()
            + OVERHEAD);
    setArguments(bb, collectionInsert.getCommand(), key, subkey,
            collectionInsert.getElementFlagByHex(), data.length, args);
    bb.put(data);
    bb.put(CRLF);
    ((Buffer) bb).flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: %s",
              (new String(bb.array())).replace("\r\n", "\\r\\n"));
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

  public CollectionInsert<?> getInsert() {
    return collectionInsert;
  }

  public byte[] getData() {
    return data;
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
    return !(collectionInsert instanceof ListInsert);
  }

}
