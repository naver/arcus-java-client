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
import java.util.List;

import net.spy.memcached.collection.CollectionBulkStore;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.CollectionBulkStoreOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to store collection data in a memcached server.
 */
public class CollectionBulkStoreOperationImpl extends OperationImpl
        implements CollectionBulkStoreOperation {

  private static final OperationStatus STORE_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus FAILED_END = new CollectionOperationStatus(
          false, "END", CollectionResponse.END);

  private static final OperationStatus CREATED_STORED = new CollectionOperationStatus(
          true, "CREATED_STORED", CollectionResponse.CREATED_STORED);
  private static final OperationStatus STORED = new CollectionOperationStatus(
          true, "STORED", CollectionResponse.STORED);
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
  protected final CollectionBulkStore<?> store;
  protected final CollectionBulkStoreOperation.Callback cb;

  protected int count;
  protected int index = 0;
  protected boolean successAll = true;

  public CollectionBulkStoreOperationImpl(List<String> keyList,
                                          CollectionBulkStore<?> store, OperationCallback cb) {
    super(cb);
    this.key = keyList.get(0);
    this.store = store;
    this.cb = (Callback) cb;
    if (this.store instanceof CollectionBulkStore.ListBulkStore)
      setAPIType(APIType.LOP_INSERT);
    else if (this.store instanceof CollectionBulkStore.SetBulkStore)
      setAPIType(APIType.SOP_INSERT);
    else if (this.store instanceof CollectionBulkStore.MapBulkStore)
      setAPIType(APIType.MOP_INSERT);
    else if (this.store instanceof CollectionBulkStore.BTreeBulkStore)
      setAPIType(APIType.BOP_INSERT);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
    /* ENABLE_REPLICATION if */
    if (line.equals("SWITCHOVER") || line.equals("REPL_SLAVE")) {
      this.store.setNextOpIndex(index);
      receivedMoveOperations(line);
      return;
    }

    /* ENABLE_REPLICATION end */
    if (store.getItemCount() == 1) {
      OperationStatus status = matchStatus(line, STORED, CREATED_STORED,
              NOT_FOUND, ELEMENT_EXISTS, OVERFLOWED, OUT_OF_RANGE,
              TYPE_MISMATCH, BKEY_MISMATCH);
      if (status.isSuccess()) {
        cb.receivedStatus(END);
      } else {
        cb.gotStatus(index, status);
        cb.receivedStatus(FAILED_END);
      }
      transitionState(OperationState.COMPLETE);
      return;
    }

    if (line.startsWith("END") || line.startsWith("PIPE_ERROR ")) {
      cb.receivedStatus((successAll) ? END : FAILED_END);
      transitionState(OperationState.COMPLETE);
    } else if (line.startsWith("RESPONSE ")) {
      getLogger().debug("Got line %s", line);

      // TODO server should be fixed
      line = line.replace("   ", " ");
      line = line.replace("  ", " ");

      String[] stuff = line.split(" ");
      assert "RESPONSE".equals(stuff[0]);
      count = Integer.parseInt(stuff[1]);
    } else {
      OperationStatus status = matchStatus(line, STORED, CREATED_STORED,
              NOT_FOUND, ELEMENT_EXISTS, OVERFLOWED, OUT_OF_RANGE,
              TYPE_MISMATCH, BKEY_MISMATCH);

      if (!status.isSuccess()) {
        cb.gotStatus(index, status);
        successAll = false;
      }

      index++;
    }
  }

  @Override
  public void initialize() {
    ByteBuffer buffer = store.getAsciiCommand();
    setBuffer(buffer);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: \n"
              + (new String(buffer.array())).replaceAll("\\r\\n", "\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(STORE_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public CollectionBulkStore<?> getStore() {
    return store;
  }

}
