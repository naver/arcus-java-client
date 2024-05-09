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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.collection.CollectionPipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.BTreePipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.MapPipedUpdate;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.CollectionPipedUpdateOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to update collection data in a memcached server.
 */
public class CollectionPipedUpdateOperationImpl extends OperationImpl implements
        CollectionPipedUpdateOperation {

  private static final OperationStatus STORE_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus FAILED_END = new CollectionOperationStatus(
          false, "END", CollectionResponse.END);

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
  protected final CollectionPipedUpdate<?> update;
  protected final CollectionPipedUpdateOperation.Callback cb;

  protected int count;
  protected int index = 0;
  protected boolean successAll = true;

  public CollectionPipedUpdateOperationImpl(String key,
                                            CollectionPipedUpdate<?> update, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.update = update;
    this.cb = (Callback) cb;
    if (this.update instanceof BTreePipedUpdate) {
      setAPIType(APIType.BOP_UPDATE);
    } else if (this.update instanceof MapPipedUpdate) {
      setAPIType(APIType.MOP_UPDATE);
    }
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING : "Read ``" + line
            + "'' when in " + getState() + " state";

    /* ENABLE_REPLICATION if */
    if (hasSwitchedOver(line)) {
      this.update.setNextOpIndex(index);
      receivedMoveOperations(line);
      return;
    }
    /* ENABLE_REPLICATION end */
    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      // Only one NOT_MY_KEY is provided in response of single key piped operation when redirection.
      addRedirectSingleKeyOperation(line, key);
      if (update.isNotPiped()) {
        transitionState(OperationState.REDIRECT);
      } else {
        update.setNextOpIndex(index);
      }
      return;
    }
    /* ENABLE_MIGRATION end */

    if (update.isNotPiped()) {
      OperationStatus status = matchStatus(line, UPDATED, NOT_FOUND,
              NOT_FOUND_ELEMENT, NOTHING_TO_UPDATE, TYPE_MISMATCH,
              BKEY_MISMATCH, EFLAG_MISMATCH, SERVER_ERROR);
      if (status.isSuccess()) {
        cb.receivedStatus((successAll) ? END : FAILED_END);
      } else {
        cb.gotStatus(index, update.getOpIdx(), status);
        cb.receivedStatus(FAILED_END);
      }
      transitionState(OperationState.COMPLETE);
      return;
    }

    /*
      RESPONSE <count>\r\n
      <status of the 1st pipelined command>\r\n
      [ ... ]
      <status of the last pipelined command>\r\n
      END|PIPE_ERROR <error_string>\r\n
    */
    if (line.startsWith("END") || line.startsWith("PIPE_ERROR ")) {
      /* ENABLE_MIGRATION if */
      if (needRedirect()) {
        transitionState(OperationState.REDIRECT);
        return;
      }
      /* ENABLE_MIGRATION end */
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
      OperationStatus status = matchStatus(line, UPDATED, NOT_FOUND,
              NOT_FOUND_ELEMENT, NOTHING_TO_UPDATE, TYPE_MISMATCH,
              BKEY_MISMATCH, EFLAG_MISMATCH, SERVER_ERROR);

      if (!status.isSuccess()) {
        cb.gotStatus(index, update.getOpIdx(), status);
        successAll = false;
      }
      index++;
    }
  }

  @Override
  public void initialize() {
    ByteBuffer buffer = update.getAsciiCommand();
    setBuffer(buffer);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug(
              "Request in ascii protocol: %s",
                      (new String(buffer.array())).replaceAll("\\r\\n", "\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(STORE_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public CollectionPipedUpdate<?> getUpdate() {
    return update;
  }

  @Override
  public boolean isBulkOperation() {
    return false;
  }

  @Override
  public boolean isPipeOperation() {
    return true;
  }

  @Override
  public boolean isIdempotentOperation() {
    return true;
  }

}
