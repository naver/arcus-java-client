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

import net.spy.memcached.collection.SetPipedExist;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.CollectionPipedExistOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

public class CollectionPipedExistOperationImpl extends OperationImpl implements
        CollectionPipedExistOperation {

  private static final OperationStatus EXIST_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus EXIST = new CollectionOperationStatus(
          true, "EXIST", CollectionResponse.EXIST);
  private static final OperationStatus NOT_EXIST = new CollectionOperationStatus(
          true, "NOT_EXIST", CollectionResponse.NOT_EXIST);

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus FAILED_END = new CollectionOperationStatus(
          false, "END", CollectionResponse.END);

  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);

  protected final String key;
  protected final SetPipedExist<?> setPipedExist;
  protected final CollectionPipedExistOperation.Callback cb;

  protected int count;
  protected int index = 0;
  protected boolean successAll = true;
  /* ENABLE_MIGRATION if */
  protected boolean migrating = false;
  /* ENABLE_MIGRATION end */

  public CollectionPipedExistOperationImpl(String key,
                                           SetPipedExist<?> collectionExist, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.setPipedExist = collectionExist;
    this.cb = (Callback) cb;
    if (this.setPipedExist instanceof SetPipedExist)
      setAPIType(APIType.SOP_EXIST);
    setOperationType(OperationType.READ);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING : "Read ``" + line
            + "'' when in " + getState() + " state";

    if (setPipedExist.getItemCount() == 1) {
      /* ENABLE_MIGRATION if */
      if (line.startsWith("NOT_MY_KEY ")) {
        this.setPipedExist.setNextOpIndex(index);
        receivedMigrateOperations(line, true);
      } else {
        OperationStatus status = matchStatus(line, EXIST, NOT_EXIST,
                NOT_FOUND, TYPE_MISMATCH, UNREADABLE);
        cb.gotStatus(index, status);
        cb.receivedStatus(status.isSuccess() ? END : FAILED_END);
        transitionState(OperationState.COMPLETE);
      }
      return;
      /* else */
      /*
      OperationStatus status = matchStatus(line, EXIST, NOT_EXIST,
          NOT_FOUND, TYPE_MISMATCH, UNREADABLE);
      cb.gotStatus(index, status);
      cb.receivedStatus(status.isSuccess() ? END : FAILED_END);
      transitionState(OperationState.COMPLETE);
      return;
      */
      /* ENABLE_MIGRATION end */
    }

    if (line.startsWith("END") || line.startsWith("PIPE_ERROR ")) {
      /* ENABLE_MIGRATION if */
      if (migrating) {
        transitionState(OperationState.MIGRATING);
        migrating = false;
      } else {
        cb.receivedStatus((successAll) ? END : FAILED_END);
        transitionState(OperationState.COMPLETE);
      }
      /* else */
      /*
      cb.receivedStatus((successAll) ? END : FAILED_END);
      transitionState(OperationState.COMPLETE);
      */
      /* ENABLE_MIGRATION end */
    /* ENABLE_MIGRATION if */
    } else if (line.startsWith("NOT_MY_KEY ")) {
      this.setPipedExist.setNextOpIndex(index);
      receivedMigrateOperations(line, false);
      migrating = true;
    /* ENABLE_MIGRATION end */
    } else if (line.startsWith("RESPONSE ")) {
      getLogger().debug("Got line %s", line);

      // TODO server should be fixed
      line = line.replace("   ", " ");
      line = line.replace("  ", " ");

      String[] stuff = line.split(" ");
      assert "RESPONSE".equals(stuff[0]);
      count = Integer.parseInt(stuff[1]);
    } else {
      OperationStatus status = matchStatus(line, EXIST, NOT_EXIST,
              NOT_FOUND, TYPE_MISMATCH, UNREADABLE);

      if (!status.isSuccess()) {
        successAll = false;
      }
      cb.gotStatus(index, status);
      /* ENABLE_REPLICATION if */
      this.setPipedExist.setNextOpIndex(index);
      /* ENABLE_REPLICATION end */
      index++;
    }
  }

  @Override
  public void initialize() {
    ByteBuffer buffer = setPipedExist.getAsciiCommand();
    setBuffer(buffer);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug(
              "Request in ascii protocol: \n"
                      + (new String(buffer.array())).replaceAll("\\r\\n",
                      "\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(EXIST_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public SetPipedExist<?> getExist() {
    return setPipedExist;
  }
}
