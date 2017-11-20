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
import net.spy.memcached.collection.BTreeDelete;
import net.spy.memcached.collection.CollectionDelete;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ListDelete;
import net.spy.memcached.collection.SetDelete;
import net.spy.memcached.collection.MapDelete;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionDeleteOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to delete collection data in a memcached server.
 */
public class CollectionDeleteOperationImpl extends OperationImpl
        implements CollectionDeleteOperation {

  private static final OperationStatus DELETE_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus DELETED = new CollectionOperationStatus(
          true, "DELETED", CollectionResponse.DELETED);
  private static final OperationStatus DELETED_DROPPED = new CollectionOperationStatus(
          true, "DELETED_DROPPED", CollectionResponse.DELETED_DROPPED);
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

  protected String key;
  protected CollectionDelete collectionDelete;

  public CollectionDeleteOperationImpl(String key,
                                       CollectionDelete collectionDelete, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.collectionDelete = collectionDelete;
    if (this.collectionDelete instanceof ListDelete)
      setAPIType(APIType.LOP_DELETE);
    else if (this.collectionDelete instanceof SetDelete)
      setAPIType(APIType.SOP_DELETE);
    else if (this.collectionDelete instanceof MapDelete)
      setAPIType(APIType.MOP_DELETE);
    else if (this.collectionDelete instanceof BTreeDelete)
      setAPIType(APIType.BOP_DELETE);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
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
    OperationStatus status = matchStatus(line, DELETED, DELETED_DROPPED,
            NOT_FOUND, NOT_FOUND_ELEMENT, OUT_OF_RANGE, TYPE_MISMATCH,
            BKEY_MISMATCH);
    getCallback().receivedStatus(status);
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    String cmd = collectionDelete.getCommand();
    if (collectionDelete instanceof MapDelete) {
      MapDelete mapDelete = (MapDelete)collectionDelete;
      if (getHandlingNode() == null || getHandlingNode().enabledSpaceSeparate()) {
        mapDelete.setKeySeparator(" ");
      } else {
        mapDelete.setKeySeparator(",");
      }
    }
    String args = collectionDelete.stringify();
    byte[] additionalArgs = collectionDelete.getAdditionalArgs();

    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + cmd.length() + args.length() +
            ((null == additionalArgs) ? 0 : additionalArgs.length) + 16);

    setArguments(bb, cmd, key, args);

    if (null != additionalArgs) {
      bb.put(additionalArgs);
      bb.put(CRLF);
    }

    bb.flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: "
              + (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(DELETE_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public CollectionDelete getDelete() {
    return collectionDelete;
  }

}
