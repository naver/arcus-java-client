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
import net.spy.memcached.collection.BTreeMutate;
import net.spy.memcached.collection.CollectionMutate;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionMutateOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to incr/decr item value from collection in a memcached server.
 */
public class CollectionMutateOperationImpl extends OperationImpl implements
        CollectionMutateOperation {

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
          false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);
  private static final OperationStatus OVERFLOWED = new CollectionOperationStatus(
          false, "OVERFLOWED", CollectionResponse.OVERFLOWED);
  private static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
          false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);

  protected final String key;
  protected final String subkey;
  protected final CollectionMutate collectionMutate;

  public CollectionMutateOperationImpl(String key, String subkey,
                                       CollectionMutate collectionMutate, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.subkey = subkey;
    this.collectionMutate = collectionMutate;
    if (this.collectionMutate instanceof BTreeMutate) {
      if (((BTreeMutate) this.collectionMutate).getMutator() == Mutator.incr)
        setAPIType(APIType.BOP_INCR);
      else
        setAPIType(APIType.BOP_DECR);
    }
    setOperationType(OperationType.WRITE);
  }

  /**
   * <result value>\r\n
   */
  public void handleLine(String line) {

    OperationStatus status = null;

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
    try {
      Long.valueOf(line);
      getCallback().receivedStatus(new OperationStatus(true, line));
    } catch (NumberFormatException e) {
      status = matchStatus(line, NOT_FOUND, NOT_FOUND_ELEMENT, TYPE_MISMATCH, BKEY_MISMATCH,
              UNREADABLE, OVERFLOWED, OUT_OF_RANGE);

      getLogger().debug(status);
      getCallback().receivedStatus(status);
    }

    transitionState(OperationState.COMPLETE);
  }

  public void initialize() {
    String cmd = collectionMutate.getCommand();
    String args = collectionMutate.stringify();
    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + KeyUtil.getKeyBytes(subkey).length
            + cmd.length() + args.length() + 16);

    setArguments(bb, cmd, key, subkey, args);
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

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public String getSubKey() {
    return subkey;
  }

}
