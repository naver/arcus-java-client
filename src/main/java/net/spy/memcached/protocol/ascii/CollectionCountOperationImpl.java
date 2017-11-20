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
import net.spy.memcached.collection.BTreeCount;
import net.spy.memcached.collection.CollectionCount;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionCountOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to get exists item count from collection in a memcached server.
 */
public class CollectionCountOperationImpl extends OperationImpl implements
        CollectionCountOperation {

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);

  protected final String key;
  protected final CollectionCount collectionCount;

  protected int count = 0;

  public CollectionCountOperationImpl(String key,
                                      CollectionCount collectionCount, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.collectionCount = collectionCount;
    if (this.collectionCount instanceof BTreeCount)
      setAPIType(APIType.BOP_COUNT);
    setOperationType(OperationType.READ);
  }

  /**
   * VALUE <flag> <count>\r\n
   */
  public void handleLine(String line) {
    if (line.startsWith("COUNT=")) {
      getLogger().debug("Got line %s", line);

      String[] stuff = line.split("=");
      assert "COUNT".equals(stuff[0]);
      count = Integer.parseInt(stuff[1]);

      getCallback().receivedStatus(
              new CollectionOperationStatus(new OperationStatus(true,
                      String.valueOf(count))));
      transitionState(OperationState.COMPLETE);
    /* ENABLE_MIGRATION if */
    } else if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, true);
    /* ENABLE_MIGRATION end */
    } else {
      OperationStatus status = matchStatus(line, NOT_FOUND, TYPE_MISMATCH, BKEY_MISMATCH, UNREADABLE);
      getLogger().debug(status);
      getCallback().receivedStatus(status);
      transitionState(OperationState.COMPLETE);
      return;
    }
  }

  public void initialize() {
    String cmd = collectionCount.getCommand();
    String args = collectionCount.stringify();
    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + cmd.length() + args.length() + 16);

    setArguments(bb, cmd, key, args);
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
}
