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
import net.spy.memcached.collection.BTreeFindPosition;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeFindPositionOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

public final class BTreeFindPositionOperationImpl extends OperationImpl implements
        BTreeFindPositionOperation {

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus POSITION = new CollectionOperationStatus(
          true, "POSITION", CollectionResponse.OK); // OK is arbitrary response
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

  private final String key;
  private final BTreeFindPosition get;

  public BTreeFindPositionOperationImpl(String key, BTreeFindPosition get,
                                        OperationCallback cb) {
    super(cb);
    this.key = key;
    this.get = get;
    setAPIType(APIType.BOP_POSITION);
    setOperationType(OperationType.READ);
  }

  @Override
  public BTreeFindPosition getGet() {
    return get;
  }

  @Override
  public void handleLine(String line) {
    getLogger().debug("Got line %s", line);

    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      addRedirectSingleKeyOperation(line, key);
      transitionState(OperationState.REDIRECT);
      return;
    }
    /* ENABLE_MIGRATION end */

    if (line.startsWith("POSITION=")) {
      String[] stuff = line.split("=");
      assert stuff.length == 2;
      assert "POSITION".equals(stuff[0]);

      // POSITION=<position>\r\n
      int position = Integer.parseInt(stuff[1]);
      BTreeFindPositionOperation.Callback cb =
              (BTreeFindPositionOperation.Callback) getCallback();
      cb.gotData(position);
      getCallback().receivedStatus(POSITION);
    } else {
      OperationStatus status = matchStatus(line, NOT_FOUND, UNREADABLE,
              BKEY_MISMATCH, TYPE_MISMATCH, NOT_FOUND_ELEMENT);
      getLogger().debug(status);
      getCallback().receivedStatus(status);
    }

    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    String cmd = get.getCommand();
    String args = get.stringify();

    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + cmd.length() + args.length() + 16);

    setArguments(bb, cmd, key, args);
    ((Buffer) bb).flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: %s",
              (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(GET_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
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
    return true;
  }

}
