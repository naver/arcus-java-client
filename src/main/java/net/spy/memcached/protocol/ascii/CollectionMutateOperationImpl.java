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
public final class CollectionMutateOperationImpl extends OperationImpl implements
        CollectionMutateOperation {

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

  private final String key;
  private final String subkey;
  private final CollectionMutate collectionMutate;

  public CollectionMutateOperationImpl(String key, String subkey,
                                       CollectionMutate collectionMutate, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.subkey = subkey;
    this.collectionMutate = collectionMutate;
    if (this.collectionMutate instanceof BTreeMutate) {
      if (((BTreeMutate) this.collectionMutate).getMutator() == Mutator.incr) {
        setAPIType(APIType.BOP_INCR);
      } else {
        setAPIType(APIType.BOP_DECR);
      }
    }
    setOperationType(OperationType.WRITE);
  }

  public void handleLine(String line) {

    /* ENABLE_REPLICATION if */
    if (hasSwitchedOver(line)) {
      prepareSwitchover(line);
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

    // <result value>\r\n
    boolean allDigit = line.chars().allMatch(Character::isDigit);
    OperationStatus status;
    if (allDigit) {
      status = new CollectionOperationStatus(true, line, CollectionResponse.END);
    } else {
      status = matchStatus(line, NOT_FOUND, NOT_FOUND_ELEMENT,
              UNREADABLE, OVERFLOWED, OUT_OF_RANGE,
              TYPE_MISMATCH, BKEY_MISMATCH);

      getLogger().debug(status);
    }

    getCallback().receivedStatus(status);
    transitionState(OperationState.COMPLETE);
  }

  public void initialize() {
    String cmd = collectionMutate.getCommand();
    String args = collectionMutate.stringify();
    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + KeyUtil.getKeyBytes(subkey).length
            + cmd.length() + args.length() + 16);

    setArguments(bb, cmd, key, subkey, args);
    ((Buffer) bb).flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: %s",
              (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public String getSubKey() {
    return subkey;
  }

  @Override
  public boolean isIdempotentOperation() {
    return false;
  }

}
