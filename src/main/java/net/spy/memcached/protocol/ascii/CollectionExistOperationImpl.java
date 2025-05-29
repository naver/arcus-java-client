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
import net.spy.memcached.collection.CollectionExist;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.SetExist;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionExistOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to check membership of an item in collection in a memcached server.
 */
public final class CollectionExistOperationImpl extends OperationImpl
        implements CollectionExistOperation {

  private static final int OVERHEAD = 32;

  private static final OperationStatus EXIST = new CollectionOperationStatus(
          true, "EXIST", CollectionResponse.EXIST);
  private static final OperationStatus NOT_EXIST = new CollectionOperationStatus(
          true, "NOT_EXIST", CollectionResponse.NOT_EXIST);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);

  private final String key;
  private final String subkey;
  private final CollectionExist collectionExist;

  public CollectionExistOperationImpl(String key, String subkey,
                                      CollectionExist collectionExist, OperationCallback cb) {
    super(cb);
    this.key = key;
    this.subkey = subkey;
    this.collectionExist = collectionExist;
    if (this.collectionExist instanceof SetExist) {
      setAPIType(APIType.SOP_EXIST);
    }
    setOperationType(OperationType.READ);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      addRedirectSingleKeyOperation(line, key);
      transitionState(OperationState.REDIRECT);
      return;
    }
    /* ENABLE_MIGRATION end */
    OperationStatus status = matchStatus(line, EXIST, NOT_EXIST,
            NOT_FOUND, TYPE_MISMATCH, UNREADABLE);
    complete(status);
  }

  @Override
  public void initialize() {
    String args = collectionExist.stringify();
    byte[] additionalArgs = collectionExist.getAdditionalArgs();
    ByteBuffer bb = ByteBuffer.allocate((null == additionalArgs ? 0 : additionalArgs.length)
            + KeyUtil.getKeyBytes(key).length
            + KeyUtil.getKeyBytes(subkey).length
            + args.length()
            + OVERHEAD);
    setArguments(bb, collectionExist.getCommand(), key, subkey, args);

    if (null != additionalArgs) {
      bb.put(additionalArgs);
      bb.put(CRLF);
    }

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

  public CollectionExist getExist() {
    return collectionExist;
  }

}
