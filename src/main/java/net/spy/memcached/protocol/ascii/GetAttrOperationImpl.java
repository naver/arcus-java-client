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
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.GetAttrOperation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Implementation of the gets operation.
 */
class GetAttrOperationImpl extends OperationImpl implements GetAttrOperation {

  private static final String CMD = "getattr";

  private static final OperationStatus ATTR_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus ATTR_ERROR_NOT_FOUND = new CollectionOperationStatus(
          false, "ATTR_ERROR not found",
          CollectionResponse.ATTR_ERROR_NOT_FOUND);

  protected final String key;
  protected final GetAttrOperation.Callback cb;

  public GetAttrOperationImpl(String key, GetAttrOperation.Callback cb) {
    super(cb);
    this.key = key;
    this.cb = cb;
    setAPIType(APIType.GETATTR);
    setOperationType(OperationType.READ);
  }

  @Override
  public void handleLine(String line) {
    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      addRedirectSingleKeyOperation(line, key);
      transitionState(OperationState.REDIRECT);
      return;
    }
    /* ENABLE_MIGRATION end */
    if (line.startsWith("ATTR ")) {
      getLogger().debug("Got line %s", line);

      String[] stuff = line.split(" ");

      assert stuff.length == 2;
      assert stuff[0].equals("ATTR");

      cb.gotAttribute(key, stuff[1]);
    } else {
      OperationStatus status = matchStatus(line, END, NOT_FOUND,
              ATTR_ERROR_NOT_FOUND);
      getLogger().debug(status);
      getCallback().receivedStatus(status);
      transitionState(OperationState.COMPLETE);
    }
  }

  @Override
  public void initialize() {
    int size = CMD.length() + KeyUtil.getKeyBytes(key).length + 16;
    ByteBuffer bb = ByteBuffer.allocate(size);
    setArguments(bb, CMD, key);
    ((Buffer) bb).flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: %s",
              (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(ATTR_CANCELED);
  }

  @Override
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
