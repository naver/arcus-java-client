/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2021 JaM2in Co., Ltd.
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
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.StatusCode;

/**
 * Base class for ascii store operations (add, set, replace, append, prepend).
 */
abstract class BaseStoreOperationImpl extends OperationImpl {

  private static final int OVERHEAD = 64;

  private static final OperationStatus STORED =
          new OperationStatus(true, "STORED", StatusCode.SUCCESS);
  private static final OperationStatus NOT_FOUND =
          new OperationStatus(false, "NOT_FOUND", StatusCode.ERR_NOT_FOUND);
  private static final OperationStatus EXISTS =
          new OperationStatus(false, "EXISTS", StatusCode.ERR_EXISTS);

  protected final String type;
  protected final String key;
  protected final int flags;
  protected final int exp;
  protected final byte[] data;

  public BaseStoreOperationImpl(String t, String k, int f, int e,
                                byte[] d, OperationCallback cb) {
    super(cb);
    type = t;
    key = k;
    flags = f;
    exp = e;
    data = d;
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
    /* ENABLE_REPLICATION if */
    if (hasSwitchedOver(line)) {
      receivedMoveOperations(line);
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
    getCallback().receivedStatus(matchStatus(line, STORED, NOT_FOUND, EXISTS));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    ByteBuffer bb = ByteBuffer.allocate(data.length
            + KeyUtil.getKeyBytes(key).length + OVERHEAD);
    setArguments(bb, type, key, flags, exp, data.length);
    assert bb.remaining() >= data.length + 2
            : "Not enough room in buffer, need another "
            + (2 + data.length - bb.remaining());
    bb.put(data);
    bb.put(CRLF);
    ((Buffer) bb).flip();
    setBuffer(bb);
  }

  @Override
  protected void wasCancelled() {
    // XXX:  Replace this comment with why I did this
    getCallback().receivedStatus(CANCELLED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public int getFlags() {
    return flags;
  }

  public int getExpiration() {
    return exp;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public boolean isBulkOperation() {
    return false;
  }

  @Override
  public boolean isPipeOperation() {
    return false;
  }
}
