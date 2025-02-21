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

import net.spy.memcached.CASResponse;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CASOperation;
import net.spy.memcached.ops.CASOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.StoreType;

class CASOperationImpl extends OperationImpl implements CASOperation {

  // Overhead storage stuff to make sure the buffer pushes out far enough.
  // This is "cas" + length(flags) + length(length(data)) + length(cas id)
  // + spaces
  private static final int OVERHEAD = 64;

  private static final OperationStatus STORED =
          new CASOperationStatus(true, "STORED",
                  CASResponse.OK, StatusCode.SUCCESS);
  private static final OperationStatus NOT_FOUND =
          new CASOperationStatus(false, "NOT_FOUND",
                  CASResponse.NOT_FOUND, StatusCode.ERR_NOT_FOUND);
  private static final OperationStatus EXISTS =
          new CASOperationStatus(false, "EXISTS",
                  CASResponse.EXISTS, StatusCode.ERR_EXISTS);

  private final String key;
  private final long casValue;
  private final int flags;
  private final int exp;
  private final byte[] data;

  public CASOperationImpl(String k, long c, int f, int e,
                          byte[] d, OperationCallback cb) {
    super(cb);
    key = k;
    casValue = c;
    flags = f;
    exp = e;
    data = d;
    setAPIType(APIType.CAS);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
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
    getCallback().receivedStatus(matchStatus(line, STORED, NOT_FOUND, EXISTS));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    ByteBuffer bb = ByteBuffer.allocate(data.length
            + KeyUtil.getKeyBytes(key).length + OVERHEAD);
    setArguments(bb, "cas", key, flags, exp, data.length, casValue);
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

  public byte[] getBytes() {
    return data;
  }

  public long getCasValue() {
    return casValue;
  }

  public int getExpiration() {
    return exp;
  }

  public int getFlags() {
    return flags;
  }

  public StoreType getStoreType() {
    return StoreType.set;
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
