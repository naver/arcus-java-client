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
package net.spy.memcached.protocol.binary;

import java.io.IOException;

import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.StatsOperation;

public class StatsOperationImpl extends OperationImpl
        implements StatsOperation {

  private static final int CMD = 0x10;
  private final String key;

  public StatsOperationImpl(String arg, StatsOperation.Callback c) {
    super(CMD, generateOpaque(), c);
    key = (arg == null) ? "" : arg;
  }

  @Override
  public void initialize() {
    prepareBuffer(key, 0, EMPTY_BYTES);
  }

  @Override
  protected void finishedPayload(byte[] pl) throws IOException {
    if (keyLen > 0) {
      final byte[] keyBytes = new byte[keyLen];
      final byte[] data = new byte[pl.length - keyLen];
      System.arraycopy(pl, 0, keyBytes, 0, keyLen);
      System.arraycopy(pl, keyLen, data, 0, pl.length - keyLen);
      Callback cb = (Callback) getCallback();
      cb.gotStat(new String(keyBytes, "UTF-8"),
              new String(data, "UTF-8"));
    } else {
      getCallback().receivedStatus(STATUS_OK);
      transitionState(OperationState.COMPLETE);
    }
    resetInput();
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
