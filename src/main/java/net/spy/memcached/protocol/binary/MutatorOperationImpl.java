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

import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

class MutatorOperationImpl extends OperationImpl implements
        MutatorOperation {

  private static final int CMD_INCR = 5;
  private static final int CMD_DECR = 6;

  private final Mutator mutator;
  private final String key;
  private final long by;
  private final int exp;
  private final long def;

  public MutatorOperationImpl(Mutator m, String k, long b,
                              long d, int e, OperationCallback cb) {
    super(m == Mutator.incr ? CMD_INCR : CMD_DECR, generateOpaque(), cb);
    assert d >= 0 : "Default value is below zero";
    mutator = m;
    key = k;
    by = b;
    exp = e;
    def = d;
  }

  @Override
  public void initialize() {
    // We're passing around a long so we can cover an unsigned integer.
    byte[] defBytes = new byte[8];
    defBytes[0] = (byte) ((def >> 56) & 0xff);
    defBytes[1] = (byte) ((def >> 48) & 0xff);
    defBytes[2] = (byte) ((def >> 40) & 0xff);
    defBytes[3] = (byte) ((def >> 32) & 0xff);
    defBytes[4] = (byte) ((def >> 24) & 0xff);
    defBytes[5] = (byte) ((def >> 16) & 0xff);
    defBytes[6] = (byte) ((def >> 8) & 0xff);
    defBytes[7] = (byte) (def & 0xff);
    prepareBuffer(key, 0, EMPTY_BYTES, by, defBytes, exp);
  }

  @Override
  protected OperationStatus getStatusForErrorCode(int errCode, byte[] errPl) {
    return errCode == ERR_NOT_FOUND ? NOT_FOUND_STATUS : null;
  }

  @Override
  protected void decodePayload(byte[] pl) {
    getCallback().receivedStatus(new OperationStatus(true,
            String.valueOf(decodeLong(pl, 0)), StatusCode.SUCCESS));
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public int getBy() {
    return (int) by;
  }

  public long getDefault() {
    return def;
  }

  public int getExpiration() {
    return exp;
  }

  public Mutator getType() {
    return mutator;
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
    return false;
  }

}
