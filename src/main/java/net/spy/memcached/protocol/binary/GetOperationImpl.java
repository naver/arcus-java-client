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

import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.OperationStatus;

class GetOperationImpl extends OperationImpl
        implements GetOperation, GetsOperation {

  static final int CMD = 0;

  /**
   * Length of the extra header stuff for a GET response.
   */
  static final int EXTRA_HDR_LEN = 4;

  private final String key;

  public GetOperationImpl(String k, GetOperation.Callback cb) {
    super(CMD, generateOpaque(), cb);
    key = k;
  }

  public GetOperationImpl(String k, GetsOperation.Callback cb) {
    super(CMD, generateOpaque(), cb);
    key = k;
  }

  @Override
  public void initialize() {
    prepareBuffer(key, 0, EMPTY_BYTES);
  }

  @Override
  protected void decodePayload(byte[] pl) {
    final int flags = decodeInt(pl, 0);
    final byte[] data = new byte[pl.length - EXTRA_HDR_LEN];
    System.arraycopy(pl, EXTRA_HDR_LEN, data, 0, pl.length - EXTRA_HDR_LEN);
    // Assume we're processing a get unless the cast fails.
    try {
      GetOperation.Callback cb = (GetOperation.Callback) getCallback();
      cb.gotData(key, flags, data);
    } catch (ClassCastException e) {
      GetsOperation.Callback cb = (GetsOperation.Callback) getCallback();
      cb.gotData(key, flags, responseCas, data);
    }
    complete(STATUS_OK);
  }

  @Override
  protected OperationStatus getStatusForErrorCode(int errCode, byte[] errPl) {
    return errCode == ERR_NOT_FOUND ? NOT_FOUND_STATUS : null;
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

}
