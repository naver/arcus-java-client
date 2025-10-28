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

import net.spy.memcached.auth.AuthException;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.SASLMechsOperation;
import net.spy.memcached.ops.StatusCode;

class SASLMechsOperationImpl extends OperationImpl implements
        SASLMechsOperation {

  private static final int CMD = 0x20;

  private static final OperationStatus SASL_NOT_SUPPORTED =
          new OperationStatus(true, "NOT_SUPPORTED", StatusCode.SUCCESS);

  private static final int SUCCESS = 0x00;
  private static final int UNKNOWN_COMMAND = 0x81;
  private static final int NOT_SUPPORTED = 0x83;

  private final boolean isInternal;

  public SASLMechsOperationImpl(boolean isInternal, OperationCallback cb) {
    super(CMD, generateOpaque(), cb);

    this.isInternal = isInternal;
  }

  @Override
  public void initialize() {
    prepareBuffer("", 0, EMPTY_BYTES);
  }

  @Override
  protected void finishedPayload(byte[] pl) throws IOException {
    if (errorCode == SUCCESS) {
      complete(new OperationStatus(true, new String(pl), StatusCode.SUCCESS));
    } else if (errorCode == NOT_SUPPORTED) {
      complete(SASL_NOT_SUPPORTED);
    } else if (isInternal) {
      if (errorCode == UNKNOWN_COMMAND) {
        complete(SASL_NOT_SUPPORTED);
      } else {
        String line = new String(pl);
        complete(new OperationStatus(false, line, StatusCode.fromAsciiLine(line)));
        throw new AuthException(line);
      }
    } else {
      super.finishedPayload(pl);
    }
  }

  @Override
  public boolean isIdempotentOperation() {
    return false;
  }

}
