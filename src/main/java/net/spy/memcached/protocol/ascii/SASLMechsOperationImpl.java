/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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

import java.io.IOException;
import java.nio.ByteBuffer;

import net.spy.memcached.auth.AuthException;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.SASLMechsOperation;
import net.spy.memcached.ops.StatusCode;

/**
 * Operation to request the list of supported SASL mechanisms.
 */
final class SASLMechsOperationImpl extends OperationImpl implements SASLMechsOperation {

  private static final byte[] MECHS_CMD = "sasl mech\r\n".getBytes();
  private static final String SASL_MECH_PREFIX = "SASL_MECH ";

  private static final OperationStatus SASL_NOT_SUPPORTED =
          new OperationStatus(true, "NOT_SUPPORTED", StatusCode.SUCCESS);

  private final boolean isInternal;

  public SASLMechsOperationImpl(boolean isInternal, OperationCallback cb) {
    super(cb);
    setAPIType(APIType.SASL_MECHS);
    setOperationType(OperationType.ETC);

    this.isInternal = isInternal;
  }

  @Override
  public void initialize() {
    setBuffer(ByteBuffer.wrap(MECHS_CMD));
  }

  @Override
  public void handleLine(String line) {
    /**
     * The server can respond with one of the following:
     *  - SASL_MECH {mech 1} {mech 2} {mech 3} ...\r\n
     *  - NOT_SUPPORTED\r\n
     */
    if (line.startsWith(SASL_MECH_PREFIX)) {
      String rawMechanisms = line.substring(SASL_MECH_PREFIX.length()).trim();
      String mechanisms = rawMechanisms.replaceAll("\\s+", " ").trim();
      complete(new OperationStatus(true, mechanisms, StatusCode.SUCCESS));
    } else if (line.startsWith("NOT_SUPPORTED")) {
      complete(SASL_NOT_SUPPORTED);
    } else {
      complete(new OperationStatus(false, line, StatusCode.fromAsciiLine(line)));
      if (isInternal) {
        throw new AuthException(line);
      }
    }
  }

  @Override
  protected void handleError(OperationErrorType eType, String line) throws IOException {
    if (isInternal && (line.startsWith("ERROR unknown command") ||
                       line.startsWith("ERROR no matching command"))) {
      complete(SASL_NOT_SUPPORTED);
    } else {
      super.handleError(eType, line);
    }
  }
}
