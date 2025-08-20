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

import java.nio.ByteBuffer;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.OperationCallback;
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

  public SASLMechsOperationImpl(OperationCallback cb) {
    super(cb);
    setAPIType(APIType.SASL_MECHS);
    setOperationType(OperationType.ETC);
  }

  @Override
  public void initialize() {
    setBuffer(ByteBuffer.wrap(MECHS_CMD));
  }

  @Override
  public void handleLine(String line) {
    /**
     * SASL_MECH SCRAM-SHA-256\r\n
     */
    if (line.startsWith(SASL_MECH_PREFIX)) {
      String rawMechanisms = line.substring(SASL_MECH_PREFIX.length()).trim();
      String mechanisms = rawMechanisms.replaceAll("\\s+", " ").trim();
      complete(new OperationStatus(true, mechanisms, StatusCode.SUCCESS));
    } else {
      complete(new OperationStatus(false, line, StatusCode.fromAsciiLine(line)));
    }
  }
}
