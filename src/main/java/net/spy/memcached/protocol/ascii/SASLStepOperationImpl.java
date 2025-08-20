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

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.SASLStepOperation;

/**
 * Operation to perform a single step of the SASL authentication process.
 */
final class SASLStepOperationImpl extends SASLBaseOperationImpl implements SASLStepOperation {

  private final byte[] challenge;

  public SASLStepOperationImpl(SaslClient sc, byte[] challenge, OperationCallback cb) {
    super(sc, cb);
    this.challenge = challenge;
    setAPIType(APIType.SASL_STEP);
  }

  @Override
  public void initialize() {
    try {
      /**
       * sasl auth {vlen}\r\n{value}\r\n
       */
      byte[] response = sc.evaluateChallenge(challenge);
      setSaslAuthBuffer(CMD, response);
    } catch (SaslException e) {
      getLogger().warn("Error while initializing SASL step: ", e);
      throw new RuntimeException("Error while initializing SASL step: ", e);
    }
  }
}
