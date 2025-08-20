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
import net.spy.memcached.ops.SASLAuthOperation;

/**
 * Operation to initiate the SASL authentication process.
 */
final class SASLAuthOperationImpl extends SASLBaseOperationImpl
        implements SASLAuthOperation {

  public SASLAuthOperationImpl(SaslClient sc, OperationCallback cb) {
    super(sc, cb);
    setAPIType(APIType.SASL_AUTH);
  }

  @Override
  public void initialize() {
    try {
      /**
       * sasl auth {mech} {vlen}\r\n{value}\r\n
       */
      String mechanism = sc.getMechanismName();
      byte[] response = sc.hasInitialResponse() ? sc.evaluateChallenge(new byte[0]) : new byte[0];
      String command = CMD + mechanism + " ";
      setSaslAuthBuffer(command, response);
    } catch (SaslException e) {
      getLogger().error("Error while initializing SASL auth: ", e);
      throw new RuntimeException("Error while initializing SASL auth: ", e);
    }
  }
}
