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

import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.SASLStepOperation;

public class SASLStepOperationImpl extends SASLBaseOperationImpl
        implements SASLStepOperation {

  private final static int CMD = 0x22;

  public SASLStepOperationImpl(String[] m, byte[] ch, String s,
                               Map<String, ?> p, CallbackHandler h, OperationCallback c) {
    super(CMD, m, ch, s, p, h, c);
  }

  @Override
  protected byte[] buildResponse(SaslClient sc) throws SaslException {
    return sc.evaluateChallenge(challenge);
  }

  @Override
  public boolean isIdempotentOperation() {
    return false;
  }

}
