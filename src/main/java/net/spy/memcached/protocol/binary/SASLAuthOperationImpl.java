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

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.SASLAuthOperation;

public class SASLAuthOperationImpl extends SASLBaseOperationImpl
        implements SASLAuthOperation {

  private final static int CMD = 0x21;

  public SASLAuthOperationImpl(SaslClient sc, OperationCallback cb) {
    super(CMD, sc, EMPTY_BYTES, cb);
  }

  @Override
  protected byte[] buildResponse(SaslClient sc) throws SaslException {
    return sc.hasInitialResponse() ?
            sc.evaluateChallenge(challenge)
            : EMPTY_BYTES;

  }

}
