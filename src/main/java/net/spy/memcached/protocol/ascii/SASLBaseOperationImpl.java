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

import javax.security.sasl.SaslClient;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.StatusCode;

/**
 * Base class for ascii SASL operation handlers.
 */
abstract class SASLBaseOperationImpl extends OperationImpl {

  protected static final String CMD = "sasl auth ";
  private static final String RN_STRING = "\r\n";

  protected static final OperationStatus SASL_OK =
          new OperationStatus(true, "SASL_OK", StatusCode.SUCCESS);

  protected final SaslClient sc;
  private byte[] challenge;
  private int readOffset = 0;
  private int challengeLength = -1;

  public SASLBaseOperationImpl(SaslClient sc, OperationCallback cb) {
    super(cb);
    this.sc = sc;
    setOperationType(OperationType.ETC);
  }

  protected void setSaslAuthBuffer(String command, byte[] data) {
    String header = command + data.length + RN_STRING;
    byte[] headerBytes = header.getBytes();
    byte[] terminatorBytes = RN_STRING.getBytes();

    ByteBuffer b = ByteBuffer.allocate(headerBytes.length + data.length + terminatorBytes.length);
    b.put(headerBytes);
    b.put(data);
    b.put(terminatorBytes);

    b.flip();
    setBuffer(b);
  }

  @Override
  public void handleLine(String line) {
    /**
     * The server can respond with one of the following:
     *  - SASL_CONTINUE {vlen}\r\n{value}\r\n
     *  - SASL_OK\r\n
     */
    if (line.startsWith("SASL_CONTINUE")) {
      challengeLength = Integer.parseInt(line.substring("SASL_CONTINUE".length()).trim());
      challenge = new byte[challengeLength];
      readOffset = 0;
      setReadType(OperationReadType.DATA);
    } else if (line.startsWith("SASL_OK")) {
      complete(matchStatus(line, SASL_OK));
    } else {
      complete(new OperationStatus(false, line, StatusCode.ERR_AUTH));
    }
  }

  @Override
  public void handleRead(ByteBuffer buffer) {
    if (readOffset < challengeLength) {
      int toRead = Math.min(challengeLength - readOffset, buffer.remaining());
      buffer.get(challenge, readOffset, toRead);
      readOffset += toRead;
    }

    byte found = 0;
    while (found != '\n' && buffer.hasRemaining()) {
      found = buffer.get();
    }
    if (found == '\n') {
      complete(new OperationStatus(true, new String(challenge), StatusCode.SUCCESS));
    }
  }
}
