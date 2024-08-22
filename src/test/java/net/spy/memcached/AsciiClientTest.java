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
package net.spy.memcached;

import java.nio.ByteBuffer;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.protocol.ascii.ExtensibleOperationImpl;

import org.junit.jupiter.api.Test;

/**
 * This test assumes a client is running on localhost:11211.
 */
public class AsciiClientTest extends ProtocolBaseCase {

  @Test
  public void testBadOperation() throws Exception {
    client.addOp("x", new ExtensibleOperationImpl(new OperationCallback() {
      public void complete() {
        System.err.println("Complete.");
      }

      public void receivedStatus(OperationStatus s) {
        System.err.println("Received a line.");
      }
    }) {

      @Override
      public void handleLine(String line) {
        System.out.println("Woo! A line!");
      }

      @Override
      public void initialize() {
        setBuffer(ByteBuffer.wrap("garbage\r\n".getBytes()));
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
    });
  }

  @Override
  protected String getExpectedVersionSource() {
    return "/" + ARCUS_HOST;
  }

}
