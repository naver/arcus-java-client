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
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.FlushOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.StatusCode;

/**
 * Memcached flush_all operation.
 */
final class FlushOperationImpl extends OperationImpl
        implements FlushOperation {

  private static final byte[] FLUSH = "flush_all\r\n".getBytes();

  private static final OperationStatus OK =
          new OperationStatus(true, "OK", StatusCode.SUCCESS);

  private final int delay;

  public FlushOperationImpl(int d, OperationCallback cb) {
    super(cb);
    delay = d;
    setAPIType(APIType.FLUSH);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    getLogger().debug("Flush completed successfully");
    /* ENABLE_REPLICATION if */
    if (hasSwitchedOver(line)) {
      receivedMoveOperations(line);
      return;
    }
    /* ENABLE_REPLICATION end */
    getCallback().receivedStatus(matchStatus(line, OK));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    ByteBuffer b = null;
    if (delay == -1) {
      b = ByteBuffer.wrap(FLUSH);
    } else {
      b = ByteBuffer.allocate(32);
      b.put(("flush_all " + delay + "\r\n").getBytes());
      b.flip();
    }
    setBuffer(b);
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

}
