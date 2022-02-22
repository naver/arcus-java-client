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
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.StatsOperation;
import net.spy.memcached.ops.StatusCode;

/**
 * Operation to retrieve statistics from a memcached server.
 */
final class StatsOperationImpl extends OperationImpl
        implements StatsOperation {

  private static final OperationStatus END =
          new OperationStatus(true, "END", StatusCode.SUCCESS);

  private static final byte[] MSG = "stats\r\n".getBytes();

  private final byte[] msg;
  private final StatsOperation.Callback cb;

  public StatsOperationImpl(String arg, StatsOperation.Callback c) {
    super(c);
    cb = c;
    if (arg == null) {
      msg = MSG;
    } else {
      msg = ("stats " + arg + "\r\n").getBytes();
    }
    setAPIType(APIType.STATS);
    setOperationType(OperationType.ETC);
  }

  @Override
  public void handleLine(String line) {
    if (line.startsWith("END")) {
      cb.receivedStatus(END);
      transitionState(OperationState.COMPLETE);
    } else {
      String[] parts = line.split(" ", 3);
      assert parts.length == 3;
      cb.gotStat(parts[1], parts[2]);
    }
  }

  @Override
  public void initialize() {
    setBuffer(ByteBuffer.wrap(msg));
  }

  @Override
  protected void wasCancelled() {
    cb.receivedStatus(CANCELLED);
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
    return true;
  }

}
