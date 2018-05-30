/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
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
import net.spy.memcached.ops.FlushOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Arcus flush by prefix operation.
 */
final class FlushByPrefixOperationImpl extends OperationImpl implements
        FlushOperation {

  private static final OperationStatus OK = new OperationStatus(true, "OK");
  private static final OperationStatus NOT_FOUND = new OperationStatus(false, "NOT_FOUND");

  private final String prefix;
  private final int delay;
  private final boolean noreply;

  public FlushByPrefixOperationImpl(String prefix, int delay,
                                    boolean noreply, OperationCallback cb) {
    super(cb);
    this.prefix = prefix;
    this.delay = delay;
    this.noreply = noreply;
    setAPIType(APIType.FLUSH);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    getLogger().debug("Flush completed successfully");
    /* ENABLE_REPLICATION if */
    if (line.equals("SWITCHOVER") || line.equals("REPL_SLAVE")) {
      receivedMoveOperations(line);
      return;
    }

    /* ENABLE_REPLICATION end */
    getCallback().receivedStatus(matchStatus(line, OK, NOT_FOUND));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    StringBuilder sb = new StringBuilder();
    sb.append("flush_prefix ");
    sb.append(prefix);
    if (delay != -1)
      sb.append(" ").append(delay);
    if (noreply)
      sb.append(" noreply");
    sb.append("\r\n");

    ByteBuffer bb = ByteBuffer.allocate(sb.length());
    bb.put(sb.toString().getBytes());
    bb.flip();
    setBuffer(bb);
  }
}
