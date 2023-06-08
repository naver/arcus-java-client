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
package net.spy.memcached.protocol.ascii;

import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.TCPMemcachedNodeImpl;

/**
 * Memcached node for the ASCII protocol.
 */
public final class AsciiMemcachedNodeImpl extends TCPMemcachedNodeImpl {
  public AsciiMemcachedNodeImpl(String name,
                                SocketAddress sa,
                                int bufSize, BlockingQueue<Operation> rq,
                                BlockingQueue<Operation> wq, BlockingQueue<Operation> iq,
                                Long opQueueMaxBlockTimeNs) {
    super(name, sa, bufSize, rq, wq, iq, opQueueMaxBlockTimeNs,
        false /* ascii never does auth */, true /* ascii protocol */);
  }

  @Override
  protected void optimize() {
    // GetOperation이 여러개 입력될 경우 async처리가 안되는 bulk로 변경되는
    // 기능으로 인해 asyncGet이 의도한대로 수행되지 않아 주석처리
    // make sure there are at least two get operations in a row before
    // attempting to optimize them.
    /* Operation nxtOp = writeQ.peek();
    if (nxtOp instanceof GetOperation && nxtOp.getAPIType() != APIType.MGET) {
      optimizedOp = writeQ.remove();
      nxtOp = writeQ.peek();
      if (nxtOp instanceof GetOperation && nxtOp.getAPIType() != APIType.MGET) {
        OptimizedGetImpl og = new OptimizedGetImpl(
                (GetOperation) optimizedOp);
        optimizedOp = og;

        do {
          GetOperationImpl o = (GetOperationImpl) writeQ.remove();
          if (!o.isCancelled()) {
            og.addOperation(o);
          }
          nxtOp = writeQ.peek();
        } while (nxtOp instanceof GetOperation &&
                nxtOp.getAPIType() != APIType.MGET);

        // Initialize the new mega get
        optimizedOp.initialize();
        assert optimizedOp.getState() == OperationState.WRITE_QUEUED;
        ProxyCallback pcb = (ProxyCallback) og.getCallback();
        getLogger().debug("Set up %s with %s keys and %s callbacks",
                this, pcb.numKeys(), pcb.numCallbacks());
      }
    }*/
  }

}
