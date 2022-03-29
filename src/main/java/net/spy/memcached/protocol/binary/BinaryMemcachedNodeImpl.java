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

import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import net.spy.memcached.ops.CASOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.protocol.ProxyCallback;
import net.spy.memcached.protocol.TCPMemcachedNodeImpl;

/**
 * Implementation of MemcachedNode for speakers of the binary protocol.
 */
public class BinaryMemcachedNodeImpl extends TCPMemcachedNodeImpl {

  private final int MAX_SET_OPTIMIZATION_COUNT = 65535;
  private final int MAX_SET_OPTIMIZATION_BYTES = 2 * 1024 * 1024;

  public BinaryMemcachedNodeImpl(String name,
                                 SocketAddress sa,
                                 int bufSize, BlockingQueue<Operation> rq,
                                 BlockingQueue<Operation> wq, BlockingQueue<Operation> iq,
                                 Long opQueueMaxBlockTimeNs, boolean waitForAuth) {
    super(name, sa, bufSize, rq, wq, iq, opQueueMaxBlockTimeNs,
            waitForAuth, false /* binary protocol */);
  }

  @Override
  protected void optimize() {
    // MGetOperation does not support binary protocol
    Operation firstOp = writeQ.peek();
    if (firstOp instanceof GetOperation) {
      optimizeGets();
    } else if (firstOp instanceof CASOperation) {
      optimizeSets();
    }
  }

  private void optimizeGets() {
    // make sure there are at least two get operations in a row before
    // attempting to optimize them.
    optimizedOp = writeQ.remove();
    if (writeQ.peek() instanceof GetOperation) {
      OptimizedGetImpl og = new OptimizedGetImpl(
              (GetOperation) optimizedOp);
      optimizedOp = og;

      while (writeQ.peek() instanceof GetOperation) {
        GetOperation o = (GetOperation) writeQ.remove();
        if (!o.isCancelled()) {
          og.addOperation(o);
        }
      }

      // Initialize the new mega get
      optimizedOp.initialize();
      assert optimizedOp.getState() == OperationState.WRITE_QUEUED;
      ProxyCallback pcb = (ProxyCallback) og.getCallback();
      getLogger().debug("Set up %s with %s keys and %s callbacks",
              this, pcb.numKeys(), pcb.numCallbacks());
    }
  }

  private void optimizeSets() {
    // make sure there are at least two get operations in a row before
    // attempting to optimize them.
    optimizedOp = writeQ.remove();
    if (writeQ.peek() instanceof CASOperation) {
      OptimizedSetImpl og = new OptimizedSetImpl(
              (CASOperation) optimizedOp);
      optimizedOp = og;

      while (writeQ.peek() instanceof StoreOperation
              && og.size() < MAX_SET_OPTIMIZATION_COUNT
              && og.bytes() < MAX_SET_OPTIMIZATION_BYTES) {
        CASOperation o = (CASOperation) writeQ.remove();
        if (!o.isCancelled()) {
          og.addOperation(o);
        }
      }

      // Initialize the new mega set
      optimizedOp.initialize();
      assert optimizedOp.getState() == OperationState.WRITE_QUEUED;
    }
  }
}
