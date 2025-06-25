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
package net.spy.memcached.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.StoreOperation;

/**
 * Timeout exception that tracks the original operation.
 */
public class CheckedOperationTimeoutException extends TimeoutException {

  private static final long serialVersionUID = 5187393339735774489L;
  private final ArrayList<Operation> operations;

  public CheckedOperationTimeoutException(long duration,
                                          TimeUnit unit,
                                          long elapsed,
                                          Operation op) {
    this(duration, unit, elapsed, new ArrayList<>(Collections.singleton(op)));
  }

  public CheckedOperationTimeoutException(long duration,
                                          TimeUnit unit,
                                          long elapsed,
                                          Collection<Operation> ops) {
    super(createMessage(duration, unit, elapsed, ops));
    operations =  new ArrayList<>(ops);
  }

  /**
   * Get the operation that timed out.
   */
  public List<Operation> getOperations() {
    return operations;
  }

  private static String createMessage(long duration,
                                     TimeUnit unit,
                                     long elapsed,
                                     Collection<Operation> ops) {

    StringBuilder rv = new StringBuilder();
    Operation firstOp = ops.iterator().next();
    if (isBulkOperation(firstOp, ops)) {
      rv.append("bulk ");
    }
    if (firstOp.isPipeOperation()) {
      rv.append("pipe ");
    }

    rv.append(firstOp.getAPIType());
    rv.append(" operation timed out (");
    rv.append(unit.convert(elapsed, TimeUnit.MILLISECONDS));
    rv.append(" >= ").append(duration);
    rv.append(" ").append(unit).append(") - ");

    rv.append("failing node");
    rv.append(ops.size() == 1 ? ": " : "s: ");

    boolean first = true;
    for (Operation op : ops) {
      if (first) {
        first = false;
      } else {
        rv.append(", ");
      }
      MemcachedNode node = op == null ? null : op.getHandlingNode();
      rv.append(node == null ? "<unknown>" : node.getNodeName());
      if (op != null) {
        rv.append(" [").append(op.getState()).append("]");
      }
      if (node != null) {
        rv.append(" [").append(node.getOpQueueStatus()).append("]");
        if (!node.isActive() && node.isFirstConnecting()) {
          rv.append(" (Not connected yet)");
        }
      }
    }
    return rv.toString();
  }

  /**
   * check bulk operation or not
   * @param op operation
   * @param ops number of operation (used in StoreOperationImpl, DeleteOperationImpl)
   */
  private static boolean isBulkOperation(Operation op, Collection<Operation> ops) {
    if (op instanceof StoreOperation || op instanceof DeleteOperation) {
      return ops.size() > 1;
    }
    return op.isBulkOperation();
  }
}
