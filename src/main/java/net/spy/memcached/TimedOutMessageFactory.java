package net.spy.memcached;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.StoreOperation;

public final class TimedOutMessageFactory {

  private TimedOutMessageFactory() {
  }

  public static String createTimedoutMessage(long duration,
                                             TimeUnit unit,
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
    rv.append(" operation timed out (>").append(duration);
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
