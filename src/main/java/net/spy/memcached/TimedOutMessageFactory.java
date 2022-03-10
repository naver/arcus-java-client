package net.spy.memcached;

import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.DeleteOperation;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public final class TimedOutMessageFactory {

  private TimedOutMessageFactory() {
  }

  public static String createTimedoutMessage(long duration,
                                             TimeUnit units,
                                             Collection<Operation> ops) {
    StringBuilder rv = new StringBuilder();
    Operation firstOp = ops.iterator().next();
    if (isBulkOperation(firstOp, ops)) {
      rv.append("bulk ");
    }
    if (firstOp.isPipeOperation()) {
      rv.append("pipe ");
    }
    rv.append(firstOp.getAPIType())
      .append(" operation timed out (>").append(duration)
      .append(" ").append(units).append(")");
    return createMessage(rv.toString(), ops);
  }

  /**
   * check bulk operation or not
   * @param op operation
   * @param ops number of operation (used in GetOpeartion, StoreOperationImpl, DeleteOperationImpl)
   */
  private static boolean isBulkOperation(Operation op, Collection<Operation> ops) {
    if (op instanceof GetOperation) {
      GetOperation gop = (GetOperation) op;
      return gop.getKeys().size() > 1;
    } else if (op instanceof StoreOperation) {
      return ops.size() > 1;
    } else if (op instanceof DeleteOperation) {
      return ops.size() > 1;
    }
    return op.isBulkOperation();
  }

  public static String createMessage(String message,
                                     Collection<Operation> ops) {
    StringBuilder rv = new StringBuilder(message);
    rv.append(" - failing node");
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
        rv.append(" [").append(node.getStatus()).append("]");
      }
    }
    return rv.toString();
  }

}
