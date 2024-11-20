package net.spy.memcached;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.StoreOperation;

public final class ExceptionMessageFactory {

  private ExceptionMessageFactory() {
  }

  public static String createTimedOutMessage(long duration,
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

  public static String createCompositeMessage(List<Exception> exceptions) {
    if (exceptions == null || exceptions.isEmpty()) {
      throw new IllegalArgumentException("At least one exception must be specified");
    }

    StringBuilder rv = new StringBuilder();
    rv.append("Multiple exceptions (");
    rv.append(exceptions.size());
    rv.append(") reported: ");
    boolean first = true;
    for (Exception e : exceptions) {
      if (first) {
        first = false;
      } else {
        rv.append(", ");
      }
      rv.append(e.getMessage());
    }
    return rv.toString();

  }
}
