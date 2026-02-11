package net.spy.memcached.ops;

import java.util.List;

public interface PipelineOperation extends KeyedOperation {

  List<KeyedOperation> getOps();

  interface Callback extends OperationCallback {
    void gotStatus(Operation op, OperationStatus status);
  }
}
