package net.spy.memcached.ops;

public interface PipedOperationCallback extends OperationCallback {
  void gotStatus(Integer index, OperationStatus status);
}
