package net.spy.memcached.ops;

public interface SingleKeyPipedOperationCallback extends OperationCallback {
  void gotStatus(Integer index, OperationStatus status);
}
