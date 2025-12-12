package net.spy.memcached.ops;

public interface MultiKeyPipedOperationCallback extends OperationCallback {
  void gotStatus(String key, OperationStatus status);
}
