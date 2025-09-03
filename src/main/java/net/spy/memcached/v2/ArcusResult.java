package net.spy.memcached.v2;

import java.util.List;

import net.spy.memcached.ops.OperationStatus;

public interface ArcusResult<T> {

  T get();

  void addError(OperationStatus status);

  void addError(String key, OperationStatus status);

  boolean hasError();

  List<Exception> getError();

}
