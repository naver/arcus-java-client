package net.spy.memcached.v2;

import java.util.List;

import net.spy.memcached.ops.OperationStatus;

/**
 * Class to hold the result and errors occurred during Arcus operation.
 * @param <T> the type of the result
 */
public interface ArcusResult<T> {

  T get();

  void set(T value);

  void addError(String key, OperationStatus status);

  boolean hasError();

  List<Exception> getError();

}
