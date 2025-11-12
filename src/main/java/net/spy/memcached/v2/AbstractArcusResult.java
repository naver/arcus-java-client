package net.spy.memcached.v2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationStatus;

public class AbstractArcusResult<T> implements ArcusResult<T> {

  protected AtomicReference<T> value;

  private final List<Exception> exceptions = new ArrayList<>();

  public AbstractArcusResult(AtomicReference<T> value) {
    this.value = value;
  }

  @Override
  public T get() {
    return value.get();
  }

  @Override
  public void set(T value) {
    this.value.set(value);
  }

  @Override
  public void addError(String key, OperationStatus status) {
    exceptions.add(new OperationException(OperationErrorType.GENERAL,
        "{ key: " + key + " message: " + status.getMessage() + " }"));
  }

  @Override
  public boolean hasError() {
    return !exceptions.isEmpty();
  }

  @Override
  public List<Exception> getError() {
    return Collections.unmodifiableList(exceptions);
  }
}
