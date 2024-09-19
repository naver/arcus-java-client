package net.spy.memcached.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.ExceptionMessageFactory;

public class CompositeException extends ExecutionException {

  private static final long serialVersionUID = -599478797582490012L;
  private final ArrayList<Exception> exceptions = new ArrayList<>();

  CompositeException(List<Exception> exceptions) {
    super(ExceptionMessageFactory.createCompositeMessage(exceptions));
    this.exceptions.addAll(exceptions);
  }

  public List<Exception> getExceptions() {
    return exceptions;
  }
}
