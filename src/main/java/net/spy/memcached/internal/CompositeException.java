package net.spy.memcached.internal;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.ExceptionMessageFactory;

public class CompositeException extends ExecutionException {

  private static final long serialVersionUID = -599478797582490012L;
  private final ArrayList<Exception> exceptions = new ArrayList<>();
  private final Throwable cause;

  public CompositeException(List<Exception> exceptions) {
    super(ExceptionMessageFactory.createCompositeMessage(exceptions));

    if (exceptions.size() > 1) {
      StringWriter sw = new StringWriter();
      sw.write(System.lineSeparator());
      try (PrintWriter pw = new PrintWriter(sw)) {
        for (Exception e : exceptions) {
          e.printStackTrace(pw);
        }
      }

      this.cause = new ExceptionOverview(sw.toString());
    } else {
      this.cause = exceptions.get(0);
    }
    this.exceptions.addAll(exceptions);
  }

  public List<Exception> getExceptions() {
    return exceptions;
  }

  public int size() {
    return exceptions.size();
  }

  @Override
  public synchronized Throwable getCause() {
    return cause;
  }

  static final class ExceptionOverview extends RuntimeException {

    private static final long serialVersionUID = -641960514509105302L;

    ExceptionOverview(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
