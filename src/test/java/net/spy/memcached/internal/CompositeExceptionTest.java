package net.spy.memcached.internal;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CompositeExceptionTest {

  @Test
  void printStackTraceOfAllExceptions() {
    List<Exception> exceptions = new ArrayList<>();
    exceptions.add(new OperationException(OperationErrorType.SERVER, "msg1"));
    exceptions.add(new OperationException(OperationErrorType.CLIENT, "msg2"));
    Exception e = throwError1();
    exceptions.add(e);
    CompositeException compositeException = new CompositeException(exceptions);
    String message = compositeException.getCause().getMessage();

    assertTrue(message
            .contains("OperationException: SERVER: msg1"));
    assertTrue(message
            .contains("OperationException: CLIENT: msg2"));
    assertTrue(message
            .contains("OperationException: SERVER: msg3"));
    assertTrue(message
            .contains("at net.spy.memcached.internal.CompositeExceptionTest.throwError2"));
    assertTrue(message
            .contains("at net.spy.memcached.internal.CompositeExceptionTest.throwError1"));
  }

  private Exception throwError1() {
    return throwError2();
  }

  private Exception throwError2() {
    return new OperationException(OperationErrorType.SERVER, "msg3");
  }
}
