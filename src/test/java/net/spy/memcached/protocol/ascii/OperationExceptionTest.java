package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Test operation exception constructors and accessors and stuff.
 */
public class OperationExceptionTest {

  @Test
  public void testEmpty() {
    OperationException oe = new OperationException();
    assertSame(OperationErrorType.GENERAL, oe.getType());
    assertEquals("OperationException: GENERAL", String.valueOf(oe));
  }

  @Test
  public void testServer() {
    OperationException oe = new OperationException(
            OperationErrorType.SERVER, "SERVER_ERROR figures");
    assertSame(OperationErrorType.SERVER, oe.getType());
    assertEquals("OperationException: SERVER: SERVER_ERROR figures",
            String.valueOf(oe));
  }

  @Test
  public void testClient() {
    OperationException oe = new OperationException(
            OperationErrorType.CLIENT, "CLIENT_ERROR nope");
    assertSame(OperationErrorType.CLIENT, oe.getType());
    assertEquals("OperationException: CLIENT: CLIENT_ERROR nope",
            String.valueOf(oe));
  }

  @Test
  public void testGeneral() {
    // General type doesn't have additional info
    OperationException oe = new OperationException(
            OperationErrorType.GENERAL, "GENERAL wtf");
    assertSame(OperationErrorType.GENERAL, oe.getType());
    assertEquals("OperationException: GENERAL", String.valueOf(oe));
  }
}
