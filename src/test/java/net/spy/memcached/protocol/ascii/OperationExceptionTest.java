package net.spy.memcached.protocol.ascii;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test operation exception constructors and accessors and stuff.
 */
class OperationExceptionTest extends BaseIntegrationTest {

  private final APIType apiType = APIType.GET;

  @Test
  void testServer() {
    OperationException oe = new OperationException(OperationErrorType.SERVER,
            "SERVER_ERROR figures" + " @ "
                    + mc.getMemcachedConnection()
                    .getPrimaryNode("test", apiType)
                    .getNodeName());
    assertSame(OperationErrorType.SERVER, oe.getType());
    assertTrue(String.valueOf(oe).startsWith("OperationException: SERVER:" +
            " SERVER_ERROR figures @ ArcusClient-"));
  }

  @Test
  void testClient() {
    OperationException oe = new OperationException(OperationErrorType.CLIENT,
            "CLIENT_ERROR nope" + " @ "
                    + mc.getMemcachedConnection()
                    .getPrimaryNode("test", apiType)
                    .getNodeName());
    assertSame(OperationErrorType.CLIENT, oe.getType());
    assertTrue(String.valueOf(oe).startsWith("OperationException: CLIENT:" +
            " CLIENT_ERROR nope @ ArcusClient-"));
  }

  @Test
  void testGeneral() {
    OperationException oe = new OperationException(OperationErrorType.GENERAL,
            "ERROR no matching command" + " @ "
                    + mc.getMemcachedConnection()
                    .getPrimaryNode("test", apiType)
                    .getNodeName());
    assertSame(OperationErrorType.GENERAL, oe.getType());
    assertTrue(String.valueOf(oe).startsWith("OperationException: GENERAL:" +
            " ERROR no matching command @ ArcusClient-"));
  }
}
