package net.spy.memcached;

import java.nio.ByteBuffer;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.protocol.ascii.ExtensibleOperationImpl;

/**
 * This test assumes a client is running on localhost:11211.
 */
public class AsciiClientTest extends ProtocolBaseCase {

  public void testBadOperation() throws Exception {
    client.addOp("x", new ExtensibleOperationImpl(new OperationCallback() {
      public void complete() {
        System.err.println("Complete.");
      }

      public void receivedStatus(OperationStatus s) {
        System.err.println("Received a line.");
      }
    }) {

      @Override
      public void handleLine(String line) {
        System.out.println("Woo! A line!");
      }

      @Override
      public void initialize() {
        setBuffer(ByteBuffer.wrap("garbage\r\n".getBytes()));
      }

      @Override
      public boolean isBulkOperation() {
        return false;
      }

      @Override
      public boolean isPipeOperation() {
        return false;
      }
    });
  }

  @Override
  protected String getExpectedVersionSource() {
    return "/" + ARCUS_HOST;
  }

}
