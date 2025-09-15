package net.spy.memcached;

/**
 * Test cancellation in the binary protocol.
 */
class BinaryCancellationTest extends CancellationBaseCase {

  @Override
  protected void initClient() throws Exception {
    initClient(new ConnectionFactoryBuilder()
            .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
            .setFailureMode(FailureMode.Retry));
  }

}
