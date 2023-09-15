package net.spy.memcached;

import java.net.SocketException;
import java.util.Collection;

import junit.framework.TestCase;

import org.junit.Assert;

public class SocketTest extends TestCase {
  private ArcusClient client;
  private final String ZK_STRING = "127.0.0.1:2181";
  private final String SERVICE_CODE = "test";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();
    connectionFactoryBuilder.setKeepAlive(true);
    client = ArcusClient.createArcusClient(ZK_STRING, SERVICE_CODE, connectionFactoryBuilder);
  }

  public void testSocketKeepAliveTest() throws SocketException {
    Collection<MemcachedNode> allNodes =
            client.getAllNodes();
    for (MemcachedNode node : allNodes) {
      Assert.assertTrue(node.getChannel().socket().getKeepAlive());
    }
  }
}
