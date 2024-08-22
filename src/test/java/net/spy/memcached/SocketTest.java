package net.spy.memcached;

import java.net.SocketException;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SocketTest {
  private ArcusClient client;
  private final String ZK_STRING = "127.0.0.1:2181";
  private final String SERVICE_CODE = "test";

  @BeforeEach
  protected void setUp() throws Exception {
    ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();
    connectionFactoryBuilder.setKeepAlive(true);
    client = ArcusClient.createArcusClient(ZK_STRING, SERVICE_CODE, connectionFactoryBuilder);
  }

  @Test
  public void testSocketKeepAliveTest() throws SocketException {
    Collection<MemcachedNode> allNodes =
            client.getAllNodes();
    for (MemcachedNode node : allNodes) {
      assertTrue(node.getChannel().socket().getKeepAlive());
    }
  }
}
