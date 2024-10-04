package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class ArcusClientCreateTest {

  private static final String clientName = "TEST";
  private static final String hostName = "localhost/127.0.0.1:11211";
  private static final List<InetSocketAddress> addrs = new ArrayList<>();

  @BeforeEach
  protected void setUp() throws Exception {
    // This test assumes we does not use ZK
    assumeFalse(BaseIntegrationTest.USE_ZK);
    addrs.add(new InetSocketAddress("localhost", 11211));
  }

  @AfterEach
  protected void release() {
    addrs.clear();
  }

  @Test
  void testCreateClientWithDefaultName() throws IOException {
    ArcusClient arcusClient = ArcusClient.createArcusClient(addrs, new ConnectionFactoryBuilder());

    Collection<MemcachedNode> nodes = arcusClient.getAllNodes();
    assertEquals(nodes.size(), 1);

    MemcachedNode node = nodes.iterator().next();
    Pattern compile = Pattern.compile("ArcusClient-\\d+ " + hostName);
    Matcher matcher = compile.matcher(node.getNodeName());
    assertTrue(matcher.matches());
  }
}
