package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assume.assumeFalse;

public class ArcusClientCreateTest {

  private static final String clientName = "TEST";
  private static final String hostName = "localhost/127.0.0.1:11211";
  private static final List<InetSocketAddress> addrs = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    // This test assumes we does not use ZK
    assumeFalse(BaseIntegrationTest.USE_ZK);
    addrs.add(new InetSocketAddress("localhost", 11211));
  }

  @After
  public void release() {
    addrs.clear();
  }

  @Test
  public void testCreateClientWithCustomName() throws IOException {
    ArcusClient arcusClient = new ArcusClient(new DefaultConnectionFactory(), clientName, addrs);

    Collection<MemcachedNode> nodes = arcusClient.getAllNodes();
    MemcachedNode node = nodes.iterator().next();

    Assert.assertEquals(nodes.size(), 1);
    Assert.assertEquals(node.getNodeName(), clientName + " " + hostName);
  }

  @Test
  public void testCreateClientWithDefaultName() throws IOException {
    ArcusClient arcusClient = new ArcusClient(new DefaultConnectionFactory(), addrs);

    Collection<MemcachedNode> nodes = arcusClient.getAllNodes();
    Assert.assertEquals(nodes.size(), 1);

    MemcachedNode node = nodes.iterator().next();
    Pattern compile = Pattern.compile("ArcusClient-\\d+ " + hostName);
    Matcher matcher = compile.matcher(node.getNodeName());
    Assert.assertTrue(matcher.matches());
  }

  @Test(expected = NullPointerException.class)
  public void testCreateClientNullName() throws IOException {
    new ArcusClient(new DefaultConnectionFactory(), null, addrs);
  }
}
