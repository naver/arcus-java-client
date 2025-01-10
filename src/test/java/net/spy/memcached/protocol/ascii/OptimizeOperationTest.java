package net.spy.memcached.protocol.ascii;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.protocol.TCPMemcachedNodeImpl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptimizeOperationTest {

  private final GetOperation.Callback cb = new GetOperation.Callback() {
    @Override
    public void gotData(String key, int flags, byte[] data) {
      // do nothing
    }

    @Override
    public void receivedStatus(OperationStatus status) {
      // do nothing
    }

    @Override
    public void complete() {
      // do nothing
    }
  };

  @Test
  void chooseGetOrMGet() {
    GetOperationImpl op1 = new GetOperationImpl("key", cb);
    GetOperationImpl op2 = new GetOperationImpl("key2", cb);
    OptimizedGetImpl optimizedOpWithMGet = new OptimizedGetImpl(op1, true);
    optimizedOpWithMGet.addOperation(op2);
    optimizedOpWithMGet.initialize();

    OptimizedGetImpl optimizedOpWithGet = new OptimizedGetImpl(op1, false);
    optimizedOpWithGet.addOperation(op2);
    optimizedOpWithGet.initialize();

    ByteBuffer bbWithMGet = optimizedOpWithMGet.getBuffer();
    byte[] bytesWithMGet = new byte[bbWithMGet.remaining()];
    bbWithMGet.get(bytesWithMGet);
    String commandWithMGet = new String(bytesWithMGet, StandardCharsets.UTF_8);
    assertTrue(commandWithMGet.contains("mget"));

    ByteBuffer bbWithGet = optimizedOpWithGet.getBuffer();
    byte[] bytesWithGet = new byte[bbWithGet.remaining()];
    bbWithGet.get(bytesWithGet);
    String commandWithGet = new String(bytesWithGet, StandardCharsets.UTF_8);
    assertFalse(commandWithGet.contains("mget"));
  }


  @Test
  void doNotMergeTwoOperations() {
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
    builder.setShouldOptimize(true);
    ConnectionFactory cf = builder.build();
    TCPMemcachedNodeImpl node = (TCPMemcachedNodeImpl) cf.createMemcachedNode("node1",
            new InetSocketAddress("localhost", 11211), 4096);
    node.setVersion("1.11.0");

    List<String> keyList = new ArrayList<>();
    for (int i = 0; i < 250; i++) {
      keyList.add("k" + i);
    }

    GetOperationImpl op = new GetOperationImpl(keyList.get(0), cb);
    GetOperationImpl op1 = new GetOperationImpl(keyList.subList(0, 190), cb, true);
    GetOperationImpl op2 = new GetOperationImpl(keyList.subList(191, 205), cb, true);
    node.addOpToWriteQ(op);
    node.addOpToWriteQ(op1);
    node.addOpToWriteQ(op2);
    node.fillWriteBuffer(true);

    ByteBuffer buffer = node.getWbuf();
    byte[] bytesWithMGet = new byte[buffer.remaining()];
    buffer.get(bytesWithMGet);
    String commandWithMGet = new String(bytesWithMGet, StandardCharsets.UTF_8);
    assertTrue(commandWithMGet.contains("mget 839 190"));
    assertTrue(commandWithMGet.contains("mget 69 14"));
  }
}
