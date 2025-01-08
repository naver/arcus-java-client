package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.OperationStatus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptimizeOperationTest {

  @Test
  void chooseGetOrMGet() {
    GetOperation.Callback cb = new GetOperation.Callback() {
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
}
