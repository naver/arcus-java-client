package net.spy.memcached.v2.migration;

import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.RedirectHandler;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.PipelineOperation;
import net.spy.memcached.protocol.ascii.AsciiMemcachedNodeImpl;
import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.ascii.PipelineOperationImpl;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.v2.AsyncArcusCommands;
import net.spy.memcached.v2.pipe.Pipeline;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineOperationMigrationTest {

  private static ArcusClient arcusClient;
  private static AsyncArcusCommands<Object> async;
  private static final List<String> KEYS = Arrays.asList("migration_key1", "migration_key2",
      "migration_key3", "migration_key4");
  private static final String VALUE = "migration_value";

  @BeforeAll
  static void setUp() {
    arcusClient = ArcusClient.createArcusClient("localhost:2181", "test");
    async = arcusClient.asyncCommands();
  }

  @BeforeEach
  void cleanUp() throws ExecutionException, InterruptedException, TimeoutException {
    async.flush(-1).get(300, TimeUnit.MILLISECONDS);
  }

  @AfterAll
  static void tearDown() {
    arcusClient.shutdown();
  }

  @Test
  void redirectMultiKeysFromPipelineOperation() {
    // given
    SerializingTranscoder tc = SerializingTranscoder.forCollection().build();
    Pipeline<Object> pipeline = new Pipeline<>(new AsciiOperationFactory(), tc);

    pipeline.lopInsert(KEYS.get(0), 0, VALUE);
    pipeline.sopInsert(KEYS.get(1), VALUE);
    pipeline.mopInsert(KEYS.get(2), "mkey", VALUE);
    pipeline.bopInsert(KEYS.get(3), new BTreeElement<>(BKey.of(0L), VALUE, null));

    PipelineOperationImpl pipelineOp = getPipelineOp(pipeline);

    // when
    ByteBuffer b = ByteBuffer.allocate(200);

    String line1 = "RESPONSE 3\r\n";
    b.put(line1.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line2 = "STORED\r\n";
    b.put(line2.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line3 = "NOT_MY_KEY 1000 2000\r\n";
    b.put(line3.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line4 = "STORED\r\n";
    b.put(line4.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line5 = "NOT_MY_KEY 4000 5000\r\n";
    b.put(line5.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line6 = "END\r\n";
    b.put(line6.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    // then
    RedirectHandler redirectHandler = pipelineOp.getAndClearRedirectHandler();
    assertEquals(OperationState.REDIRECT, pipelineOp.getState());
    assertInstanceOf(RedirectHandler.RedirectHandlerMultiKey.class, redirectHandler);
    List<String> keys = ((RedirectHandler.RedirectHandlerMultiKey) redirectHandler)
        .groupRedirectKeys(arcusClient.getMemcachedConnection(), pipelineOp)
        .values().stream().flatMap(List::stream).collect(Collectors.toList());
    assertTrue(keys.contains(KEYS.get(1)));
    assertTrue(keys.contains(KEYS.get(3)));
  }

  private PipelineOperationImpl getPipelineOp(Pipeline<Object> pipeline) {
    PipelineOperation.Callback cb = new PipelineOperation.Callback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        // Do nothing
      }

      @Override
      public void complete() {
        // Do nothing
      }

      @Override
      public void gotStatus(Operation op, OperationStatus status) {
        // Do nothing
      }
    };

    PipelineOperationImpl pipelineOp =
        new PipelineOperationImpl(pipeline.getOps(), new ArrayList<>(pipeline.getKeys()), cb);

    LinkedBlockingQueue<Operation> queue = new LinkedBlockingQueue<>();
    pipelineOp.setHandlingNode(new AsciiMemcachedNodeImpl("testnode", new InetSocketAddress(11211),
        60, queue, queue, queue, 0L, false));
    pipelineOp.writeComplete();
    return pipelineOp;
  }

  @Test
  void redirectSingleKeyFromPipelineOperation() {
    SerializingTranscoder tc = SerializingTranscoder.forCollection().build();
    Pipeline<Object> pipeline = new Pipeline<>(new AsciiOperationFactory(), tc);

    pipeline.lopInsert(KEYS.get(0), 0, VALUE);
    pipeline.lopInsert(KEYS.get(0), 1, VALUE);
    pipeline.lopInsert(KEYS.get(0), 2, VALUE);
    pipeline.lopInsert(KEYS.get(0), 3, VALUE);

    PipelineOperationImpl pipelineOp = getPipelineOp(pipeline);

    // when
    ByteBuffer b = ByteBuffer.allocate(200);

    String line1 = "RESPONSE 2\r\n";
    b.put(line1.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line2 = "STORED\r\n";
    b.put(line2.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line3 = "NOT_MY_KEY 1000 2000\r\n";
    b.put(line3.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    String line4 = "END\r\n";
    b.put(line4.getBytes());
    ((Buffer) b).flip();
    assertDoesNotThrow(() -> pipelineOp.readFromBuffer(b));
    ((Buffer) b).clear();

    // then
    RedirectHandler redirectHandler = pipelineOp.getAndClearRedirectHandler();
    assertEquals(OperationState.REDIRECT, pipelineOp.getState());
    assertInstanceOf(RedirectHandler.RedirectHandlerSingleKey.class, redirectHandler);
    assertEquals(KEYS.get(0),
        ((RedirectHandler.RedirectHandlerSingleKey) redirectHandler).getKey());
  }
}
