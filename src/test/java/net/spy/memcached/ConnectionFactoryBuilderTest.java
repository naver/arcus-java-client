/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.protocol.ascii.AsciiMemcachedNodeImpl;
import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.WhalinTranscoder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the connection factory builder.
 */
class ConnectionFactoryBuilderTest {

  private ConnectionFactoryBuilder b;

  @BeforeEach
  protected void setUp() throws Exception {
    b = new ConnectionFactoryBuilder();
  }

  @Test
  void testDefaults() throws Exception {
    ConnectionFactory f = b.build();
    assertEquals(DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT,
            f.getOperationTimeout());
    assertEquals(DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE,
            f.getReadBufSize());
    //assertSame(DefaultConnectionFactory.DEFAULT_HASH, f.getHashAlg());
    assertSame(HashAlgorithm.KETAMA_HASH, f.getHashAlg());
    assertTrue(f.getDefaultTranscoder() instanceof SerializingTranscoder);
    assertInstanceOf(CollectionTranscoder.class, f.getDefaultCollectionTranscoder());
    //assertSame(DefaultConnectionFactory.DEFAULT_FAILURE_MODE,
    // f.getFailureMode());
    assertSame(FailureMode.Cancel, f.getFailureMode());
    assertEquals(0, f.getInitialObservers().size());
    assertTrue(f.getOperationFactory() instanceof AsciiOperationFactory);

    BlockingQueue<Operation> opQueue = f.createOperationQueue();
    assertTrue(opQueue instanceof ArrayBlockingQueue<?>);
    assertEquals(DefaultConnectionFactory.DEFAULT_OP_QUEUE_LEN,
            opQueue.remainingCapacity());

    BlockingQueue<Operation> readOpQueue = f.createReadOperationQueue();
    assertTrue(readOpQueue instanceof LinkedBlockingQueue<?>);

    BlockingQueue<Operation> writeOpQueue = f.createWriteOperationQueue();
    assertTrue(writeOpQueue instanceof LinkedBlockingQueue<?>);

    // This test case fails.  Arcus Ketama locator builds the hash ring
    // during construction.  Cannot get around the failure.  So, don't run...
    //
    // ...: unexpected invocation
    // Invoked: mockMemcachedNode.getSocketAddress()
    /*
    MemcachedNode n = (MemcachedNode)mock(MemcachedNode.class).proxy();
    assertTrue(f.createLocator(Collections.singletonList(n))
        instanceof ArrayModNodeLocator);
    */

    assertTrue(f.createMemcachedNode("factory builder test node",
            InetSocketAddress.createUnresolved("localhost", 11211), 1)
            instanceof AsciiMemcachedNodeImpl);
    assertTrue(f.isDaemon());
    assertFalse(f.shouldOptimize());
    assertFalse(f.useNagleAlgorithm());
    assertFalse(f.getKeepAlive());
    assertTrue(f.getDnsCacheTtlCheck());
    assertEquals(f.getOpQueueMaxBlockTime(),
            DefaultConnectionFactory.DEFAULT_OP_QUEUE_MAX_BLOCK_TIME);
  }

  @Test
  void testModifications() throws Exception {
    ConnectionObserver testObserver = new ConnectionObserver() {
      public void connectionLost(SocketAddress sa) {
        // none
      }

      public void connectionEstablished(SocketAddress sa, int reconnectCount) {
        // none
      }
    };
    BlockingQueue<Operation> oQueue = new LinkedBlockingQueue<>();
    BlockingQueue<Operation> rQueue = new LinkedBlockingQueue<>();
    BlockingQueue<Operation> wQueue = new LinkedBlockingQueue<>();

    OperationQueueFactory opQueueFactory = new DirectFactory(oQueue);
    OperationQueueFactory rQueueFactory = new DirectFactory(rQueue);
    OperationQueueFactory wQueueFactory = new DirectFactory(wQueue);
    AuthDescriptor anAuthDescriptor = new AuthDescriptor(new String[]{"PLAIN"},
            new PlainCallbackHandler("username", "password"));

    ConnectionFactory f = b.setDaemon(true)
            .setShouldOptimize(false)
            .setFailureMode(FailureMode.Redistribute)
            .setHashAlg(HashAlgorithm.KETAMA_HASH)
            .setInitialObservers(Collections.singleton(testObserver))
            .setOpFact(new BinaryOperationFactory())
            .setOpTimeout(4225)
            .setOpQueueFactory(opQueueFactory)
            .setReadOpQueueFactory(rQueueFactory)
            .setWriteOpQueueFactory(wQueueFactory)
            .setReadBufferSize(19)
            .setTranscoder(new WhalinTranscoder())
            .setCollectionTranscoder(
                    new CollectionTranscoder(CollectionTranscoder.MAX_ELEMENT_BYTES - 1))
            .setUseNagleAlgorithm(true)
            .setKeepAlive(true)
            .setDnsCacheTtlCheck(false)
            .setLocatorType(Locator.CONSISTENT)
            .setOpQueueMaxBlockTime(19)
            .setAuthDescriptor(anAuthDescriptor)
            .build();

    assertEquals(4225, f.getOperationTimeout());
    assertEquals(19, f.getReadBufSize());
    assertSame(HashAlgorithm.KETAMA_HASH, f.getHashAlg());
    assertTrue(f.getDefaultTranscoder() instanceof WhalinTranscoder);
    assertEquals(CollectionTranscoder.MAX_ELEMENT_BYTES - 1,
            f.getDefaultCollectionTranscoder().getMaxSize());
    assertSame(FailureMode.Redistribute, f.getFailureMode());
    assertEquals(1, f.getInitialObservers().size());
    assertSame(testObserver, f.getInitialObservers().iterator().next());
    assertTrue(f.getOperationFactory() instanceof BinaryOperationFactory);
    assertSame(oQueue, f.createOperationQueue());
    assertSame(rQueue, f.createReadOperationQueue());
    assertSame(wQueue, f.createWriteOperationQueue());
    assertTrue(f.isDaemon());
    assertFalse(f.shouldOptimize());
    assertTrue(f.useNagleAlgorithm());
    assertTrue(f.getKeepAlive());
    assertFalse(f.getDnsCacheTtlCheck());
    assertEquals(f.getOpQueueMaxBlockTime(), 19);
    assertSame(anAuthDescriptor, f.getAuthDescriptor());

    MemcachedNode n = new MockMemcachedNode(
            InetSocketAddress.createUnresolved("localhost", 11211));
    assertTrue(f.createLocator(Collections.singletonList(n))
            instanceof KetamaNodeLocator);
    assertTrue(f.createMemcachedNode("factory builder test node",
            InetSocketAddress.createUnresolved("localhost", 11211), 1)
            instanceof BinaryMemcachedNodeImpl);
  }

  @Test
  void testProtocolSetterBinary() {
    assertTrue(
            b.setProtocol(Protocol.BINARY).build().getOperationFactory()
                    instanceof BinaryOperationFactory);
  }

  @Test
  void testProtocolSetterText() {
    assertTrue(
            b.setProtocol(Protocol.TEXT).build().getOperationFactory()
                    instanceof AsciiOperationFactory);

  }

  static class DirectFactory implements OperationQueueFactory {
    private final BlockingQueue<Operation> queue;

    public DirectFactory(BlockingQueue<Operation> q) {
      super();
      queue = q;
    }

    public BlockingQueue<Operation> create() {
      return queue;
    }

  }
}
