/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Corp.
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.ascii.AsciiMemcachedNodeImpl;
import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Default implementation of ConnectionFactory.
 *
 * <p>
 * This implementation creates connections where the operation queue is an
 * ArrayBlockingQueue and the read and write queues are unbounded
 * LinkedBlockingQueues.  The <code>Redistribute</code> FailureMode is always
 * used.  If other FailureModes are needed, look at the
 * ConnectionFactoryBuilder.
 *
 * </p>
 */
public class DefaultConnectionFactory extends SpyObject
        implements ConnectionFactory {

  /**
   * Default failure mode.
   */
  public static final FailureMode DEFAULT_FAILURE_MODE =
          FailureMode.Redistribute;

  /**
   * Default hash algorithm.
   */
  public static final HashAlgorithm DEFAULT_HASH = HashAlgorithm.NATIVE_HASH;

  /**
   * Maximum length of the operation queue returned by this connection
   * factory.
   */
  public static final int DEFAULT_OP_QUEUE_LEN = 16384;

  /**
   * The maximum time to block waiting for op queue operations to complete,
   * in milliseconds. The default has been set with the expectation that
   * most requests are interactive and waiting for more than a few seconds
   * is thus more undesirable than failing the request.
   */
  public static final long DEFAULT_OP_QUEUE_MAX_BLOCK_TIME =
          TimeUnit.SECONDS.toMillis(10);

  /**
   * The read buffer size for each server connection from this factory.
   */
  public static final int DEFAULT_READ_BUFFER_SIZE = 16384;

  /**
   * Default operation timeout in milliseconds.
   *
   * operation timeout : 700ms
   * It avoids the occurrence of operation timeout
   * even if two packet retransmissions exist in linux.
   */
  public static final long DEFAULT_OPERATION_TIMEOUT = 700L;

  /**
   * Maximum amount of time (in seconds) to wait between reconnect attempts.
   */
  public static final long DEFAULT_MAX_RECONNECT_DELAY = 30;

  /**
   * Maximum number + 2 of timeout exception for shutdown connection
   */
  public static final int DEFAULT_MAX_TIMEOUTEXCEPTION_THRESHOLD = 998;

  /**
   * Maximum timeout ratio for shutdown connection
   */
  public static final int DEFAULT_MAX_TIMEOUTRATIO_THRESHOLD = 0;

  /**
   * Maximum timeout duration in milliseconds for shutdown connection
   */
  public static final int DEFAULT_MAX_TIMEOUTDURATION_THRESHOLD = 0;

  /**
   * Maximum number of Front cache elements
   */
  public static final int DEFAULT_MAX_FRONTCACHE_ELEMENTS = 0;

  /**
   * Maximum number of Front cache elements
   */
  public static final int DEFAULT_FRONTCACHE_EXPIRETIME = 5;

  /**
   * Default front cache name
   */
  private static final String DEFAULT_FRONT_CACHE_NAME =
      "ArcusFrontCache" + new Object().hashCode();

  /**
   * copyOnRead is no longer used
   * Default copyOnRead : false
   */
  @Deprecated
  public static final boolean DEFAULT_FRONT_CACHE_COPY_ON_READ = false;

  /**
   * copyOnWrite is no longer used
   * Default copyOnWrite : false
   */
  @Deprecated
  public static final boolean DEFAULT_FRONT_CACHE_COPY_ON_WRITE = false;

  /**
   * Max smget key chunk size per request
   */
  public static final int DEFAULT_MAX_SMGET_KEY_CHUNK_SIZE = 500;

  /**
   * The default delimiter that separates the key and prefix
   */
  public static final byte DEFAULT_DELIMITER = ':';

  private final int opQueueLen;
  private final int readBufSize;
  private final HashAlgorithm hashAlg;

  /* ENABLE_REPLICATION if */
  public static final ReadPriority DEFAULT_READ_PRIORITY = ReadPriority.MASTER;
  private Map<APIType, ReadPriority> DEFAULT_API_READ_PRIORITY_LIST =
          new HashMap<>();
  /* ENABLE_REPLICATION end */

  /**
   * Construct a DefaultConnectionFactory with the given parameters.
   *
   * @param qLen    the queue length.
   * @param bufSize the buffer size
   * @param hash    the algorithm to use for hashing
   */
  public DefaultConnectionFactory(int qLen, int bufSize, HashAlgorithm hash) {
    super();
    opQueueLen = qLen;
    readBufSize = bufSize;
    hashAlg = hash;
  }

  /**
   * Create a DefaultConnectionFactory with the given maximum operation
   * queue length, and the given read buffer size.
   */
  public DefaultConnectionFactory(int qLen, int bufSize) {
    this(qLen, bufSize, DEFAULT_HASH);
  }

  /**
   * Create a DefaultConnectionFactory with the default parameters.
   */
  public DefaultConnectionFactory() {
    this(DEFAULT_OP_QUEUE_LEN, DEFAULT_READ_BUFFER_SIZE);
  }

  public MemcachedNode createMemcachedNode(String name,
                                           SocketAddress sa,
                                           int bufSize) {

    OperationFactory of = getOperationFactory();
    if (of instanceof AsciiOperationFactory) {
      return new AsciiMemcachedNodeImpl(name,
              sa, bufSize,
              createReadOperationQueue(),
              createWriteOperationQueue(),
              createOperationQueue(),
              getOpQueueMaxBlockTime());
    } else if (of instanceof BinaryOperationFactory) {
      boolean doAuth = false;
      if (getAuthDescriptor() != null) {
        doAuth = true;
      }
      return new BinaryMemcachedNodeImpl(name,
              sa, bufSize,
              createReadOperationQueue(),
              createWriteOperationQueue(),
              createOperationQueue(),
              getOpQueueMaxBlockTime(),
              doAuth);
    } else {
      throw new IllegalStateException(
              "Unhandled operation factory type " + of);
    }
  }

  public MemcachedConnection createConnection(String name,
                                              List<InetSocketAddress> addrs)
          throws IOException {
    return new MemcachedConnection(name, this, addrs,
            getInitialObservers(), getFailureMode(), getOperationFactory());
  }

  public FailureMode getFailureMode() {
    return DEFAULT_FAILURE_MODE;
  }

  public BlockingQueue<Operation> createOperationQueue() {
    return new ArrayBlockingQueue<>(getOpQueueLen());
  }

  public BlockingQueue<Operation> createReadOperationQueue() {
    return new LinkedBlockingQueue<>();
  }

  public BlockingQueue<Operation> createWriteOperationQueue() {
    return new LinkedBlockingQueue<>();
  }

  public NodeLocator createLocator(List<MemcachedNode> nodes) {
    return new ArrayModNodeLocator(nodes, getHashAlg());
  }

  /**
   * Get the op queue length set at construct time.
   */
  public int getOpQueueLen() {
    return opQueueLen;
  }

  /**
   * @return the maximum time to block waiting for op queue operations to
   * complete, in milliseconds, or null for no waiting.
   */
  public long getOpQueueMaxBlockTime() {
    return DEFAULT_OP_QUEUE_MAX_BLOCK_TIME;
  }

  public int getReadBufSize() {
    return readBufSize;
  }

  public HashAlgorithm getHashAlg() {
    return hashAlg;
  }

  public OperationFactory getOperationFactory() {
    return new AsciiOperationFactory();
  }

  public long getOperationTimeout() {
    return DEFAULT_OPERATION_TIMEOUT;
  }

  public boolean isDaemon() {
    return false;
  }

  public Collection<ConnectionObserver> getInitialObservers() {
    return Collections.emptyList();
  }

  public Transcoder<Object> getDefaultTranscoder() {
    return new SerializingTranscoder();
  }

  public Transcoder<Object> getDefaultCollectionTranscoder() {
    return new CollectionTranscoder();
  }

  public boolean useNagleAlgorithm() {
    return false;
  }
  @Override
  public boolean getKeepAlive() {
    return false;
  }

  @Override
  public boolean getDnsCacheTtlCheck() {
    return true;
  }

  public boolean shouldOptimize() {
    return false;
  }

  public long getMaxReconnectDelay() {
    return DEFAULT_MAX_RECONNECT_DELAY;
  }

  public AuthDescriptor getAuthDescriptor() {
    return null;
  }

  public int getTimeoutExceptionThreshold() {
    return DEFAULT_MAX_TIMEOUTEXCEPTION_THRESHOLD;
  }

  public int getTimeoutRatioThreshold() {
    return DEFAULT_MAX_TIMEOUTRATIO_THRESHOLD;
  }

  public int getTimeoutDurationThreshold() {
    return DEFAULT_MAX_TIMEOUTDURATION_THRESHOLD;
  }

  public int getMaxFrontCacheElements() {
    return DEFAULT_MAX_FRONTCACHE_ELEMENTS;
  }

  public int getFrontCacheExpireTime() {
    return DEFAULT_FRONTCACHE_EXPIRETIME;
  }

  @Override
  public String getFrontCacheName() {
    return DEFAULT_FRONT_CACHE_NAME;
  }

  @Override
  @Deprecated
  public boolean getFrontCacheCopyOnRead() {
    return DEFAULT_FRONT_CACHE_COPY_ON_READ;
  }

  @Override
  @Deprecated
  public boolean getFrontCacheCopyOnWrite() {
    return DEFAULT_FRONT_CACHE_COPY_ON_WRITE;
  }

  @Override
  public int getDefaultMaxSMGetKeyChunkSize() {
    return DEFAULT_MAX_SMGET_KEY_CHUNK_SIZE;
  }

  @Override
  public byte getDelimiter() {
    return DEFAULT_DELIMITER;
  }

  /* ENABLE_REPLICATION if */
  public ReadPriority getReadPriority() {
    return DEFAULT_READ_PRIORITY;
  }

  public Map<APIType, ReadPriority> getAPIReadPriority() {
    return DEFAULT_API_READ_PRIORITY_LIST;
  }
  /* ENABLE_REPLICATION end */
}
