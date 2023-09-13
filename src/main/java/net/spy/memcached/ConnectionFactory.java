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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Factory for creating instances of MemcachedConnection.
 * This is used to provide more fine-grained configuration of connections.
 */
public interface ConnectionFactory {

  /**
   * Create a MemcachedConnection for the given SocketAddresses.
   *
   * @param name  client name
   * @param addrs the addresses of the memcached servers
   * @return a new MemcachedConnection connected to those addresses
   * @throws IOException for problems initializing the memcached connections
   */
  MemcachedConnection createConnection(String name, List<InetSocketAddress> addrs)
          throws IOException;

  /**
   * Create a new memcached node.
   */
  MemcachedNode createMemcachedNode(String name,
                                    SocketAddress sa,
                                    int bufSize);

  /**
   * Create a BlockingQueue for operations for a connection.
   */
  BlockingQueue<Operation> createOperationQueue();

  /**
   * Create a BlockingQueue for the operations currently expecting to read
   * responses from memcached.
   */
  BlockingQueue<Operation> createReadOperationQueue();

  /**
   * Create a BlockingQueue for the operations currently expecting to write
   * requests to memcached.
   */
  BlockingQueue<Operation> createWriteOperationQueue();

  /**
   * Get the maximum amount of time (in milliseconds) a client is willing
   * to wait to add a new item to a queue.
   */
  long getOpQueueMaxBlockTime();

  /**
   * Create a NodeLocator instance for the given list of nodes.
   */
  NodeLocator createLocator(List<MemcachedNode> nodes);

  /**
   * Get the operation factory for connections built by this connection
   * factory.
   */
  OperationFactory getOperationFactory();

  /**
   * Get the operation timeout used by this connection.
   */
  long getOperationTimeout();

  /**
   * If true, the IO thread should be a daemon thread.
   */
  boolean isDaemon();

  /**
   * If true, the nagle algorithm will be used on connected sockets.
   *
   * <p>
   * See {@link java.net.Socket#setTcpNoDelay(boolean)} for more information.
   * </p>
   */
  boolean useNagleAlgorithm();

  /**
   * If true, keep alive will be used on connected sockets.
   *
   * <p>
   * See {@link java.net.Socket#setKeepAlive(boolean)} for more information.
   * </p>
   */
  boolean getKeepAlive();

  /**
   * Observers that should be established at the time of connection
   * instantiation.
   *
   * These observers will see the first connection established.
   */
  Collection<ConnectionObserver> getInitialObservers();

  /**
   * Get the default failure mode for the underlying connection.
   */
  FailureMode getFailureMode();

  /**
   * Get the default transcoder to be used in connections created by this
   * factory.
   */
  Transcoder<Object> getDefaultTranscoder();

  /**
   * Get the default collection transcoder to be used in connections created by this
   * factory.
   */
  Transcoder<Object> getDefaultCollectionTranscoder();

  /**
   * If true, low-level optimization is in effect.
   */
  boolean shouldOptimize();

  /**
   * Get the read buffer size set at construct time.
   */
  int getReadBufSize();

  /**
   * Get the hash algorithm to be used.
   */
  public HashAlgorithm getHashAlg();

  /**
   * Maximum number of seconds to wait between reconnect attempts.
   */
  long getMaxReconnectDelay();

  /**
   * Authenticate connections using the given auth descriptor.
   *
   * @return null if no authentication should take place
   */
  AuthDescriptor getAuthDescriptor();

  /**
   * Maximum number of timeout exception for shutdown connection
   */
  int getTimeoutExceptionThreshold();

  /**
   * Maximum timeout ratio for shutdown connection
   */
  int getTimeoutRatioThreshold();

  /**
   * Maximum timeout duration in milliseconds for shutdown connection
   */
  int getTimeoutDurationThreshold();

  /**
   * Set the maximum number of front cache elements.
   */
  int getMaxFrontCacheElements();

  /**
   * Set front cache's expire time.
   */
  int getFrontCacheExpireTime();

  /**
   * get front cache name
   */
  String getFrontCacheName();

  /**
   * get copyOnRead property for front cache
   */
  boolean getFrontCacheCopyOnRead();

  /**
   * get copyOnWrite property for front cache
   */
  boolean getFrontCacheCopyOnWrite();

  /**
   * get max smget key chunk size
   */
  int getDefaultMaxSMGetKeyChunkSize();

  /**
   * get delimiter
   */
  byte getDelimiter();

  /* ENABLE_REPLICATION if */
  /**
   * get read priority on replica nodes
   */
  ReadPriority getReadPriority();

  /**
   * get api read priority
   *
   * @return
   */
  Map<APIType, ReadPriority> getAPIReadPriority();
  /* ENABLE_REPLICATION end */
}
