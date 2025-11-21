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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Builder for more easily configuring a ConnectionFactory.
 */
public class ConnectionFactoryBuilder {

  private OperationQueueFactory opQueueFactory;
  private OperationQueueFactory readQueueFactory;
  private OperationQueueFactory writeQueueFactory;

  private Transcoder<Object> transcoder;
  private Transcoder<Object> collectionTranscoder;

  private FailureMode failureMode = FailureMode.Cancel;

  private Collection<ConnectionObserver> initialObservers
          = Collections.emptyList();

  private OperationFactory opFact;

  private Locator locator = Locator.ARCUSCONSISTENT;
  private long opTimeout = -1;
  private boolean isDaemon = true;
  private boolean shouldOptimize = false;
  private boolean useNagle = false;
  private boolean keepAlive = false;
  private boolean dnsCacheTtlCheck = true;
  private long maxReconnectDelay = 1;

  private int readBufSize = -1;
  private HashAlgorithm hashAlg = HashAlgorithm.KETAMA_HASH;
  private AuthDescriptor authDescriptor = null;
  private long opQueueMaxBlockTime = -1;

  private int timeoutExceptionThreshold = 10;
  private int timeoutDurationThreshold = 1600;

  private int maxFrontCacheElements = DefaultConnectionFactory.DEFAULT_MAX_FRONTCACHE_ELEMENTS;
  private int frontCacheExpireTime = DefaultConnectionFactory.DEFAULT_FRONTCACHE_EXPIRETIME;
  private String frontCacheName;

  private int maxSMGetChunkSize = DefaultConnectionFactory.DEFAULT_MAX_SMGET_KEY_CHUNK_SIZE;
  private byte delimiter = DefaultConnectionFactory.DEFAULT_DELIMITER;

  /* ENABLE_REPLICATION if */
  private boolean arcusReplEnabled = false;

  private ReadPriority readPriority = ReadPriority.MASTER;
  private Map<APIType, ReadPriority> apiReadPriorityList = new HashMap<>();
  /* ENABLE_REPLICATION end */

  /* ENABLE_MIGRATION if */
  private boolean arcusMigrationCheck = false; // for arcus users
  private boolean arcusMigrEnabled = false; // for internal

  public void internalArcusMigrEnabled(boolean b) {
    arcusMigrEnabled = b;
  }

  public ConnectionFactoryBuilder setArcusMigrationCheck(boolean enable) {
    arcusMigrationCheck = enable;
    return this;
  }

  public boolean getArcusMigrationCheck() {
    return arcusMigrationCheck;
  }
  /* ENABLE_MIGRATION end */

  /* ENABLE_REPLICATION if */
  /* Called by cache manager after checking ZK nodes */
  void setArcusReplEnabled(boolean b) {
    arcusReplEnabled = b;
  }
  /* ENABLE_REPLICATION end */

  public FailureMode getFailureMode() {
    return failureMode;
  }

  /**
   * Set the operation queue factory.
   */
  public ConnectionFactoryBuilder setOpQueueFactory(OperationQueueFactory q) {
    if (q == null) {
      throw new IllegalArgumentException("OperationQueueFactory must not be null.");
    }

    opQueueFactory = q;
    return this;
  }

  /**
   * Set the read queue factory.
   */
  public ConnectionFactoryBuilder setReadOpQueueFactory(OperationQueueFactory q) {
    if (q == null) {
      throw new IllegalArgumentException("OperationQueueFactory must not be null.");
    }

    readQueueFactory = q;
    return this;
  }

  /**
   * Set the write queue factory.
   */
  public ConnectionFactoryBuilder setWriteOpQueueFactory(OperationQueueFactory q) {
    if (q == null) {
      throw new IllegalArgumentException("OperationQueueFactory must not be null.");
    }

    writeQueueFactory = q;
    return this;
  }

  /**
   * Set the maximum amount of time (in milliseconds) a client is willing to
   * wait for space to become available in an output queue.
   * If the value is negative or 0, the client will not be blocked
   * and the request will be failed immediately.
   */
  public ConnectionFactoryBuilder setOpQueueMaxBlockTime(long t) {
    opQueueMaxBlockTime = t;
    return this;
  }

  /**
   * Set the default transcoder.
   */
  public ConnectionFactoryBuilder setTranscoder(Transcoder<Object> t) {
    if (t == null) {
      throw new IllegalArgumentException("Transcoder must not be null.");
    }

    transcoder = t;
    return this;
  }

  /**
   * Set the default collection transcoder.
   */
  public ConnectionFactoryBuilder setCollectionTranscoder(Transcoder<Object> t) {
    if (t == null) {
      throw new IllegalArgumentException("CollectionTranscoder must not be null.");
    }

    collectionTranscoder = t;
    return this;
  }

  /**
   * Set the failure mode.
   */
  public ConnectionFactoryBuilder setFailureMode(FailureMode fm) {
    if (fm == null) {
      throw new IllegalArgumentException("FailureMode must not be null.");
    }

    failureMode = fm;
    return this;
  }

  /**
   * Set the initial connection observers (will observe initial connection).
   */
  public ConnectionFactoryBuilder setInitialObservers(
          Collection<ConnectionObserver> obs) {
    if (obs == null || obs.isEmpty()) {
      throw new IllegalArgumentException("Initial observers must not be null or empty.");
    }

    initialObservers = obs;
    return this;
  }

  /**
   * Set the operation factory.
   *
   * Note that the operation factory is used to also imply the type of
   * nodes to create.
   *
   * @see MemcachedNode
   */
  public ConnectionFactoryBuilder setOpFact(OperationFactory f) {
    if (f == null) {
      throw new IllegalArgumentException("OperationFactory must not be null.");
    }

    opFact = f;
    return this;
  }

  /**
   * Set the default operation timeout in milliseconds.
   */
  public ConnectionFactoryBuilder setOpTimeout(long t) {
    if (t <= 0) {
      throw new IllegalArgumentException("Operation timeout must be positive.");
    }

    opTimeout = t;
    return this;
  }

  /**
   * Set the daemon state of the IO thread (defaults to true).
   */
  public ConnectionFactoryBuilder setDaemon(boolean d) {
    isDaemon = d;
    return this;
  }

  /**
   * Set to false if the default operation optimization is not desirable.
   */
  public ConnectionFactoryBuilder setShouldOptimize(boolean o) {
    shouldOptimize = o;
    return this;
  }

  /**
   * Set the read buffer size.
   */
  public ConnectionFactoryBuilder setReadBufferSize(int to) {
    if (to <= 0) {
      throw new IllegalArgumentException("Read buffer size must be positive.");
    }

    readBufSize = to;
    return this;
  }

  /**
   * Set the hash algorithm.
   */
  public ConnectionFactoryBuilder setHashAlg(HashAlgorithm to) {
    if (to == null) {
      throw new IllegalArgumentException("HashAlgorithm must not be null.");
    }

    hashAlg = to;
    return this;
  }

  /**
   * Set to true if you'd like to enable the Nagle algorithm.
   */
  public ConnectionFactoryBuilder setUseNagleAlgorithm(boolean to) {
    useNagle = to;
    return this;
  }

  /**
   * Convenience method to specify the protocol to use.
   */
  public ConnectionFactoryBuilder setProtocol(Protocol prot) {
    switch (prot) {
      case TEXT:
        opFact = new AsciiOperationFactory();
        break;
      case BINARY:
        opFact = new BinaryOperationFactory();
        break;
      default:
        throw new IllegalArgumentException("Unhandled protocol: " + prot);
    }
    return this;
  }

  /**
   * Set the locator type.
   */
  public ConnectionFactoryBuilder setLocatorType(Locator l) {
    if (l == null) {
      throw new IllegalArgumentException("Locator must not be null.");
    }

    locator = l;
    return this;
  }

  /**
   * Set the maximum reconnect delay. Should be positive number.
   */
  public ConnectionFactoryBuilder setMaxReconnectDelay(long to) {
    if (to <= 0) {
      throw new IllegalArgumentException("Reconnect delay must be a positive number.");
    }

    maxReconnectDelay = to;
    return this;
  }

  /**
   * Set the auth descriptor to enable authentication on new connections.
   */
  public ConnectionFactoryBuilder setAuthDescriptor(AuthDescriptor to) {
    if (to == null) {
      throw new IllegalArgumentException("AuthDescriptor must not be null.");
    }

    authDescriptor = to;
    return this;
  }

  /**
   * Get the auth descriptor.
   *
   * @return the auth descriptor
   */
  public AuthDescriptor getAuthDescriptor() {
    return authDescriptor;
  }

  /**
   * Set the maximum timeout exception threshold. Should be larger than 2 because
   * this property is to detect continuously occurred exceptions.
   */
  public ConnectionFactoryBuilder setTimeoutExceptionThreshold(int to) {
    if (to < 2) {
      throw new IllegalArgumentException("Minimum timeout exception threshold is 2.");
    }

    timeoutExceptionThreshold = to - 2;
    return this;
  }

  /**
   * Set the maximum timeout duration threshold: 0(disabled), 1000~5000
   */
  public ConnectionFactoryBuilder setTimeoutDurationThreshold(int to) {
    if (!(to == 0 || to >= 1000 && to <= 5000)) {
      throw new IllegalArgumentException(
              "Timeout duration threshold must be 0 or 1000~5000 range.");
    }

    timeoutDurationThreshold = to;
    return this;
  }

  /**
   * Set the maximum number of front cache elements. Should be positive number.
   */
  public ConnectionFactoryBuilder setMaxFrontCacheElements(int to) {
    if (to <= 0) {
      throw new IllegalArgumentException(
              "Max number of front cache elements must be positive.");
    }

    maxFrontCacheElements = to;
    return this;
  }

  /**
   * Set front cache's expire time. Should be positive number.
   */
  public ConnectionFactoryBuilder setFrontCacheExpireTime(int to) {
    if (to <= 0) {
      throw new IllegalArgumentException("Expire time of front cache elements must be positive.");
    }

    frontCacheExpireTime = to;
    return this;
  }

  /**
   * Set front cache copyOnRead property
   */
  @Deprecated
  public ConnectionFactoryBuilder setFrontCacheCopyOnRead(boolean copyOnRead) {
    return this;
  }

  /**
   * Set front cache copyOnWrite property
   */
  @Deprecated
  public ConnectionFactoryBuilder setFrontCacheCopyOnWrite(boolean copyOnWrite) {
    return this;
  }

  /**
   * Set max smget key chunk size
   */
  public ConnectionFactoryBuilder setMaxSMGetKeyChunkSize(int size) {
    if (size <= 0 || size > 10000) {
      throw new IllegalArgumentException(
              "Max smget key chunk size must be a positive number and less than 10000");
    }

    maxSMGetChunkSize = size;
    return this;
  }

  /**
   * Set delimiter to separate key and prefix
   */
  public ConnectionFactoryBuilder setDelimiter(byte to) {
    delimiter = to;
    return this;
  }

  /* ENABLE_REPLICATION if */
  /**
   * Set read priority for choosing replica node to read data
   */
  public ConnectionFactoryBuilder setReadPriority(ReadPriority priority) {
    if (priority == null) {
      throw new IllegalArgumentException("ReadPriority must not be null.");
    }

    readPriority = priority;
    return this;
  }

  public ConnectionFactoryBuilder setAPIReadPriority(APIType apiType, ReadPriority readPriority) {
    if (apiType == null || readPriority == null) {
      throw new IllegalArgumentException("APIType and ReadPriority must not be null.");
    }

    OperationType type = apiType.getAPIOpType();

    if (type == OperationType.READ) {
      this.apiReadPriorityList.put(apiType, readPriority);
    }

    return this;
  }

  public ConnectionFactoryBuilder setAPIReadPriority(Map<APIType, ReadPriority> apiList) {
    if (apiList == null || apiList.isEmpty()) {
      throw new IllegalArgumentException("APIType and ReadPriority must not be null.");
    }

    this.apiReadPriorityList.clear();

    for (Map.Entry<APIType, ReadPriority> entry : apiList.entrySet()) {
      OperationType type = entry.getKey().getAPIOpType();
      if (type == OperationType.READ) {
        this.apiReadPriorityList.put(entry.getKey(), entry.getValue());
      }
    }

    return this;
  }

  public ConnectionFactoryBuilder clearAPIReadPriority() {
    this.apiReadPriorityList.clear();

    return this;
  }
  /* ENABLE_REPLICATION end */

  public ConnectionFactoryBuilder setKeepAlive(boolean on) {
    keepAlive = on;
    return this;
  }

  public ConnectionFactoryBuilder setDnsCacheTtlCheck(boolean dnsCacheTtlCheck) {
    this.dnsCacheTtlCheck = dnsCacheTtlCheck;
    return this;
  }

  /**
   * Get the ConnectionFactory set up with the provided parameters.
   */
  public ConnectionFactory build() {
    return new DefaultConnectionFactory() {

      /* ENABLE_REPLICATION if */
      @Override
      public MemcachedConnection createConnection(String name,
                                                  List<InetSocketAddress> addrs)
              throws IOException {
        MemcachedConnection c = super.createConnection(name, addrs);
        c.setArcusReplEnabled(arcusReplEnabled);
        /* ENABLE_MIGRATION if */
        c.setArcusMigrEnabled(arcusMigrEnabled);
        /* ENABLE_MIGRATION end */
        return c;
      }
      /* ENABLE_REPLICATION end */

      @Override
      public BlockingQueue<Operation> createOperationQueue() {
        return opQueueFactory == null ?
                super.createOperationQueue() : opQueueFactory.create();
      }

      @Override
      public BlockingQueue<Operation> createReadOperationQueue() {
        return readQueueFactory == null ?
                super.createReadOperationQueue()
                : readQueueFactory.create();
      }

      @Override
      public BlockingQueue<Operation> createWriteOperationQueue() {
        return writeQueueFactory == null ?
                super.createWriteOperationQueue()
                : writeQueueFactory.create();
      }

      @Override
      public NodeLocator createLocator(List<MemcachedNode> nodes) {
        switch (locator) {
          case ARRAY_MOD:
            return new ArrayModNodeLocator(nodes, getHashAlg());
          case CONSISTENT:
            return new KetamaNodeLocator(nodes, getHashAlg());
          case ARCUSCONSISTENT:
            /* ENABLE_REPLICATION if */
            if (arcusReplEnabled) {
              // Arcus repl cluster
              // This locator uses ArcusReplKetamaNodeLocatorConfiguration
              // which builds keys off the server's group name, not
              // its ip:port.
              return new ArcusReplKetamaNodeLocator(nodes);
            }
            /* ENABLE_REPLICATION end */
            return new ArcusKetamaNodeLocator(nodes);
          default:
            throw new IllegalStateException(
                    "Unhandled locator type: " + locator);
        }
      }

      @Override
      public Transcoder<Object> getDefaultTranscoder() {
        return transcoder == null ?
                super.getDefaultTranscoder() : transcoder;
      }

      @Override
      public Transcoder<Object> getDefaultCollectionTranscoder() {
        return collectionTranscoder == null ?
                super.getDefaultCollectionTranscoder() : collectionTranscoder;
      }

      @Override
      public FailureMode getFailureMode() {
        return failureMode == null ?
                super.getFailureMode() : failureMode;
      }

      @Override
      public HashAlgorithm getHashAlg() {
        return hashAlg == null ? super.getHashAlg() : hashAlg;
      }

      @Override
      public Collection<ConnectionObserver> getInitialObservers() {
        return initialObservers;
      }

      @Override
      public OperationFactory getOperationFactory() {
        return opFact == null ? super.getOperationFactory() : opFact;
      }

      @Override
      public long getOperationTimeout() {
        return opTimeout == -1 ?
                super.getOperationTimeout() : opTimeout;
      }

      @Override
      public int getReadBufSize() {
        return readBufSize == -1 ?
                super.getReadBufSize() : readBufSize;
      }

      @Override
      public boolean isDaemon() {
        return isDaemon;
      }

      @Override
      public boolean shouldOptimize() {
        return shouldOptimize;
      }

      @Override
      public boolean useNagleAlgorithm() {
        return useNagle;
      }

      @Override
      public boolean getKeepAlive() {
        return keepAlive;
      }

      @Override
      public boolean getDnsCacheTtlCheck() {
        return dnsCacheTtlCheck;
      }

      @Override
      public long getMaxReconnectDelay() {
        return maxReconnectDelay;
      }

      @Override
      public AuthDescriptor getAuthDescriptor() {
        return authDescriptor;
      }

      @Override
      public long getOpQueueMaxBlockTime() {
        return opQueueMaxBlockTime > -1 ? opQueueMaxBlockTime
                : super.getOpQueueMaxBlockTime();
      }

      @Override
      public int getTimeoutExceptionThreshold() {
        return timeoutExceptionThreshold;
      }

      @Override
      public int getTimeoutDurationThreshold() {
        return timeoutDurationThreshold;
      }

      @Override
      public int getMaxFrontCacheElements() {
        return maxFrontCacheElements;
      }

      @Override
      public int getFrontCacheExpireTime() {
        return frontCacheExpireTime;
      }

      @Override
      public String getFrontCacheName() {
        if (frontCacheName == null) {
          frontCacheName = "ArcusFrontCache_" + this.hashCode();
        }
        return frontCacheName;
      }

      @Override
      @SuppressWarnings("deprecation")
      @Deprecated
      public boolean getFrontCacheCopyOnRead() {
        return DefaultConnectionFactory.DEFAULT_FRONT_CACHE_COPY_ON_READ;
      }

      @Override
      @SuppressWarnings("deprecation")
      @Deprecated
      public boolean getFrontCacheCopyOnWrite() {
        return DefaultConnectionFactory.DEFAULT_FRONT_CACHE_COPY_ON_WRITE;
      }

      @Override
      public int getDefaultMaxSMGetKeyChunkSize() {
        return maxSMGetChunkSize;
      }

      @Override
      public byte getDelimiter() {
        return delimiter;
      }

      /* ENABLE_REPLICATION if */
      @Override
      public ReadPriority getReadPriority() {
        return readPriority;
      }

      @Override
      public ReadPriority getAPIReadPriority(APIType apiType) {
        ReadPriority readPriority = apiReadPriorityList.get(apiType);
        if (readPriority == null) {
          return this.getReadPriority();
        }
        return readPriority;
      }
      /* ENABLE_REPLICATION end */
    };
  }

  /**
   * Type of protocol to use for connections.
   */
  public enum Protocol {
    /**
     * Use the text (ascii) protocol.
     */
    TEXT,
    /**
     * Use the binary protocol.
     */
    BINARY
  }

  /**
   * Type of node locator to use.
   */
  public enum Locator {
    /**
     * Array modulus - the classic node location algorithm.
     */
    ARRAY_MOD,
    /**
     * Fixed Consistent hash algorithm.
     *
     * This uses ketema's distribution algorithm, but may be used with any
     * hash algorithm.
     */
    CONSISTENT,
    /**
     * Live Consistent hash algorithm
     *
     * This uses ketama's distribution algorithm, and used with
     * node change(add, delete)
     */
    ARCUSCONSISTENT
  }
}
