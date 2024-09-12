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
  //private long maxReconnectDelay =
  //DefaultConnectionFactory.DEFAULT_MAX_RECONNECT_DELAY;
  private long maxReconnectDelay = 1;

  private int readBufSize = -1;
  private HashAlgorithm hashAlg = HashAlgorithm.KETAMA_HASH;
  private AuthDescriptor authDescriptor = null;
  private long opQueueMaxBlockTime = -1;

  // private int timeoutExceptionThreshold =
  //     DefaultConnectionFactory.DEFAULT_MAX_TIMEOUTEXCEPTION_THRESHOLD;
  private int timeoutExceptionThreshold = 10;
  private int timeoutRatioThreshold = DefaultConnectionFactory.DEFAULT_MAX_TIMEOUTRATIO_THRESHOLD;
  private int timeoutDurationThreshold = 1000;

  private int maxFrontCacheElements = DefaultConnectionFactory.DEFAULT_MAX_FRONTCACHE_ELEMENTS;
  private int frontCacheExpireTime = DefaultConnectionFactory.DEFAULT_FRONTCACHE_EXPIRETIME;
  private String frontCacheName;
  private boolean frontCacheCopyOnRead = DefaultConnectionFactory.DEFAULT_FRONT_CACHE_COPY_ON_READ;
  private boolean frontCacheCopyOnWrite =
      DefaultConnectionFactory.DEFAULT_FRONT_CACHE_COPY_ON_WRITE;

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
  public void internalArcusReplEnabled(boolean b) {
    arcusReplEnabled = b;
  }

  /* @deprecated  This method is no longer needed by applications.
   * The replication cluster is internally determined by checking
   * ZK directories for the given service code. The service code
   * must be unique in each ZK ensemble.
   */
  @Deprecated
  public ConnectionFactoryBuilder setArcusReplEnabled(boolean enable) {
    return this;
  }

  /* @deprecated  This method is no longer needed by applications.
   * The replication cluster is internally determined by checking
   * ZK directories for the given service code. The service code
   * must be unique in each ZK ensemble.
   */
  @Deprecated
  public boolean getArcusReplEnabled() {
    return arcusReplEnabled;
  }
  /* ENABLE_REPLICATION end */

  public FailureMode getFailureMode() {
    return failureMode;
  }

  /**
   * Set the operation queue factory.
   */
  public ConnectionFactoryBuilder setOpQueueFactory(OperationQueueFactory q) {
    opQueueFactory = q;
    return this;
  }

  /**
   * Set the read queue factory.
   */
  public ConnectionFactoryBuilder setReadOpQueueFactory(OperationQueueFactory q) {
    readQueueFactory = q;
    return this;
  }

  /**
   * Set the write queue factory.
   */
  public ConnectionFactoryBuilder setWriteOpQueueFactory(OperationQueueFactory q) {
    writeQueueFactory = q;
    return this;
  }

  /**
   * Set the maximum amount of time (in milliseconds) a client is willing to
   * wait for space to become available in an output queue.
   */
  public ConnectionFactoryBuilder setOpQueueMaxBlockTime(long t) {
    opQueueMaxBlockTime = t;
    return this;
  }

  /**
   * Set the default transcoder.
   */
  public ConnectionFactoryBuilder setTranscoder(Transcoder<Object> t) {
    transcoder = t;
    return this;
  }

  /**
   * Set the default collection transcoder.
   */
  public ConnectionFactoryBuilder setCollectionTranscoder(Transcoder<Object> t) {
    collectionTranscoder = t;
    return this;
  }

  /**
   * Set the failure mode.
   */
  public ConnectionFactoryBuilder setFailureMode(FailureMode fm) {
    failureMode = fm;
    return this;
  }

  /**
   * Set the initial connection observers (will observe initial connection).
   */
  public ConnectionFactoryBuilder setInitialObservers(
          Collection<ConnectionObserver> obs) {
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
    opFact = f;
    return this;
  }

  /**
   * Set the default operation timeout in milliseconds.
   */
  public ConnectionFactoryBuilder setOpTimeout(long t) {
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
    readBufSize = to;
    return this;
  }

  /**
   * Set the hash algorithm.
   */
  public ConnectionFactoryBuilder setHashAlg(HashAlgorithm to) {
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
        assert false : "Unhandled protocol: " + prot;
    }
    return this;
  }

  /**
   * Set the locator type.
   */
  public ConnectionFactoryBuilder setLocatorType(Locator l) {
    locator = l;
    return this;
  }

  /**
   * Set the maximum reconnect delay.
   */
  public ConnectionFactoryBuilder setMaxReconnectDelay(long to) {
    assert to > 0 : "Reconnect delay must be a positive number";
    maxReconnectDelay = to;
    return this;
  }

  /**
   * Set the auth descriptor to enable authentication on new connections.
   */
  public ConnectionFactoryBuilder setAuthDescriptor(AuthDescriptor to) {
    authDescriptor = to;
    return this;
  }

  /**
   * Set the maximum timeout exception threshold
   */
  public ConnectionFactoryBuilder setTimeoutExceptionThreshold(int to) {
    assert to > 1 : "Minimum timeout exception threshold is 2";
    if (to > 1) {
      timeoutExceptionThreshold = to - 2;
    }
    return this;
  }

  /**
   * Set the maximum timeout ratio threshold: 0(disabled, default), 1~99
   */
  public ConnectionFactoryBuilder setTimeoutRatioThreshold(int to) {
    assert (to >= 0 && to < 100) : "Timeout ratio threshold range is 0~99.";
    if (to < 0 || to >= 100) {
      timeoutRatioThreshold = 0; // disable
    } else {
      timeoutRatioThreshold = to;
    }
    return this;
  }

  /**
   * Set the maximum timeout duration threshold
   */
  public ConnectionFactoryBuilder setTimeoutDurationThreshold(int to) {
    assert (to == 0 || to >= 1000 && to <= 5000) :
        "Timeout duration threshold must be 0 or 1000~5000 range.";
    timeoutDurationThreshold = to == 0 ? 0 : // 0 is disable
        Math.max(Math.min(to, 5000), 1000);
    return this;
  }

  /**
   * Set the maximum number of front cache elements.
   */
  public ConnectionFactoryBuilder setMaxFrontCacheElements(int to) {
    assert to > 0 : "In case of front cache, the number must be a positive number";
    maxFrontCacheElements = to;
    return this;
  }

  /**
   * Set front cache's expire time.
   */
  public ConnectionFactoryBuilder setFrontCacheExpireTime(int to) {
    assert to > 0 : "Front cache's expire time must be a positive number";
    frontCacheExpireTime = to;
    return this;
  }

  /**
   * Set front cache copyOnRead property
   */
  public ConnectionFactoryBuilder setFrontCacheCopyOnRead(boolean copyOnRead) {
    frontCacheCopyOnRead = copyOnRead;
    return this;
  }

  /**
   * Set front cache copyOnWrite property
   */
  public ConnectionFactoryBuilder setFrontCacheCopyOnWrite(boolean copyOnWrite) {
    frontCacheCopyOnWrite = copyOnWrite;
    return this;
  }

  /**
   * Set max smget key chunk size
   */
  public ConnectionFactoryBuilder setMaxSMGetKeyChunkSize(int size) {
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
    readPriority = priority;
    return this;
  }

  public ConnectionFactoryBuilder setAPIReadPriority(APIType apiType, ReadPriority readPriority) {
    OperationType type = apiType.getAPIOpType();

    if (type == OperationType.READ || type == OperationType.RW) {
      this.apiReadPriorityList.put(apiType, readPriority);
    }

    return this;
  }

  public ConnectionFactoryBuilder setAPIReadPriority(Map<APIType, ReadPriority> apiList) {
    this.apiReadPriorityList.clear();

    for (Map.Entry<APIType, ReadPriority> entry : apiList.entrySet()) {
      OperationType type = entry.getKey().getAPIOpType();
      if (type == OperationType.READ || type == OperationType.RW) {
        this.apiReadPriorityList.put(entry.getKey(), entry.getValue());
      }
    }

    return this;
  }

  public ConnectionFactoryBuilder clearAPIReadPriority() {
    this.apiReadPriorityList.clear();

    return this;
  }

  public ReadPriority getAPIReadPriority(APIType apiType) {
    ReadPriority priority = this.apiReadPriorityList.get(apiType);

    return priority != null ? priority : ReadPriority.MASTER;
  }

  public Map<APIType, ReadPriority> getAPIReadPriority() {
    return this.apiReadPriorityList;
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
                super.createReadOperationQueue()
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
      public int getTimeoutRatioThreshold() {
        return timeoutRatioThreshold;
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
      public boolean getFrontCacheCopyOnRead() {
        return frontCacheCopyOnRead;
      }

      @Override
      public boolean getFrontCacheCopyOnWrite() {
        return frontCacheCopyOnWrite;
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
      public Map<APIType, ReadPriority> getAPIReadPriority() {
        return apiReadPriorityList;
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
