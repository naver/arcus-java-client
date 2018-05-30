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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import junit.framework.TestCase;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.transcoders.Transcoder;

public abstract class ClientBaseCase extends TestCase {

  protected static String ZK_HOST = System.getProperty("ZK_HOST",
          "127.0.0.1:2181");

  protected static String ZK_SERVICE_ID = System.getProperty("ZK_SERVICE_ID",
          "test");

  protected static String ARCUS_HOST = System
          .getProperty("ARCUS_HOST",
                  "127.0.0.1:11211");

  protected static boolean USE_ZK = Boolean.valueOf(System.getProperty(
          "USE_ZK", "false"));

  protected static boolean SHUTDOWN_AFTER_EACH_TEST = USE_ZK;

  static {
    System.out.println("---------------------------------------------");
    System.out.println("[ArcusClient initialization info.]");
    System.out.println("USE_ZK=" + USE_ZK);
    System.out.println("SHUTDOWN_AFTER_EACH_TEST=" + USE_ZK);
    if (USE_ZK) {
      System.out.println("ZK_HOST=" + ZK_HOST + ", ZK_SERVICE_ID="
              + ZK_SERVICE_ID);
    } else {
      System.out.println("ARCUS_HOST=" + ARCUS_HOST);
    }
    System.out.println("---------------------------------------------");
  }

  protected MemcachedClient client = null;

  private static class CFB extends ConnectionFactoryBuilder {

    private final ConnectionFactory inner;
    private Collection<ConnectionObserver> observers = Collections.emptyList();

    public CFB(ConnectionFactory cf) {
      this.inner = cf;
    }

    @Override
    public ConnectionFactory build() {
      return new ConnectionFactory() {
        /*
         * CAUTION! Never override this createConnection() method, or your code could not
         * work as you expected. See https://github.com/jam2in/arcus-java-client/issue/4
         */
        @Override
        public MemcachedConnection createConnection(
                List<InetSocketAddress> addrs) throws IOException {
          return new MemcachedConnection(getReadBufSize(), this, addrs,
                  getInitialObservers(), getFailureMode(), getOperationFactory());
        }

        @Override
        public MemcachedNode createMemcachedNode(SocketAddress sa,
                                                 SocketChannel c, int bufSize) {
          return inner.createMemcachedNode(sa, c, bufSize);
        }

        @Override
        public BlockingQueue<Operation> createOperationQueue() {
          return inner.createOperationQueue();
        }

        @Override
        public BlockingQueue<Operation> createReadOperationQueue() {
          return inner.createReadOperationQueue();
        }

        @Override
        public BlockingQueue<Operation> createWriteOperationQueue() {
          return inner.createWriteOperationQueue();
        }

        @Override
        public long getOpQueueMaxBlockTime() {
          return inner.getOpQueueMaxBlockTime();
        }

        @Override
        public NodeLocator createLocator(List<MemcachedNode> nodes) {
          return inner.createLocator(nodes);
        }

        @Override
        public OperationFactory getOperationFactory() {
          return inner.getOperationFactory();
        }

        @Override
        public long getOperationTimeout() {
          return inner.getOperationTimeout();
        }

        @Override
        public boolean isDaemon() {
          return inner.isDaemon();
        }

        @Override
        public boolean useNagleAlgorithm() {
          return inner.useNagleAlgorithm();
        }

        @Override
        public Collection<ConnectionObserver> getInitialObservers() {
          return observers;
        }

        @Override
        public FailureMode getFailureMode() {
          return inner.getFailureMode();
        }

        @Override
        public Transcoder<Object> getDefaultTranscoder() {
          return inner.getDefaultTranscoder();
        }

        @Override
        public Transcoder<Object> getDefaultCollectionTranscoder() {
          return inner.getDefaultCollectionTranscoder();
        }

        @Override
        public boolean shouldOptimize() {
          return inner.shouldOptimize();
        }

        @Override
        public int getReadBufSize() {
          return inner.getReadBufSize();
        }

        @Override
        public HashAlgorithm getHashAlg() {
          return inner.getHashAlg();
        }

        @Override
        public long getMaxReconnectDelay() {
          return inner.getMaxReconnectDelay();
        }

        @Override
        public AuthDescriptor getAuthDescriptor() {
          return inner.getAuthDescriptor();
        }

        @Override
        public int getTimeoutExceptionThreshold() {
          return inner.getTimeoutExceptionThreshold();
        }

        @Override
        public int getTimeoutRatioThreshold() {
          return inner.getTimeoutRatioThreshold();
        }

        @Override
        public int getMaxFrontCacheElements() {
          return inner.getMaxFrontCacheElements();
        }

        @Override
        public String getFrontCacheName() {
          return inner.getFrontCacheName();
        }

        @Override
        public boolean getFrontCacheCopyOnRead() {
          return inner.getFrontCacheCopyOnRead();
        }

        @Override
        public boolean getFrontCacheCopyOnWrite() {
          return inner.getFrontCacheCopyOnWrite();
        }

        @Override
        public int getFrontCacheExpireTime() {
          return inner.getFrontCacheExpireTime();
        }

        @Override
        public int getBulkServiceThreadCount() {
          return inner.getBulkServiceThreadCount();
        }

        @Override
        public int getBulkServiceLoopLimit() {
          return inner.getBulkServiceLoopLimit();
        }

        @Override
        public long getBulkServiceSingleOpTimeout() {
          return inner.getBulkServiceSingleOpTimeout();
        }

        @Override
        public int getDefaultMaxSMGetKeyChunkSize() {
          return inner.getDefaultMaxSMGetKeyChunkSize();
        }

        @Override
        public byte getDelimiter() {
          return inner.getDelimiter();
        }

        /* ENABLE_REPLICATION if */

        @Override
        public ReadPriority getReadPriority() {
          return inner.getReadPriority();
        }

        @Override
        public Map<APIType, ReadPriority> getAPIReadPriority() {
          return inner.getAPIReadPriority();
        }
        /* ENABLE_REPLICATION end */
      };
    }

    @Override
    public ConnectionFactoryBuilder setInitialObservers(
            Collection<ConnectionObserver> obs) {
      this.observers = obs;
      return this;
    }
  }

  protected void initClient() throws Exception {
    initClient(new DefaultConnectionFactory() {
      @Override
      public long getOperationTimeout() {
        return 15000;
      }

      @Override
      public FailureMode getFailureMode() {
        return FailureMode.Retry;
      }
    });
  }

  protected void initClient(ConnectionFactory cf) throws Exception {
    if (USE_ZK) {
      openFromZK(new CFB(cf));
    } else {
      openDirect(cf);
    }
  }

  protected void openFromZK(ConnectionFactoryBuilder cfb) {
    client = ArcusClient.createArcusClient(ZK_HOST, ZK_SERVICE_ID, cfb);
  }

  protected void openDirect(ConnectionFactory cf) throws Exception {
    client = new ArcusClient(cf, AddrUtil.getAddresses(ARCUS_HOST));
  }

  protected Collection<String> stringify(Collection<?> c) {
    Collection<String> rv = new ArrayList<String>();
    for (Object o : c) {
      rv.add(String.valueOf(o));
    }
    return rv;
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    initClient();
  }

  @Override
  protected void tearDown() throws Exception {
    // Shut down, start up, flush, and shut down again. Error tests have
    // unpredictable timing issues.
    client.shutdown();
    client = null;
    initClient();
    flushPause();
    assertTrue(client.flush().get());
    client.shutdown();
    client = null;
    super.tearDown();
  }

  protected void flushPause() throws InterruptedException {
    // nothing useful
  }

}
