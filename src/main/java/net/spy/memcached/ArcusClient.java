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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import net.spy.memcached.collection.Attributes;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.BTreeCount;
import net.spy.memcached.collection.BTreeCreate;
import net.spy.memcached.collection.BTreeDelete;
import net.spy.memcached.collection.BTreeElement;
import net.spy.memcached.collection.BTreeFindPosition;
import net.spy.memcached.collection.BTreeFindPositionWithGet;
import net.spy.memcached.collection.BTreeGet;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeGetBulkWithByteTypeBkey;
import net.spy.memcached.collection.BTreeGetBulkWithLongTypeBkey;
import net.spy.memcached.collection.BTreeGetByPosition;
import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.collection.BTreeMutate;
import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkeyOld;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkeyOld;
import net.spy.memcached.collection.BTreeStore;
import net.spy.memcached.collection.BTreeStoreAndGet;
import net.spy.memcached.collection.BTreeUpdate;
import net.spy.memcached.collection.BTreeUpsert;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.ByteArrayTreeMap;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionBulkStore;
import net.spy.memcached.collection.CollectionCount;
import net.spy.memcached.collection.CollectionCreate;
import net.spy.memcached.collection.CollectionDelete;
import net.spy.memcached.collection.CollectionExist;
import net.spy.memcached.collection.CollectionGet;
import net.spy.memcached.collection.CollectionMutate;
import net.spy.memcached.collection.CollectionPipedStore;
import net.spy.memcached.collection.CollectionPipedStore.BTreePipedStore;
import net.spy.memcached.collection.CollectionPipedStore.ByteArraysBTreePipedStore;
import net.spy.memcached.collection.CollectionPipedStore.ListPipedStore;
import net.spy.memcached.collection.CollectionPipedStore.SetPipedStore;
import net.spy.memcached.collection.CollectionPipedStore.MapPipedStore;
import net.spy.memcached.collection.CollectionPipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.BTreePipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.MapPipedUpdate;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.CollectionStore;
import net.spy.memcached.collection.CollectionUpdate;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.ListCreate;
import net.spy.memcached.collection.ListDelete;
import net.spy.memcached.collection.ListGet;
import net.spy.memcached.collection.ListStore;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.collection.MapCreate;
import net.spy.memcached.collection.MapDelete;
import net.spy.memcached.collection.MapGet;
import net.spy.memcached.collection.MapStore;
import net.spy.memcached.collection.MapUpdate;
import net.spy.memcached.collection.RangeGet;
import net.spy.memcached.collection.SetCreate;
import net.spy.memcached.collection.SetDelete;
import net.spy.memcached.collection.SetExist;
import net.spy.memcached.collection.SetGet;
import net.spy.memcached.collection.SetPipedExist;
import net.spy.memcached.collection.SetStore;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.internal.BTreeStoreAndGetFuture;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.CollectionGetBulkFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.BTreeFindPositionOperation;
import net.spy.memcached.ops.BTreeFindPositionWithGetOperation;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.BTreeGetByPositionOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperationOld;
import net.spy.memcached.ops.BTreeStoreAndGetOperation;
import net.spy.memcached.ops.CollectionBulkStoreOperation;
import net.spy.memcached.ops.CollectionGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.CollectionPipedExistOperation;
import net.spy.memcached.ops.CollectionPipedStoreOperation;
import net.spy.memcached.ops.CollectionPipedUpdateOperation;
import net.spy.memcached.ops.GetAttrOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.RangeGetOperation;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.plugin.FrontCacheMemcachedClient;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.BTreeUtil;

/**
 * Client to a Arcus.
 *
 * <h2>Basic usage</h2>
 *
 * <pre>
 * final static String arcusAdminAddrs = &quot;127.0.0.1:2181&quot;;
 * final static String serviceCode = &quot;cafe&quot;;
 *
 * ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
 *
 * ArcusClient c = ArcusClient.createArcusClient(arcusAdminAddrs, serviceCode, cfb);
 *
 * // Store a value (async) for one hour
 * c.set(&quot;someKey&quot;, 3600, someObject);
 * // Retrieve a value.
 * Future&lt;Object&gt; myFuture = c.asyncGet(&quot;someKey&quot;);
 *
 * If pool style is needed, it will be used as follows
 *
 * int poolSize = 4;
 * ArcusClientPool pool = ArcusClient.createArcusClientPool(arcusAdminAddrs, serviceCode, cfb, poolSize);
 *
 * // Store a value
 * pool.set(&quot;someKey&quot;, 3600, someObject);
 * // Retrieve a value
 * Future&lt;Object&gt; myFuture = pool.asyncGet(&quot;someKey&quot;);
 *
 * </pre>
 */
public class ArcusClient extends FrontCacheMemcachedClient implements ArcusClientIF {

  static String VERSION;
  static Logger arcusLogger = LoggerFactory.getLogger("net.spy.memcached");
  static final String ARCUS_CLOUD_ADDR = "127.0.0.1:2181";
  public boolean dead;

  final BulkService bulkService;
  final Transcoder<Object> collectionTranscoder;

  final int smgetKeyChunkSize;

  static final int BOPGET_BULK_CHUNK_SIZE = 200;
  static final int NON_PIPED_BULK_INSERT_CHUNK_SIZE = 500;

  static final int MAX_GETBULK_KEY_COUNT = 200;
  static final int MAX_GETBULK_ELEMENT_COUNT = 50;
  static final int MAX_SMGET_COUNT = 1000; // server configuration is 2000.
  private static final int MAX_MKEY_LENGTH = 250;

  private CacheManager cacheManager;

  public void setCacheManager(CacheManager cacheManager) {
    this.cacheManager = cacheManager;
  }

  /**
   * @param hostPorts   arcus admin addresses
   * @param serviceCode service code
   * @param cfb         ConnectionFactoryBuilder
   * @return a single ArcusClient
   */
  public static ArcusClient createArcusClient(String hostPorts, String serviceCode,
                                              ConnectionFactoryBuilder cfb) {

    return ArcusClient.createArcusClient(hostPorts, serviceCode, cfb, 1, 10000).getClient();

  }

  /**
   * @param serviceCode service code
   * @param cfb         ConnectionFactoryBuilder
   * @return a single ArcusClient
   */
  public static ArcusClient createArcusClient(String serviceCode,
                                              ConnectionFactoryBuilder cfb) {

    return ArcusClient.createArcusClient(ARCUS_CLOUD_ADDR, serviceCode, cfb, 1, 10000).getClient();

  }

  /**
   * @param hostPorts   arcus admin addresses
   * @param serviceCode service code
   * @param poolSize    Arcus clinet pool size
   * @param cfb         ConnectionFactoryBuilder
   * @return multiple ArcusClient
   */
  public static ArcusClientPool createArcusClientPool(String hostPorts, String serviceCode,
                                                      ConnectionFactoryBuilder cfb, int poolSize) {

    return ArcusClient.createArcusClient(hostPorts, serviceCode, cfb, poolSize, 0);

  }

  /**
   * @param serviceCode service code
   * @param poolSize    Arcus clinet pool size
   * @param cfb         ConnectionFactoryBuilder
   * @return multiple ArcusClient
   */
  public static ArcusClientPool createArcusClientPool(String serviceCode,
                                                      ConnectionFactoryBuilder cfb, int poolSize) {

    return ArcusClient.createArcusClient(ARCUS_CLOUD_ADDR, serviceCode, cfb, poolSize, 0);

  }

  /**
   * @param hostPorts   arcus admin addresses
   * @param serviceCode service code
   * @param cfb         ConnectionFactoryBuilder
   * @param poolSize    Arcus clinet pool size
   * @param waitTimeFor Connect
   *                    waiting time for connection establishment(milliseconds)
   * @return multiple ArcusClient
   */
  private static ArcusClientPool createArcusClient(String hostPorts, String serviceCode,
                                                   ConnectionFactoryBuilder cfb, int poolSize, int waitTimeForConnect) {

    if (hostPorts == null) {
      throw new NullPointerException("Arcus admin address required.");
    }
    if (serviceCode == null) {
      throw new NullPointerException("Service code required.");
    }
    if (hostPorts.isEmpty()) {
      throw new IllegalArgumentException("Arcus admin address is empty.");
    }
    if (serviceCode.isEmpty()) {
      throw new IllegalArgumentException("Service code is empty.");
    }

    if (VERSION == null) {
      VERSION = getVersion();
    }

    final CountDownLatch latch = new CountDownLatch(1);

    CacheManager exe = new CacheManager(
            hostPorts, serviceCode, cfb, latch, poolSize,
            waitTimeForConnect);

    try {
      latch.await();
    } catch (Exception e) {
      arcusLogger.warn("you cannot see this message!");
    }

    ArcusClient[] client = exe.getAC();

    return new ArcusClientPool(poolSize, client);
  }

  /**
   * Create an Arcus client for the given memcached server addresses.
   *
   * @param cf    connection factory to configure connections for this client
   * @param addrs socket addresses for the memcached servers
   * @return Arcus client
   */
  protected static ArcusClient getInstance(ConnectionFactory cf,
                                           List<InetSocketAddress> addrs) throws IOException {
    return new ArcusClient(cf, addrs);
  }

  /**
   * Create an Arcus client for the given memcached server addresses.
   *
   * @param cf    connection factory to configure connections for this client
   * @param addrs socket addresses for the memcached servers
   * @throws IOException if connections cannot be established
   */
  public ArcusClient(ConnectionFactory cf, List<InetSocketAddress> addrs)
          throws IOException {
    super(cf, addrs);
    bulkService = new BulkService(cf.getBulkServiceLoopLimit(),
            cf.getBulkServiceThreadCount(), cf.getBulkServiceSingleOpTimeout());
    collectionTranscoder = new CollectionTranscoder();
    smgetKeyChunkSize = cf.getDefaultMaxSMGetKeyChunkSize();
    registerMbean();
  }

  /**
   * Register mbean for Arcus client statistics.
   */
  private void registerMbean() {
    if ("false".equals(System.getProperty("arcus.mbean", "false")
            .toLowerCase())) {
      getLogger().info("Arcus client statistics MBean is NOT registered.");
      return;
    }

    try {
      StatisticsHandler mbean = new StatisticsHandler(this);
      ArcusMBeanServer.getInstance().registMBean(
              mbean,
              mbean.getClass().getPackage().getName() + ":type="
                      + mbean.getClass().getSimpleName() + "-"
                      + mbean.hashCode());

      getLogger().info("Arcus client statistics MBean is registered.");
    } catch (Exception e) {
      getLogger().warn("Failed to initialize statistics mbean.", e);
    }
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#shutdown()
   */
  @Override
  public void shutdown() {
    super.shutdown();
    // Connect to Arcus server directly, cache manager may be null.
    if (cacheManager != null) {
      cacheManager.shutdown();
    }
    dead = true;
    if (bulkService != null) {
      bulkService.shutdown();
    }
  }

  private void validateMKey(String mkey) {
    byte[] keyBytes = KeyUtil.getKeyBytes(mkey);
    if (keyBytes.length > MAX_MKEY_LENGTH) {
      throw new IllegalArgumentException("MKey is too long (maxlen = "
              + MAX_MKEY_LENGTH + ")");
    }
    if (keyBytes.length == 0) {
      throw new IllegalArgumentException(
              "MKey must contain at least one character.");
    }
    // Validate the mkey
    for (byte b : keyBytes) {
      if (b == ' ' || b == '\n' || b == '\r' || b == 0) {
        throw new IllegalArgumentException(
                "MKey contains invalid characters:  ``" + mkey + "''");
      }
    }
  }

  Future<Boolean> asyncStore(StoreType storeType, String key,
                             int exp, CachedData co) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<Boolean> rv = new OperationFuture<Boolean>(latch,
            operationTimeout);
    Operation op = opFact.store(storeType, key, co.getFlags(),
            exp, co.getData(), new OperationCallback() {
              public void receivedStatus(OperationStatus val) {
                rv.set(val.isSuccess());
              }

              public void complete() {
                latch.countDown();
              }
            });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncSetAttr(java.lang.String, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncSetAttr(String key,
                                                Attributes attrs) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<Boolean>(
            latch, operationTimeout);
    Operation op = opFact.setAttr(key, attrs, new OperationCallback() {
      public void receivedStatus(OperationStatus status) {
        if (status instanceof CollectionOperationStatus) {
          rv.set(status.isSuccess(), (CollectionOperationStatus) status);
        } else {
          getLogger().warn("Unhandled state: " + status);
          rv.set(status.isSuccess(), new CollectionOperationStatus(status));
        }
      }

      public void complete() {
        latch.countDown();
      }
    });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncGetAttr(java.lang.String)
   */
  @Override
  public CollectionFuture<CollectionAttributes> asyncGetAttr(final String key) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<CollectionAttributes> rv = new CollectionFuture<CollectionAttributes>(
            latch, operationTimeout);
    Operation op = opFact.getAttr(key, new GetAttrOperation.Callback() {
      CollectionAttributes attrs = new CollectionAttributes();

      public void receivedStatus(OperationStatus status) {
        CollectionOperationStatus stat;

        if (status instanceof CollectionOperationStatus) {
          stat = (CollectionOperationStatus) status;
        } else {
          stat = new CollectionOperationStatus(status);
        }

        rv.set(stat.isSuccess() ? attrs : null, stat);
      }

      public void complete() {
        latch.countDown();
      }

      public void gotAttribute(String k, String attr) {
        assert key.equals(k) : "Wrong key returned";
        attrs.setAttribute(attr);
      }
    });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  private <T> CollectionFuture<List<T>> asyncRangeGet(final RangeGet rangeGet,
                                                      final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<List<T>> rv = new CollectionFuture<List<T>>(
            latch, operationTimeout);

    Operation op = opFact.rangeget(rangeGet,
            new RangeGetOperation.Callback() {
              List<T> list = new ArrayList<T>();

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                if (cstatus.isSuccess()) {
                  rv.set(list, cstatus);
                  return;
                }
                switch (cstatus.getResponse()) {
                  case NOT_FOUND:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("rangeGet(%s) not found : %s",
                              rangeGet.getRange(), cstatus);
                    }
                    break;
                  case UNREADABLE:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("rangeGet(%s) is not readable : %s",
                              rangeGet.getRange(), cstatus);
                    }
                    break;
                }
              }
              public void complete() { latch.countDown();}

              public void gotData(byte[] key) {
                list.add(tc.decode(new CachedData(0, key, tc.getMaxSize())));
              }
            });
    rv.setOperation(op);
    addOp(rangeGet.getFrkey(), op);
    return rv;
  }

  /**
   * Generic get operation for list items. Public methods for list items call this method.
   *
   * @param k             list item's key
   * @param collectionGet operation parameters (element key and so on)
   * @param tc            transcoder to serialize and unserialize value
   * @return future holding the fetched value
   */
  private <T> CollectionFuture<List<T>> asyncLopGet(final String k,
                                                    final CollectionGet collectionGet, final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<List<T>> rv = new CollectionFuture<List<T>>(
            latch, operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
            new CollectionGetOperation.Callback() {
              List<T> list = new ArrayList<T>();

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                if (cstatus.isSuccess()) {
                  rv.set(list, cstatus);
                  return;
                }
                switch (cstatus.getResponse()) {
                  case NOT_FOUND:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) not found : %s", k,
                              cstatus);
                    }
                    break;
                  case NOT_FOUND_ELEMENT:
                    rv.set(list, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) not found : %s",
                              k, cstatus);
                    }
                    break;
                  case OUT_OF_RANGE:
                    rv.set(list, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) not found in condition : %s",
                              k, cstatus);
                    }
                    break;
                  case UNREADABLE:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) is not readable : %s",
                              k, cstatus);
                    }
                    break;
                  default:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) unknown status : %s",
                              k, cstatus);
                    }
                    break;
                }
              }

              public void complete() {
                latch.countDown();
              }

              public void gotData(String key, String subkey, int flags,
                                  byte[] data) {
                assert key.equals(k) : "Wrong key returned";
                list.add(tc.decode(new CachedData(flags, data, tc
                        .getMaxSize())));
              }
            });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncSopExist(java.lang.String, T, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncSopExist(String key, T value,
                                                     Transcoder<T> tc) {
    SetExist<T> exist = new SetExist<T>(value, tc);
    return asyncCollectionExist(key, "", exist, tc);
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncSopExist(java.lang.String, java.lang.Object)
   */
  @Override
  public CollectionFuture<Boolean> asyncSopExist(String key, Object value) {
    SetExist<Object> exist = new SetExist<Object>(value, collectionTranscoder);
    return asyncCollectionExist(key, "", exist, collectionTranscoder);
  }

  /**
   * Generic get operation for set items. Public methods for set items call this method.
   *
   * @param k             set item's key
   * @param collectionGet operation parameters (element key and so on)
   * @param tc            transcoder to serialize and unserialize value
   * @return future holding the fetched value
   */
  private <T> CollectionFuture<Set<T>> asyncSopGet(final String k,
                                                   final CollectionGet collectionGet, final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Set<T>> rv = new CollectionFuture<Set<T>>(latch,
            operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
            new CollectionGetOperation.Callback() {
              Set<T> set = new HashSet<T>();

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                if (cstatus.isSuccess()) {
                  rv.set(set, cstatus);
                  return;
                }

                switch (cstatus.getResponse()) {
                  case NOT_FOUND:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) not found : %s", k,
                              cstatus);
                    }
                    break;
                  case NOT_FOUND_ELEMENT:
                    rv.set(set, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) not found : %s",
                              k, cstatus);
                    }
                    break;
                  case UNREADABLE:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Collection(%s) is not readable : %s",
                              k, cstatus);
                    }
                    break;
                  default:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) unknown status : %s",
                              k, cstatus);
                    }
                    break;
                }
              }

              public void complete() {
                latch.countDown();
              }

              public void gotData(String key, String subkey, int flags,
                                  byte[] data) {
                assert key.equals(k) : "Wrong key returned";
                set.add(tc.decode(new CachedData(flags, data, tc
                        .getMaxSize())));
              }
            });

    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /**
   * Generic get operation for b+tree items. Public methods for b+tree items call this method.
   *
   * @param k             b+tree item's key
   * @param collectionGet operation parameters (element keys and so on)
   * @param reverse       false=forward or true=backward
   * @param tc            transcoder to serialize and unserialize value
   * @return future holding the map of fetched elements and their keys
   */
  private <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(
          final String k, final CollectionGet collectionGet,
          final boolean reverse, final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<Long, Element<T>>> rv = new CollectionFuture<Map<Long, Element<T>>>(
            latch, operationTimeout);
    Operation op = opFact.collectionGet(k, collectionGet,
            new CollectionGetOperation.Callback() {
              TreeMap<Long, Element<T>> map = new TreeMap<Long, Element<T>>(
                      (reverse) ? Collections.reverseOrder() : null);

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                if (cstatus.isSuccess()) {
                  rv.set(map, cstatus);
                  return;
                }
                switch (cstatus.getResponse()) {
                  case NOT_FOUND:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) not found : %s", k,
                              cstatus);
                    }
                    break;
                  case NOT_FOUND_ELEMENT:
                    rv.set(map, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) not found : %s",
                              k, cstatus);
                    }
                    break;
                  case UNREADABLE:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) is not readable : %s",
                              k, cstatus);
                    }
                    break;
                  default:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) Unknown response : %s",
                              k, cstatus);
                    }
                    break;
                }
              }

              public void complete() {
                latch.countDown();
              }

              public void gotData(String key, String subkey, int flags,
                                  byte[] data) {
                assert key.equals(k) : "Wrong key returned";
                long longSubkey = Long.parseLong(subkey);
                map.put(longSubkey,
                        new Element<T>(longSubkey, tc
                                .decode(new CachedData(flags, data, tc
                                        .getMaxSize())), collectionGet
                                .getElementFlag()));
              }
            });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /**
   * Generic get operation for map items. Public methods for b+tree items call this method.
   *
   * @param k             map item's key
   * @param collectionGet operation parameters (element keys and so on)
   * @param tc            transcoder to serialize and unserialize value
   * @return future holding the map of fetched elements and their keys
   */
  private <T> CollectionFuture<Map<String, T>> asyncMopGet(
          final String k, final CollectionGet collectionGet, final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<String, T>> rv = new CollectionFuture<Map<String, T>>(
            latch, operationTimeout);
    Operation op = opFact.collectionGet(k, collectionGet,
            new CollectionGetOperation.Callback() {
              HashMap<String, T> map = new HashMap<String, T>();

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                if (cstatus.isSuccess()) {
                  rv.set(map, cstatus);
                  return;
                }
                switch (cstatus.getResponse()) {
                  case NOT_FOUND:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) not found : %s", k,
                              cstatus);
                    }
                    break;
                  case NOT_FOUND_ELEMENT:
                    rv.set(map, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) not found : %s",
                              k, cstatus);
                    }
                    break;
                  case UNREADABLE:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) is not readable : %s",
                              k, cstatus);
                    }
                    break;
                  default:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) Unknown response : %s",
                              k, cstatus);
                    }
                    break;
                }
              }

              public void complete() {
                latch.countDown();
              }

              public void gotData(String key, String subkey, int flags,
                                  byte[] data) {
                assert key.equals(k) : "Wrong key returned";
                map.put(subkey, tc.decode(new CachedData(flags, data, tc
                        .getMaxSize())));
              }
            });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /**
   * Generic store operation for collection items. Public methods for collection items call this method.
   *
   * @param key             collection item's key
   * @param subkey          element key (list index, b+tree bkey)
   * @param collectionStore operation parameters (value, eflags, attributes, and so on)
   * @param tc              transcoder to serialize and unserialize value
   * @return future holding the success/failure of the operation
   */
  private <T> CollectionFuture<Boolean> asyncCollectionStore(String key,
                                                             String subkey, CollectionStore<T> collectionStore, Transcoder<T> tc) {
    CachedData co = tc.encode(collectionStore.getValue());
    collectionStore.setFlags(co.getFlags());
    return asyncCollectionStore(key, subkey, collectionStore, co);
  }

  /**
   * Generic store operation for collection items. Public methods for collection items call this method.
   *
   * @param key             collection item's key
   * @param subkey          element key (list index, b+tree bkey)
   * @param collectionStore operation parameters (value, eflags, attributes, and so on)
   * @param co              transcoded value
   * @return future holding the success/failure of the operation
   */
  <T> CollectionFuture<Boolean> asyncCollectionStore(final String key,
                                                     final String subkey, final CollectionStore<T> collectionStore,
                                                     final CachedData co) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<Boolean>(
            latch, operationTimeout);
    Operation op = opFact.collectionStore(key, subkey, collectionStore,
            co.getData(), new OperationCallback() {
              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(cstatus.isSuccess(), cstatus);
                if (!cstatus.isSuccess()
                        && getLogger().isDebugEnabled()) {
                  getLogger().debug(
                          "Insertion to the collection failed : "
                                  + cstatus.getMessage()
                                  + " (type="
                                  + collectionStore.getClass()
                                  .getName() + ", key=" + key
                                  + ", subkey=" + subkey + ", value="
                                  + collectionStore.getValue() + ")");
                }
              }

              public void complete() {
                latch.countDown();
              }
            });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /**
   * Generic pipelined store operation for collection items. Public methods for collection items call this method.
   *
   * @param key   collection item's key
   * @param store operation parameters (values, attributes, and so on)
   * @return future holding the success/failure codes of individual operations and their index
   */
  <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncCollectionPipedStore(
          final String key, final CollectionPipedStore<T> store) {

    if (store.getItemCount() == 0) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }
    if (store.getItemCount() > CollectionPipedStore.MAX_PIPED_ITEM_COUNT) {
      throw new IllegalArgumentException(
              "The number of piped operations must not exceed a maximum of "
                      + CollectionPipedStore.MAX_PIPED_ITEM_COUNT + ".");
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<Integer, CollectionOperationStatus>> rv =
            new CollectionFuture<Map<Integer, CollectionOperationStatus>>(latch, operationTimeout);

    Operation op = opFact.collectionPipedStore(key, store,
            new CollectionPipedStoreOperation.Callback() {
              Map<Integer, CollectionOperationStatus> result =
                      new TreeMap<Integer, CollectionOperationStatus>();

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(result, cstatus);
              }

              public void complete() {
                latch.countDown();
              }

              public void gotStatus(Integer index, OperationStatus status) {
                if (status instanceof CollectionOperationStatus) {
                  result.put(index, (CollectionOperationStatus) status);
                } else {
                  result.put(index, new CollectionOperationStatus(status));
                }
              }
            });

    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /**
   * Generic pipelined update operation for collection items. Public methods for collection items call this method.
   *
   * @param key    collection item's key
   * @param update operation parameters (values and so on)
   * @return future holding the success/failure codes of individual operations and their index
   */
  <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncCollectionPipedUpdate(
          final String key, final CollectionPipedUpdate<T> update) {

    if (update.getItemCount() == 0) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }
    if (update.getItemCount() > CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT) {
      throw new IllegalArgumentException(
              "The number of piped operations must not exceed a maximum of "
                      + CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT + ".");
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<Integer, CollectionOperationStatus>> rv = new CollectionFuture<Map<Integer, CollectionOperationStatus>>(
            latch, operationTimeout);

    Operation op = opFact.collectionPipedUpdate(key, update,
            new CollectionPipedUpdateOperation.Callback() {
              Map<Integer, CollectionOperationStatus> result = new TreeMap<Integer, CollectionOperationStatus>();

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(result, cstatus);
              }

              public void complete() {
                latch.countDown();
              }

              public void gotStatus(Integer index, OperationStatus status) {
                if (status instanceof CollectionOperationStatus) {
                  result.put(index,
                          (CollectionOperationStatus) status);
                } else {
                  result.put(index, new CollectionOperationStatus(
                          status));
                }
              }
            });

    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /**
   * Generic pipelined update operation for collection items. Public methods for collection items call this method.
   *
   * @param key        collection item's key
   * @param updateList list of operation parameters (values and so on)
   * @return future holding the success/failure codes of individual operations and their index
   */
  <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncCollectionPipedUpdate(
          final String key, final List<CollectionPipedUpdate<T>> updateList) {

    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();

    final CountDownLatch latch = new CountDownLatch(updateList.size());

    final List<OperationStatus> mergedOperationStatus = Collections
            .synchronizedList(new ArrayList<OperationStatus>(1));

    final Map<Integer, CollectionOperationStatus> mergedResult = new ConcurrentHashMap<Integer, CollectionOperationStatus>();

    for (int i = 0; i < updateList.size(); i++) {
      final CollectionPipedUpdate<T> update = updateList.get(i);
      final int idx = i;

      Operation op = opFact.collectionPipedUpdate(key, update,
              new CollectionPipedUpdateOperation.Callback() {
                // each result status
                public void receivedStatus(OperationStatus status) {
                  CollectionOperationStatus cstatus;

                  if (status instanceof CollectionOperationStatus) {
                    cstatus = (CollectionOperationStatus) status;
                  } else {
                    getLogger().warn("Unhandled state: " + status);
                    cstatus = new CollectionOperationStatus(status);
                  }
                  mergedOperationStatus.add(cstatus);
                }

                // complete
                public void complete() {
                  latch.countDown();
                }

                // got status
                public void gotStatus(Integer index,
                                      OperationStatus status) {
                  if (status instanceof CollectionOperationStatus) {
                    mergedResult
                            .put(index
                                            + (idx * CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT),
                                    (CollectionOperationStatus) status);
                  } else {
                    mergedResult
                            .put(index
                                            + (idx * CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT),
                                    new CollectionOperationStatus(
                                            status));
                  }
                }
              });
      addOp(key, op);
      ops.add(op);
    }

    return new CollectionFuture<Map<Integer, CollectionOperationStatus>>(
            latch, operationTimeout) {

      @Override
      public boolean cancel(boolean ign) {
        boolean rv = false;
        for (Operation op : ops) {
          op.cancel("by application.");
          rv |= op.getState() == OperationState.WRITING;
        }
        return rv;
      }

      @Override
      public boolean isCancelled() {
        for (Operation op : ops) {
          if (op.isCancelled())
            return true;
        }
        return false;
      }

      @Override
      public Map<Integer, CollectionOperationStatus> get(long duration,
                                                         TimeUnit units) throws InterruptedException,
              TimeoutException, ExecutionException {

        if (!latch.await(duration, units)) {
          for (Operation op : ops) {
            MemcachedConnection.opTimedOut(op);
          }
          throw new CheckedOperationTimeoutException(
                  "Timed out waiting for operation >" + duration + " " + units, ops);
        } else {
          // continuous timeout counter will be reset
          for (Operation op : ops) {
            MemcachedConnection.opSucceeded(op);
          }
        }

        for (Operation op : ops) {
          if (op != null && op.hasErrored()) {
            throw new ExecutionException(op.getException());
          }

          if (op.isCancelled()) {
            throw new ExecutionException(new RuntimeException(op.getCancelCause()));
          }
        }

        return mergedResult;
      }

      @Override
      public CollectionOperationStatus getOperationStatus() {
        for (OperationStatus status : mergedOperationStatus) {
          if (!status.isSuccess()) {
            return new CollectionOperationStatus(status);
          }
        }
        return new CollectionOperationStatus(true, "END",
                CollectionResponse.END);
      }
    };
  }

  /**
   * Generic delete operation for collection items. Public methods for collection items call this method.
   *
   * @param key              collection item's key
   * @param collectionDelete operation parameters (element index/key, value, and so on)
   * @return future holding the success/failure of the operation
   */
  private CollectionFuture<Boolean> asyncCollectionDelete(
          final String key, final CollectionDelete collectionDelete) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<Boolean>(
            latch, operationTimeout);
    Operation op = opFact.collectionDelete(key, collectionDelete,
            new OperationCallback() {
              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(cstatus.isSuccess(), cstatus);
                if (!cstatus.isSuccess()
                        && getLogger().isDebugEnabled()) {
                  getLogger().debug(
                          "Deletion to the collection failed : "
                                  + cstatus.getMessage()
                                  + " (type="
                                  + collectionDelete.getClass()
                                  .getName() + ", key=" + key
                                  + ")");
                }
              }

              public void complete() {
                latch.countDown();
              }
            });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /**
   * Generic existence operation for collection items. Public methods for collection items call this method.
   *
   * @param key             collection item's key
   * @param subkey          element key (list index, b+tree bkey)
   * @param collectionExist operation parameters (element value and so on)
   * @param tc              transcoder to serialize and unserialize value
   * @return future holding the success/failure of the operation
   */
  private <T> CollectionFuture<Boolean> asyncCollectionExist(
          final String key, final String subkey,
          final CollectionExist collectionExist, Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<Boolean>(
            latch, operationTimeout);
    Operation op = opFact.collectionExist(key, subkey, collectionExist,
            new OperationCallback() {
              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                boolean isExist = (CollectionResponse.EXIST == cstatus
                        .getResponse()) ? true : false;
                rv.set(isExist, cstatus);
                if (!cstatus.isSuccess()
                        && getLogger().isDebugEnabled()) {
                  getLogger().debug(
                          "Exist command to the collection failed : "
                                  + cstatus.getMessage()
                                  + " (type="
                                  + collectionExist.getClass()
                                  .getName() + ", key=" + key
                                  + ", subkey=" + subkey + ")");
                }
              }

              public void complete() {
                latch.countDown();
              }
            });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncSetBulk(java.util.List, int, T, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(List<String> key, int exp, T o, Transcoder<T> tc) {
    if (key == null) {
      throw new IllegalArgumentException("Key list is null.");
    }
    return bulkService.setBulk(key, exp, o, tc, new ArcusClient[]{this});
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncSetBulk(java.util.List, int, java.lang.Object)
   */
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSetBulk(List<String> key, int exp, Object o) {
    if (key == null) {
      throw new IllegalArgumentException("Key list is null.");
    }
    return asyncSetBulk(key, exp, o, transcoder);
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncSetBulk(java.util.Map, int, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(Map<String, T> o, int exp, Transcoder<T> tc) {
    if (o == null) {
      throw new IllegalArgumentException("Map is null.");
    }
    return bulkService.setBulk(o, exp, tc, new ArcusClient[]{this});
  }

  /* (non-Javadoc)
   * @see net.spy.memcached.ArcusClient#asyncSetBulk(java.util.Map, int)
   */
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSetBulk(Map<String, Object> o, int exp) {
    if (o == null) {
      throw new IllegalArgumentException("Map is null.");
    }
    return asyncSetBulk(o, exp, transcoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#getMaxPipedItemCount()
   */
  @Override
  public int getMaxPipedItemCount() {
    return CollectionPipedStore.MAX_PIPED_ITEM_COUNT;
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopCreate(java.lang.String, net.spy.memcached.collection.ElementValueType, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopCreate(String key,
                                                  ElementValueType valueType, CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(valueType);
    boolean noreply = false;
    CollectionCreate bTreeCreate = new BTreeCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getOverflowAction(), attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, bTreeCreate);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopCreate(java.lang.String, net.spy.memcached.collection.ElementValueType, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncMopCreate(String key,
                                                  ElementValueType type, CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(type);
    boolean noreply = false;
    CollectionCreate mapCreate = new MapCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(), attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, mapCreate);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopCreate(java.lang.String, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncSopCreate(String key,
                                                  ElementValueType type, CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(type);
    boolean noreply = false;
    CollectionCreate setCreate = new SetCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(), attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, setCreate);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopCreate(java.lang.String, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncLopCreate(String key,
                                                  ElementValueType type, CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(type);
    boolean noreply = false;
    CollectionCreate listCreate = new ListCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getOverflowAction(), attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, listCreate);
  }

  /**
   * Generic create operation for collection items. Public methods for collection items call this method.
   *
   * @param key              collection item's key
   * @param collectionCreate operation parameters (flags, expiration time, and so on)
   * @return future holding the success/failure of the operation
   */
  CollectionFuture<Boolean> asyncCollectionCreate(final String key,
                                                  final CollectionCreate collectionCreate) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<Boolean>(
            latch, operationTimeout);

    Operation op = opFact.collectionCreate(key, collectionCreate,
            new OperationCallback() {
              @Override
              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(cstatus.isSuccess(), cstatus);
                if (!cstatus.isSuccess()
                        && getLogger().isDebugEnabled()) {
                  getLogger()
                          .debug("Insertion to the collection failed : "
                                  + cstatus.getMessage()
                                  + " (type="
                                  + collectionCreate.getClass()
                                  .getName()
                                  + ", key="
                                  + key
                                  + ", attribute="
                                  + collectionCreate.toString() + ")");
                }
              }

              @Override
              public void complete() {
                latch.countDown();
              }
            });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, long, boolean, boolean)
   */
  @Override
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long bkey, ElementFlagFilter eFlagFilter, boolean withDelete, boolean dropIfEmpty) {
    BTreeGet get = new BTreeGet(bkey, withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopGet(key, get, false, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, long, long, int, int, boolean, boolean)
   */
  @Override
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long from, long to, ElementFlagFilter eFlagFilter, int offset, int count,
                                                                  boolean withDelete, boolean dropIfEmpty) {
    BTreeGet get = new BTreeGet(from, to, offset, count,
            withDelete, dropIfEmpty, eFlagFilter);
    boolean reverse = from > to;
    return asyncBopGet(key, get, reverse, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, long, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long bkey, ElementFlagFilter eFlagFilter, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    BTreeGet get = new BTreeGet(bkey, withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopGet(key, get, false, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, long, long, int, int, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long from, long to, ElementFlagFilter eFlagFilter, int offset, int count,
                                                                 boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    BTreeGet get = new BTreeGet(from, to, offset, count, withDelete,
            dropIfEmpty, eFlagFilter);
    boolean reverse = from > to;
    return asyncBopGet(key, get, reverse, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopGet(java.lang.String, boolean, boolean)
   */
  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           boolean withDelete, boolean dropIfEmpty) {
    List<String> mkeyList = new ArrayList<String>();
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, collectionTranscoder);
  }


  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopGet(java.lang.String, java.lang.String, boolean, boolean)
   */
  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           String mkey, boolean withDelete, boolean dropIfEmpty) {
    if (mkey == null) {
      throw new IllegalArgumentException("mkey is null");
    }
    validateMKey(mkey);
    List<String> mkeyList = new ArrayList<String>(1);
    mkeyList.add(mkey);
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopGet(java.lang.String, java.util.List, boolean, boolean)
   */
  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           List<String> mkeyList, boolean withDelete, boolean dropIfEmpty) {
    if (mkeyList == null) {
      throw new IllegalArgumentException("mkeyList is null");
    }
    for (int i = 0; i < mkeyList.size(); i++) {
      validateMKey(mkeyList.get(i));
    }
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopGet(java.lang.String, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    List<String> mkeyList = new ArrayList<String>();
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopGet(java.lang.String, java.lang.String, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          String mkey, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    if (mkey == null) {
      throw new IllegalArgumentException("mkey is null");
    }
    validateMKey(mkey);
    List<String> mkeyList = new ArrayList<String>(1);
    mkeyList.add(mkey);
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopGet(java.lang.String, java.util.List, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          List<String> mkeyList, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    if (mkeyList == null) {
      throw new IllegalArgumentException("mkeyList is null");
    }
    for (int i = 0; i < mkeyList.size(); i++) {
      validateMKey(mkeyList.get(i));
    }
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, tc);
  }

  @Override
  public CollectionFuture<List<Object>> asyncRangeGet(String frkey, String tokey,
                                                      int count) {
    RangeGet rangeGet = new RangeGet(frkey, tokey, count);
    return asyncRangeGet(rangeGet, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopGet(java.lang.String, int, boolean, boolean)
   */
  @Override
  public CollectionFuture<List<Object>> asyncLopGet(String key, int index,
                                                    boolean withDelete, boolean dropIfEmpty) {
    ListGet get = new ListGet(index, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopGet(java.lang.String, int, int, boolean, boolean)
   */
  @Override
  public CollectionFuture<List<Object>> asyncLopGet(String key, int from,
                                                    int to, boolean withDelete, boolean dropIfEmpty) {
    ListGet get = new ListGet(from, to, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopGet(java.lang.String, int, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int index,
                                                   boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    ListGet get = new ListGet(index, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopGet(java.lang.String, int, int, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int from,
                                                   int to, boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    ListGet get = new ListGet(from, to, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopGet(java.lang.String, int, boolean, boolean)
   */
  @Override
  public CollectionFuture<Set<Object>> asyncSopGet(String key, int count,
                                                   boolean withDelete, boolean dropIfEmpty) {
    SetGet get = new SetGet(count, withDelete, dropIfEmpty);
    return asyncSopGet(key, get, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopGet(java.lang.String, int, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Set<T>> asyncSopGet(String key, int count,
                                                  boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    SetGet get = new SetGet(count, withDelete, dropIfEmpty);
    return asyncSopGet(key, get, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopDelete(java.lang.String, long, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key, long bkey,
                                                  ElementFlagFilter eFlagFilter, boolean dropIfEmpty) {
    BTreeDelete delete = new BTreeDelete(bkey, false,
            dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopDelete(java.lang.String, long, long, int, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key, long from,
                                                  long to, ElementFlagFilter eFlagFilter, int count, boolean dropIfEmpty) {
    BTreeDelete delete = new BTreeDelete(from, to, count,
            false, dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopDelete(java.lang.String, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncMopDelete(String key,
                                                  boolean dropIfEmpty) {
    List<String> mkeyList = new ArrayList<String>();
    MapDelete delete = new MapDelete(mkeyList, false,
            dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopDelete(java.lang.String, java.lang.String, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncMopDelete(String key, String mkey,
                                                  boolean dropIfEmpty) {
    if (mkey == null) {
      throw new IllegalArgumentException("mkey is null");
    }
    validateMKey(mkey);
    List<String> mkeyList = new ArrayList<String>(1);
    mkeyList.add(mkey);
    MapDelete delete = new MapDelete(mkeyList, false,
            dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopDelete(java.lang.String, int, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncLopDelete(String key, int index,
                                                  boolean dropIfEmpty) {
    ListDelete delete = new ListDelete(index, false,
            dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopDelete(java.lang.String, int, int, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncLopDelete(String key, int from,
                                                  int to, boolean dropIfEmpty) {
    ListDelete delete = new ListDelete(from, to, false,
            dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopDelete(java.lang.String, java.lang.Object, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncSopDelete(String key, Object value,
                                                  boolean dropIfEmpty) {
    SetDelete<Object> delete = new SetDelete<Object>(value, false,
            dropIfEmpty, collectionTranscoder);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopDelete(java.lang.String, java.lang.Object, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncSopDelete(String key, T value,
                                                      boolean dropIfEmpty, Transcoder<T> tc) {
    SetDelete<T> delete = new SetDelete<T>(value, false, dropIfEmpty, tc);
    return asyncCollectionDelete(key, delete);
  }

  /**
   * Generic count operation for collection items. Public methods for collection items call this method.
   *
   * @param k               collection item's key
   * @param collectionCount operation parameters (element key range, eflags, and so on)
   * @return future holding the element count
   */
  private CollectionFuture<Integer> asyncCollectionCount(final String k,
                                                         final CollectionCount collectionCount) {

    final CountDownLatch latch = new CountDownLatch(1);

    final CollectionFuture<Integer> rv = new CollectionFuture<Integer>(
            latch, operationTimeout);

    Operation op = opFact.collectionCount(k, collectionCount,
            new OperationCallback() {

              @Override
              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }

                if (cstatus.isSuccess()) {
                  rv.set(new Integer(cstatus.getMessage()),
                          new CollectionOperationStatus(
                                  new OperationStatus(true, "END")));
                  return;
                }

                rv.set(null, cstatus);
              }

              @Override
              public void complete() {
                latch.countDown();
              }
            });

    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGetItemCount(java.lang.String, long, long)
   */
  @Override
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        long from, long to, ElementFlagFilter eFlagFilter) {
    CollectionCount collectionCount = new BTreeCount(from, to, eFlagFilter);
    return asyncCollectionCount(key, collectionCount);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopInsert(java.lang.String, byte[], long, java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                  byte[] eFlag, Object value, CollectionAttributes attributesForCreate) {
    BTreeStore<Object> bTreeStore = new BTreeStore<Object>(value,
            eFlag, (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key, String.valueOf(bkey), bTreeStore,
            collectionTranscoder);
  }


  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopInsert(java.lang.String, java.lang.String, java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                  Object value, CollectionAttributes attributesForCreate) {
    validateMKey(mkey);
    MapStore<Object> mapStore = new MapStore<Object>(value,
            (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key, mkey, mapStore,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopInsert(java.lang.String, int, java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                  Object value, CollectionAttributes attributesForCreate) {
    ListStore<Object> listStore = new ListStore<Object>(value,
            (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key, String.valueOf(index), listStore,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopInsert(java.lang.String, java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncSopInsert(String key, Object value,
                                                  CollectionAttributes attributesForCreate) {
    SetStore<Object> setStore = new SetStore<Object>(value,
            (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key, "", setStore, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopInsert(java.lang.String, long, byte[], java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                      byte[] eFlag, T value, CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    BTreeStore<T> bTreeStore = new BTreeStore<T>(value, eFlag,
            (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key, String.valueOf(bkey), bTreeStore, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopInsert(java.lang.String, java.lang.String, java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                      T value, CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    validateMKey(mkey);
    MapStore<T> mapStore = new MapStore<T>(value,
            (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key, mkey, mapStore, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopInsert(java.lang.String, int, java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                      T value, CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    ListStore<T> listStore = new ListStore<T>(value, (attributesForCreate != null),
            null, attributesForCreate);
    return asyncCollectionStore(key, String.valueOf(index), listStore, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopInsert(java.lang.String, java.lang.Object, boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncSopInsert(String key, T value,
                                                      CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    SetStore<T> setStore = new SetStore<T>(value, (attributesForCreate != null),
            null, attributesForCreate);
    return asyncCollectionStore(key, "", setStore, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopPipedInsertBulk(java.lang.String, java.util.Map, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, Object> elements,
          CollectionAttributes attributesForCreate) {
    return asyncBopPipedInsertBulk(key, elements, attributesForCreate,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopPipedInsertBulk(java.lang.String, java.util.Map, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, Object> elements,
          CollectionAttributes attributesForCreate) {
    return asyncMopPipedInsertBulk(key, elements, attributesForCreate,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopPipedInsertBulk(java.lang.String, int, java.util.List, boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<Object> valueList, CollectionAttributes attributesForCreate) {
    return asyncLopPipedInsertBulk(key, index, valueList, attributesForCreate,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopPipedInsertBulk(java.lang.String, java.util.List, boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<Object> valueList, CollectionAttributes attributesForCreate) {
    return asyncSopPipedInsertBulk(key, valueList, attributesForCreate,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopPipedInsertBulk(java.lang.String, java.util.Map, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, T> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    if (elements.size() <= CollectionPipedStore.MAX_PIPED_ITEM_COUNT) {
      BTreePipedStore<T> store = new BTreePipedStore<T>(key, elements,
              (attributesForCreate != null), attributesForCreate, tc);
      return asyncCollectionPipedStore(key, store);
    } else {
      List<CollectionPipedStore<T>> storeList = new ArrayList<CollectionPipedStore<T>>();

      PartitionedMap<Long, T> list = new PartitionedMap<Long, T>(
              elements, CollectionPipedStore.MAX_PIPED_ITEM_COUNT);

      for (int i = 0; i < list.size(); i++) {
        storeList
                .add(new BTreePipedStore<T>(key, list.get(i),
                        (attributesForCreate != null),
                        attributesForCreate, tc));
      }
      return asyncCollectionPipedStore(key, storeList);
    }
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopPipedInsertBulk(java.lang.String, java.util.Map, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, T> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    for (Map.Entry<String, T> checkMKey
            : elements.entrySet()) {
      validateMKey(checkMKey.getKey());
    }
    if (elements.size() <= CollectionPipedStore.MAX_PIPED_ITEM_COUNT) {
      MapPipedStore<T> store = new MapPipedStore<T>(key, elements,
              (attributesForCreate != null), attributesForCreate, tc);
      return asyncCollectionPipedStore(key, store);
    } else {
      List<CollectionPipedStore<T>> storeList = new ArrayList<CollectionPipedStore<T>>();
      PartitionedMap<String, T> list = new PartitionedMap<String, T>(
              elements, CollectionPipedStore.MAX_PIPED_ITEM_COUNT);

      for (int i = 0; i < list.size(); i++) {
        storeList
                .add(new MapPipedStore<T>(key, list.get(i),
                        (attributesForCreate != null),
                        attributesForCreate, tc));
      }
      return asyncCollectionPipedStore(key, storeList);
    }
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopPipedInsertBulk(java.lang.String, int, java.util.List, boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<T> valueList,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    if (valueList.size() <= CollectionPipedStore.MAX_PIPED_ITEM_COUNT) {
      ListPipedStore<T> store = new ListPipedStore<T>(key, index,
              valueList, (attributesForCreate != null),
              attributesForCreate, tc);
      return asyncCollectionPipedStore(key, store);
    } else {
      PartitionedList<T> list = new PartitionedList<T>(valueList,
              CollectionPipedStore.MAX_PIPED_ITEM_COUNT);

      List<CollectionPipedStore<T>> storeList = new ArrayList<CollectionPipedStore<T>>(
              list.size());

      for (int i = 0; i < list.size(); i++) {
        storeList
                .add(new ListPipedStore<T>(key, index, list.get(i),
                        (attributesForCreate != null),
                        attributesForCreate, tc));
      }
      return asyncCollectionPipedStore(key, storeList);
    }
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopPipedInsertBulk(java.lang.String, java.util.List, boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<T> valueList,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    if (valueList.size() <= CollectionPipedStore.MAX_PIPED_ITEM_COUNT) {
      SetPipedStore<T> store = new SetPipedStore<T>(key, valueList,
              (attributesForCreate != null), attributesForCreate, tc);
      return asyncCollectionPipedStore(key, store);
    } else {
      PartitionedList<T> list = new PartitionedList<T>(valueList,
              CollectionPipedStore.MAX_PIPED_ITEM_COUNT);

      List<CollectionPipedStore<T>> storeList = new ArrayList<CollectionPipedStore<T>>(
              list.size());

      for (int i = 0; i < list.size(); i++) {
        storeList
                .add(new SetPipedStore<T>(key, list.get(i),
                        (attributesForCreate != null),
                        attributesForCreate, tc));
      }

      return asyncCollectionPipedStore(key, storeList);
    }
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#flush(java.lang.String)
   */
  @Override
  public OperationFuture<Boolean> flush(final String prefix) {
    return flush(prefix, -1);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#flush(java.lang.String, int)
   */
  @Override
  public OperationFuture<Boolean> flush(final String prefix, final int delay) {
    final AtomicReference<Boolean> flushResult = new AtomicReference<Boolean>(
            null);
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();

    final CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
      public Operation newOp(final MemcachedNode n,
                             final CountDownLatch latch) {
        Operation op = opFact.flush(prefix, delay, false,
                new OperationCallback() {
                  public void receivedStatus(OperationStatus s) {
                    flushResult.set(s.isSuccess());
                  }

                  public void complete() {
                    latch.countDown();
                  }
                });
        ops.add(op);
        return op;
      }
    });

    return new OperationFuture<Boolean>(blatch, flushResult,
            operationTimeout) {
      @Override
      public boolean cancel(boolean ign) {
        boolean rv = false;
        for (Operation op : ops) {
          op.cancel("by application.");
          rv |= op.getState() == OperationState.WRITING;
        }
        return rv;
      }

      @Override
      public boolean isCancelled() {
        for (Operation op : ops) {
          if (op.isCancelled())
            return true;
        }
        return false;
      }

      @Override
      public Boolean get(long duration, TimeUnit units)
              throws InterruptedException, TimeoutException, ExecutionException {
        if (!blatch.await(duration, units)) {
          // whenever timeout occurs, continuous timeout counter will increase by 1.
          for (Operation op : ops) {
            MemcachedConnection.opTimedOut(op);
          }
          throw new CheckedOperationTimeoutException(
                  "Timed out waiting for operation. >" + duration + " " + units, ops);
        } else {
          // continuous timeout counter will be reset
          for (Operation op : ops) {
            MemcachedConnection.opSucceeded(op);
          }
        }

        for (Operation op : ops) {
          if (op != null && op.hasErrored()) {
            throw new ExecutionException(op.getException());
          }

          if (op != null && op.isCancelled()) {
            throw new ExecutionException(new RuntimeException(op.getCancelCause()));
          }
        }

        return flushResult.get();
      }

      @Override
      public boolean isDone() {
        boolean rv = true;
        for (Operation op : ops) {
          rv &= op.getState() == OperationState.COMPLETE;
        }
        return rv || isCancelled();
      }
    };
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopSortMergeGet(java.util.List, long, long, int, int)
   */
  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter, int offset, int count) {
    if (keyList == null || keyList.isEmpty()) {
      throw new IllegalArgumentException("Key list is empty.");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Offset must be 0 or positive integer.");
    }
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (offset + count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException(
              "The sum of offset and count must not exceed a maximum of " + MAX_SMGET_COUNT + ".");
    }

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<BTreeSMGet<Object>>(
            arrangedKey.size());
    for (List<String> v : arrangedKey.values()) {
      if (arrangedKey.size() > 1) {
        smGetList.add(new BTreeSMGetWithLongTypeBkeyOld<Object>(v, from, to, eFlagFilter, 0, offset + count));
      } else {
        smGetList.add(new BTreeSMGetWithLongTypeBkeyOld<Object>(v, from, to, eFlagFilter, offset, count));
      }
    }
    return smget(smGetList, offset, count, (from > to),
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopSortMergeGet(java.util.List, long, long, int, int, boolean)
   */
  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter,
          int count, SMGetMode smgetMode) {
    if (keyList == null || keyList.isEmpty()) {
      throw new IllegalArgumentException("Key list is empty.");
    }
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException("The count must not exceed a maximum of "
              + MAX_SMGET_COUNT + ".");
    }

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<BTreeSMGet<Object>>(
            arrangedKey.size());
    for (List<String> v : arrangedKey.values()) {
      smGetList.add(new BTreeSMGetWithLongTypeBkey<Object>(v, from, to, eFlagFilter, count, smgetMode));
    }
    return smget(smGetList, count, (from > to),
            collectionTranscoder, smgetMode);
  }

  /**
   * Turn the list of keys into groups of keys.  All keys in a group belong to the same memcached server.
   *
   * @param keyList   list of keys
   * @param groupSize max size of the key group (number of keys)
   * @return map of group name (memcached node + sequence number) and keys in the group
   */
  private Map<String, List<String>> groupingKeys(List<String> keyList, int groupSize) {
    Map<String, Integer> chunkCount = new HashMap<String, Integer>();
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    Set<String> keySet = new HashSet<String>();

    MemcachedConnection conn = getMemcachedConnection();

    for (String k : keyList) {
      validateKey(k);
      if (!keySet.add(k)) {
        throw new IllegalArgumentException("Duplicate keys exist in key list.");
      }
      String node = conn.findNodeByKey(k).getSocketAddress().toString();
      int cc;
      if (chunkCount.containsKey(node)) {
        cc = chunkCount.get(node);
      } else {
        cc = 0;
        chunkCount.put(node, 0);
      }

      String resultKey = node + cc;

      List<String> arrangedKeyList = null;

      if (result.containsKey(resultKey)) {
        if (result.get(resultKey).size() >= groupSize) {
          arrangedKeyList = new ArrayList<String>();
          cc++;
          result.put(node + cc, arrangedKeyList);
          chunkCount.put(node, cc);
        } else {
          arrangedKeyList = result.get(resultKey);
        }
      } else {
        arrangedKeyList = new ArrayList<String>();
        result.put(resultKey, arrangedKeyList);
      }
      arrangedKeyList.add(k);
    }
    return result;
  }

  /**
   * Get the sublist of elements from the smget result.
   *
   * @param mergedResult smget result (list of elements)
   * @param offset       start index, negative offset indicates "start from the tail"
   * @param count        number of elements to get
   * @return list of elements
   */
  private <T> List<SMGetElement<T>> getSubList(
          final List<SMGetElement<T>> mergedResult, int offset, int count) {
    if (mergedResult.size() > count) {
      int toIndex = (count + offset > mergedResult.size()) ? mergedResult
              .size() : count + offset;
      if (offset > toIndex)
        return Collections.emptyList();
      return mergedResult.subList(offset, toIndex);
    } else {
      if (offset > 0) {
        int toIndex = (count + offset > mergedResult.size()) ? mergedResult
                .size() : count + offset;

        if (offset > toIndex)
          return Collections.emptyList();
        return mergedResult.subList(offset, toIndex);
      } else {
        return mergedResult;
      }
    }
  }

  /**
   * Generic smget operation for b+tree items. Public smget methods call this method.
   *
   * @param smGetList smget parameters (keys, eflags, and so on)
   * @param offset    start index of the elements
   * @param count     number of elements to fetch
   * @param reverse   forward or backward
   * @param tc        transcoder to serialize and unserialize element value
   * @return future holding the smget result (elements, return codes, and so on)
   */
  private <T> SMGetFuture<List<SMGetElement<T>>> smget(
          final List<BTreeSMGet<T>> smGetList, final int offset,
          final int count, final boolean reverse, final Transcoder<T> tc) {

    final String END = "END";
    final String TRIMMED = "TRIMMED";
    final String DUPLICATED = "DUPLICATED";
    final String DUPLICATED_TRIMMED = "DUPLICATED_TRIMMED";

    final CountDownLatch blatch = new CountDownLatch(smGetList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();
    final List<String> missedKeyList = Collections.synchronizedList(new ArrayList<String>());
    final Map<String, CollectionOperationStatus> missedKeys =
            Collections.synchronizedMap(new HashMap<String, CollectionOperationStatus>());
    final List<SMGetTrimKey> mergedTrimmedKeys = Collections.synchronizedList(new ArrayList<SMGetTrimKey>());
    final int totalResultElementCount = count + offset;

    final List<SMGetElement<T>> mergedResult = Collections.synchronizedList(new ArrayList<SMGetElement<T>>(totalResultElementCount));

    final ReentrantLock lock = new ReentrantLock();

    final List<OperationStatus> resultOperationStatus = Collections.synchronizedList(new ArrayList<OperationStatus>(1));

    final List<OperationStatus> failedOperationStatus = Collections.synchronizedList(new ArrayList<OperationStatus>(1));

    // if processedSMGetCount is 0, then all smget is done.
    final AtomicInteger processedSMGetCount = new AtomicInteger(smGetList.size());
    final AtomicBoolean mergedTrim = new AtomicBoolean(false);
    final AtomicBoolean stopCollect = new AtomicBoolean(false);

    for (BTreeSMGet<T> smGet : smGetList) {
      Operation op = opFact.bopsmget(smGet, new BTreeSortMergeGetOperationOld.Callback() {
        final List<SMGetElement<T>> eachResult = new ArrayList<SMGetElement<T>>();

        @Override
        public void receivedStatus(OperationStatus status) {
          processedSMGetCount.decrementAndGet();

          if (!status.isSuccess()) {
            getLogger().warn("SMGetFailed. status=%s", status);
            if (!stopCollect.get()) {
              stopCollect.set(true);
              failedOperationStatus.add(status);
            }
            mergedResult.clear();
            return;
          }

          boolean isTrimmed = (TRIMMED.equals(status.getMessage()) ||
                  DUPLICATED_TRIMMED.equals(status.getMessage()))
                  ? true : false;
          lock.lock();
          try {
            if (mergedResult.size() == 0) {
              // merged result is empty, add all.
              mergedResult.addAll(eachResult);
              mergedTrim.set(isTrimmed);
            } else {
              boolean addAll = true;
              int pos = 0;
              for (SMGetElement<T> result : eachResult) {
                for (; pos < mergedResult.size(); pos++) {
                  if ((reverse) ? (0 < result.compareTo(mergedResult.get(pos)))
                          : (0 > result.compareTo(mergedResult.get(pos))))
                    break;
                }
                if (pos >= totalResultElementCount) {
                  addAll = false;
                  break;
                }
                if (pos >= mergedResult.size() && mergedTrim.get() &&
                        result.compareBkeyTo(mergedResult.get(pos - 1)) != 0) {
                  addAll = false;
                  break;
                }
                mergedResult.add(pos, result);
                if (mergedResult.size() > totalResultElementCount) {
                  mergedResult.remove(totalResultElementCount);
                }
                pos += 1;
              }
              if (isTrimmed && addAll) {
                while (pos < mergedResult.size()) {
                  if (mergedResult.get(pos).compareBkeyTo(mergedResult.get(pos - 1)) == 0) {
                    pos += 1;
                  } else {
                    mergedResult.remove(pos);
                  }
                }
                mergedTrim.set(true);
              }
              if (mergedResult.size() >= totalResultElementCount) {
                mergedTrim.set(false);
              }
            }

            if (processedSMGetCount.get() == 0) {
              boolean isDuplicated = false;
              for (int i = 1; i < mergedResult.size(); i++) {
                if (mergedResult.get(i).compareBkeyTo(mergedResult.get(i - 1)) == 0) {
                  isDuplicated = true;
                  break;
                }
              }
              if (mergedTrim.get()) {
                if (isDuplicated)
                  resultOperationStatus.add(new OperationStatus(true, "DUPLICATED_TRIMMED"));
                else
                  resultOperationStatus.add(new OperationStatus(true, "TRIMMED"));
              } else {
                if (isDuplicated)
                  resultOperationStatus.add(new OperationStatus(true, "DUPLICATED"));
                else
                  resultOperationStatus.add(new OperationStatus(true, "END"));
              }
            }
          } finally {
            lock.unlock();
          }
        }

        @Override
        public void complete() {
          blatch.countDown();
        }

        @Override
        public void gotData(String key, Object subkey, int flags, byte[] data) {
          if (stopCollect.get())
            return;

          if (subkey instanceof Long) {
            eachResult.add(new SMGetElement<T>(key, (Long) subkey, tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          } else if (subkey instanceof byte[]) {
            eachResult.add(new SMGetElement<T>(key, (byte[]) subkey, tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          }
        }

        @Override
        public void gotMissedKey(byte[] data) {
          missedKeyList.add(new String(data));
          OperationStatus cause = new OperationStatus(false, "UNDEFINED");
          missedKeys.put(new String(data), new CollectionOperationStatus(cause));
        }
      });
      ops.add(op);
      addOp(smGet.getRepresentKey(), op);
    }

    return new SMGetFuture<List<SMGetElement<T>>>(ops, operationTimeout) {
      @Override
      public List<SMGetElement<T>> get(long duration, TimeUnit units)
              throws InterruptedException, TimeoutException,
              ExecutionException {
        if (!blatch.await(duration, units)) {
          for (Operation op : ops) {
            MemcachedConnection.opTimedOut(op);
          }
          throw new CheckedOperationTimeoutException(
                  "Timed out waiting for operation >" + duration + " " + units, ops);
        } else {
          // continuous timeout counter will be reset
          for (Operation op : ops) {
            MemcachedConnection.opSucceeded(op);
          }
        }

        for (Operation op : ops) {
          if (op != null && op.hasErrored()) {
            throw new ExecutionException(op.getException());
          }

          if (op.isCancelled()) {
            throw new ExecutionException(new RuntimeException(
                    op.getCancelCause()));
          }
        }

        if (smGetList.size() == 1)
          return mergedResult;

        return getSubList(mergedResult, offset, count);
      }

      @Override
      public List<String> getMissedKeyList() {
        return missedKeyList;
      }

      @Override
      public Map<String, CollectionOperationStatus> getMissedKeys() {
        return missedKeys;
      }

      @Override
      public List<SMGetTrimKey> getTrimmedKeys() {
        return mergedTrimmedKeys;
      }

      @Override
      public CollectionOperationStatus getOperationStatus() {
        if (failedOperationStatus.size() > 0) {
          return new CollectionOperationStatus(failedOperationStatus.get(0));
        }
        return new CollectionOperationStatus(resultOperationStatus.get(0));
      }
    };
  }

  private <T> SMGetFuture<List<SMGetElement<T>>> smget(
          final List<BTreeSMGet<T>> smGetList, final int count,
          final boolean reverse, final Transcoder<T> tc, final SMGetMode smgetMode) {

    final String END = "END";
    final String TRIMMED = "TRIMMED";
    final String DUPLICATED = "DUPLICATED";
    final String DUPLICATED_TRIMMED = "DUPLICATED_TRIMMED";

    final CountDownLatch blatch = new CountDownLatch(smGetList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();
    final List<String> missedKeyList =
            Collections.synchronizedList(new ArrayList<String>());
    final Map<String, CollectionOperationStatus> missedKeys =
            Collections.synchronizedMap(new HashMap<String, CollectionOperationStatus>());
    final int totalResultElementCount = count;

    final List<SMGetElement<T>> mergedResult = Collections.synchronizedList(new ArrayList<SMGetElement<T>>(totalResultElementCount));
    final List<SMGetTrimKey> mergedTrimmedKeys = Collections.synchronizedList(new ArrayList<SMGetTrimKey>());

    final ReentrantLock lock = new ReentrantLock();

    final List<OperationStatus> resultOperationStatus = Collections.synchronizedList(new ArrayList<OperationStatus>(1));

    final List<OperationStatus> failedOperationStatus = Collections.synchronizedList(new ArrayList<OperationStatus>(1));

    final AtomicBoolean stopCollect = new AtomicBoolean(false);
    // if processedSMGetCount is 0, then all smget is done.
    final AtomicInteger processedSMGetCount = new AtomicInteger(smGetList.size());

    for (BTreeSMGet<T> smGet : smGetList) {
      Operation op = opFact.bopsmget(smGet, new BTreeSortMergeGetOperation.Callback() {
        final List<SMGetElement<T>> eachResult = new ArrayList<SMGetElement<T>>();
        final List<SMGetTrimKey> eachTrimmedResult = new ArrayList<SMGetTrimKey>();

        @Override
        public void receivedStatus(OperationStatus status) {
          processedSMGetCount.decrementAndGet();

          if (!status.isSuccess()) {
            getLogger().warn("SMGetFailed. status=%s", status);
            if (!stopCollect.get()) {
              stopCollect.set(true);
              failedOperationStatus.add(status);
            }
            mergedResult.clear();
            mergedTrimmedKeys.clear();
            return;
          }

          lock.lock();
          try {
            if (mergedResult.size() == 0) {
              // merged result is empty, add all.
              mergedResult.addAll(eachResult);
            } else {
              // do sort merge
              boolean duplicated;
              int comp, pos = 0;
              for (SMGetElement<T> result : eachResult) {
                duplicated = false;
                for (; pos < mergedResult.size(); pos++) {
                  // compare b+tree key
                  comp = result.compareBkeyTo(mergedResult.get(pos));
                  if ((reverse) ? (0 < comp) : (0 > comp)) {
                    break;
                  }
                  if (comp == 0) { // compare key string
                    comp = result.compareKeyTo(mergedResult.get(pos));
                    if ((reverse) ? (0 < comp) : (0 > comp)) {
                      if (smgetMode == SMGetMode.UNIQUE)
                        mergedResult.remove(pos); // remove dup bkey
                      break;
                    } else {
                      if (smgetMode == SMGetMode.UNIQUE) {
                        duplicated = true;
                        break;
                      }
                    }
                  }
                }
                if (duplicated) { // UNIQUE
                  continue;
                }
                if (pos >= totalResultElementCount) {
                  // At this point, following conditions are met.
                  //   - mergedResult.size() == totalResultElementCount &&
                  //   - The current <bkey, key> of eachResult is
                  //     behind of the last <bkey, key> of mergedResult.
                  // Then, all the next <bkey, key> elements of eachResult are
                  // definitely behind of the last <bkey, bkey> of mergedResult.
                  // So, stop the current sort-merge.
                  break;
                }

                mergedResult.add(pos, result);
                if (mergedResult.size() > totalResultElementCount) {
                  mergedResult.remove(totalResultElementCount);
                }
                pos += 1;
              }
            }

            if (eachTrimmedResult.size() > 0) {
              if (mergedTrimmedKeys.size() == 0) {
                mergedTrimmedKeys.addAll(eachTrimmedResult);
              } else {
                // do sort merge trimmed list
                int pos = 0;
                for (SMGetTrimKey result : eachTrimmedResult) {
                  for (; pos < mergedTrimmedKeys.size(); pos++) {
                    if ((reverse) ? (0 < result.compareTo(mergedTrimmedKeys.get(pos)))
                            : (0 > result.compareTo(mergedTrimmedKeys.get(pos)))) {
                      break;
                    }
                  }
                  mergedTrimmedKeys.add(pos, result);
                  pos += 1;
                }
              }
            }

            if (processedSMGetCount.get() == 0) {
              if (mergedTrimmedKeys.size() > 0 && count <= mergedResult.size()) {
                // remove trimed keys whose bkeys are behind of the last element.
                SMGetElement<T> lastElement = mergedResult.get(mergedResult.size() - 1);
                SMGetTrimKey lastTrimKey = new SMGetTrimKey(lastElement.getKey(),
                        lastElement.getBkeyByObject());
                for (int i = mergedTrimmedKeys.size() - 1; i >= 0; i--) {
                  SMGetTrimKey me = mergedTrimmedKeys.get(i);
                  if ((reverse) ? (0 >= me.compareBkeyTo(lastTrimKey))
                          : (0 <= me.compareBkeyTo(lastTrimKey))) {
                    mergedTrimmedKeys.remove(i);
                  } else {
                    break;
                  }
                }
              }
              if (smgetMode == SMGetMode.UNIQUE) {
                resultOperationStatus.add(new OperationStatus(true, "END"));
              } else {
                boolean isDuplicated = false;
                for (int i = 1; i < mergedResult.size(); i++) {
                  if (mergedResult.get(i).compareBkeyTo(mergedResult.get(i - 1)) == 0) {
                    isDuplicated = true;
                    break;
                  }
                }
                if (isDuplicated)
                  resultOperationStatus.add(new OperationStatus(true, "DUPLICATED"));
                else
                  resultOperationStatus.add(new OperationStatus(true, "END"));
              }
            }
          } finally {
            lock.unlock();
          }
        }

        @Override
        public void complete() {
          blatch.countDown();
        }

        @Override
        public void gotData(String key, Object subkey, int flags, byte[] data) {
          if (stopCollect.get())
            return;

          if (subkey instanceof Long) {
            eachResult.add(new SMGetElement<T>(key, (Long) subkey, tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          } else if (subkey instanceof byte[]) {
            eachResult.add(new SMGetElement<T>(key, (byte[]) subkey, tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          }
        }

        @Override
        public void gotMissedKey(String key, OperationStatus cause) {
          missedKeyList.add(key);
          missedKeys.put(key, new CollectionOperationStatus(cause));
        }

        @Override
        public void gotTrimmedKey(String key, Object subkey) {
          if (stopCollect.get())
            return;

          if (subkey instanceof Long) {
            eachTrimmedResult.add(new SMGetTrimKey(key, (Long) subkey));
          } else if (subkey instanceof byte[]) {
            eachTrimmedResult.add(new SMGetTrimKey(key, (byte[]) subkey));
          }
        }
      });
      ops.add(op);
      addOp(smGet.getRepresentKey(), op);
    }

    return new SMGetFuture<List<SMGetElement<T>>>(ops, operationTimeout) {
      @Override
      public List<SMGetElement<T>> get(long duration, TimeUnit units)
              throws InterruptedException, TimeoutException,
              ExecutionException {
        if (!blatch.await(duration, units)) {
          for (Operation op : ops) {
            MemcachedConnection.opTimedOut(op);
          }
          throw new CheckedOperationTimeoutException(
                  "Timed out waiting for operation >" + duration + " " + units, ops);
        } else {
          // continuous timeout counter will be reset
          for (Operation op : ops) {
            MemcachedConnection.opSucceeded(op);
          }
        }

        for (Operation op : ops) {
          if (op != null && op.hasErrored()) {
            throw new ExecutionException(op.getException());
          }

          if (op.isCancelled()) {
            throw new ExecutionException(new RuntimeException(op.getCancelCause()));
          }
        }

        if (smGetList.size() == 1)
          return mergedResult;

        return getSubList(mergedResult, 0, count);
      }

      @Override
      public List<String> getMissedKeyList() {
        return missedKeyList;
      }

      @Override
      public Map<String, CollectionOperationStatus> getMissedKeys() {
        return missedKeys;
      }

      @Override
      public List<SMGetTrimKey> getTrimmedKeys() {
        return mergedTrimmedKeys;
      }

      @Override
      public CollectionOperationStatus getOperationStatus() {
        if (failedOperationStatus.size() > 0) {
          return new CollectionOperationStatus(failedOperationStatus.get(0));
        }
        return new CollectionOperationStatus(resultOperationStatus.get(0));
      }
    };
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpsert(java.lang.String, long, java.lang.Object, byte[], boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                  byte[] elementFlag, Object value, CollectionAttributes attributesForCreate) {

    BTreeUpsert<Object> bTreeStore = new BTreeUpsert<Object>(value,
            elementFlag, (attributesForCreate != null), null, attributesForCreate);

    return asyncCollectionUpsert(key, String.valueOf(bkey), bTreeStore,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpsert(java.lang.String, long, java.lang.Object, byte[], boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                      byte[] elementFlag, T value, CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {

    BTreeUpsert<T> bTreeStore = new BTreeUpsert<T>(value, elementFlag,
            (attributesForCreate != null), null, attributesForCreate);

    return asyncCollectionUpsert(key, String.valueOf(bkey), bTreeStore, tc);
  }

  private <T> CollectionFuture<Boolean> asyncCollectionUpsert(
          final String key, final String subkey,
          final CollectionStore<T> collectionStore, Transcoder<T> tc) {

    CachedData co = tc.encode(collectionStore.getValue());
    collectionStore.setFlags(co.getFlags());

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<Boolean>(
            latch, operationTimeout);
    Operation op = opFact.collectionUpsert(key, subkey, collectionStore,
            co.getData(), new OperationCallback() {
              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(cstatus.isSuccess(), cstatus);
                if (!cstatus.isSuccess()
                        && getLogger().isDebugEnabled()) {
                  getLogger().debug(
                          "Insertion to the collection failed : "
                                  + cstatus.getMessage()
                                  + " (type="
                                  + collectionStore.getClass()
                                  .getName() + ", key=" + key
                                  + ", subkey=" + subkey + ", value="
                                  + collectionStore.getValue() + ")");
                }
              }

              public void complete() {
                latch.countDown();
              }
            });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpdate(java.lang.String, long, java.lang.Object, net.spy.memcached.collection.ElementFlagUpdate)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                  ElementFlagUpdate eFlagUpdate, Object value) {
    BTreeUpdate<Object> collectionUpdate = new BTreeUpdate<Object>(
            value, eFlagUpdate, false);
    return asyncCollectionUpdate(key, String.valueOf(bkey),
            collectionUpdate, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpdate(java.lang.String, long, java.lang.Object, net.spy.memcached.collection.ElementFlagUpdate, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                      ElementFlagUpdate eFlagUpdate, T value, Transcoder<T> tc) {
    BTreeUpdate<T> collectionUpdate = new BTreeUpdate<T>(value,
            eFlagUpdate, false);
    return asyncCollectionUpdate(key, String.valueOf(bkey),
            collectionUpdate, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpdate(java.lang.String, byte[], net.spy.memcached.collection.ElementFlagUpdate, java.lang.Object)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopUpdate(String key,
                                                  byte[] bkey, ElementFlagUpdate eFlagUpdate, Object value) {
    BTreeUpdate<Object> collectionUpdate = new BTreeUpdate<Object>(
            value, eFlagUpdate, false);
    return asyncCollectionUpdate(key, BTreeUtil.toHex(bkey),
            collectionUpdate, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpdate(java.lang.String, byte[], net.spy.memcached.collection.ElementFlagUpdate, java.lang.Object, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key,
                                                      byte[] bkey, ElementFlagUpdate eFlagUpdate, T value,
                                                      Transcoder<T> tc) {
    BTreeUpdate<T> collectionUpdate = new BTreeUpdate<T>(value,
            eFlagUpdate, false);
    return asyncCollectionUpdate(key, BTreeUtil.toHex(bkey),
            collectionUpdate, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopUpdate(java.lang.String, java.lang.String, java.lang.Object)
   */
  @Override
  public CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                  Object value) {
    validateMKey(mkey);
    MapUpdate<Object> collectionUpdate = new MapUpdate<Object>(
            value, false);
    return asyncCollectionUpdate(key, String.valueOf(mkey),
            collectionUpdate, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopUpdate(java.lang.String, java.lang.String, java.lang.Objeat, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                      T value, Transcoder<T> tc) {
    validateMKey(mkey);
    MapUpdate<T> collectionUpdate = new MapUpdate<T>(value, false);
    return asyncCollectionUpdate(key, String.valueOf(mkey),
            collectionUpdate, tc);
  }

  /**
   * Generic update operation for collection items. Public methods for collection items call this method.
   *
   * @param key              collection item's key
   * @param subkey           element key (list index, b+tree bkey)
   * @param collectionUpdate operation parameters (element value and so on)
   * @param tc               transcoder to serialize and unserialize value
   * @return future holding the success/failure of the operation
   */
  private <T> CollectionFuture<Boolean> asyncCollectionUpdate(
          final String key, final String subkey,
          final CollectionUpdate<T> collectionUpdate, Transcoder<T> tc) {

    CachedData co = null;
    if (collectionUpdate.getNewValue() != null) {
      co = tc.encode(collectionUpdate.getNewValue());
      collectionUpdate.setFlags(co.getFlags());
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<Boolean>(
            latch, operationTimeout);

    Operation op = opFact.collectionUpdate(key, subkey, collectionUpdate,
            ((co == null) ? null : co.getData()), new OperationCallback() {
              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;

                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(cstatus.isSuccess(), cstatus);
                if (!cstatus.isSuccess()
                        && getLogger().isDebugEnabled()) {
                  getLogger().debug(
                          "Insertion to the collection failed : "
                                  + cstatus.getMessage()
                                  + " (type="
                                  + collectionUpdate.getClass()
                                  .getName() + ", key=" + key
                                  + ", subkey=" + subkey + ", value="
                                  + collectionUpdate.getNewValue()
                                  + ")");
                }
              }

              public void complete() {
                latch.countDown();
              }
            });
    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpdate(java.lang.String,
   * byte[], net.spy.memcached.collection.ElementFlagUpdate, java.lang.Object,
   * net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<Object>> elements) {
    return asyncBopPipedUpdateBulk(key, elements, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<T>> elements, Transcoder<T> tc) {

    if (elements.size() <= CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT) {
      CollectionPipedUpdate<T> collectionPipedUpdate = new BTreePipedUpdate<T>(
              key, elements, tc);
      return asyncCollectionPipedUpdate(key, collectionPipedUpdate);
    } else {
      PartitionedList<Element<T>> list = new PartitionedList<Element<T>>(
              elements, CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT);

      List<CollectionPipedUpdate<T>> collectionPipedUpdateList = new ArrayList<CollectionPipedUpdate<T>>(
              list.size());

      for (int i = 0; i < list.size(); i++) {
        collectionPipedUpdateList.add(new BTreePipedUpdate<T>(key, list
                .get(i), tc));
      }

      return asyncCollectionPipedUpdate(key, collectionPipedUpdateList);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see net.spy.memcached.ArcusClientIF#asyncMopUpdate(java.lang.String,
   * java.util.Map, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, Object> elements) {
    return asyncMopPipedUpdateBulk(key, elements, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, T> elements, Transcoder<T> tc) {

    for (Map.Entry<String, T> checkMKey
            : elements.entrySet()) {
      validateMKey(checkMKey.getKey());
    }
    if (elements.size() <= CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT) {
      CollectionPipedUpdate<T> collectionPipedUpdate = new MapPipedUpdate<T>(
              key, elements, tc);
      return asyncCollectionPipedUpdate(key, collectionPipedUpdate);
    } else {
      PartitionedMap<String, T> list = new PartitionedMap<String, T>(
              elements, CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT);

      List<CollectionPipedUpdate<T>> collectionPipedUpdateList = new ArrayList<CollectionPipedUpdate<T>>(
              list.size());

      for (int i = 0; i < list.size(); i++) {
        collectionPipedUpdateList.add(new MapPipedUpdate<T>(key, list
                .get(i), tc));
      }

      return asyncCollectionPipedUpdate(key, collectionPipedUpdateList);
    }
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopInsert(java.lang.String, byte[], java.lang.Object, byte[], boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopInsert(String key, byte[] bkey,
                                                  byte[] eFlag, Object value, CollectionAttributes attributesForCreate) {
    BTreeStore<Object> bTreeStore = new BTreeStore<Object>(value,
            eFlag, (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key,
            BTreeUtil.toHex(bkey), bTreeStore,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopInsert(java.lang.String, byte[], java.lang.Object, byte[], boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key,
                                                      byte[] bkey, byte[] eFlag, T value,
                                                      CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    BTreeStore<T> bTreeStore = new BTreeStore<T>(value, eFlag,
            (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionStore(key,
            BTreeUtil.toHex(bkey), bTreeStore, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, byte[], byte[], int, int, boolean, boolean, net.spy.memcached.collection.ElementFlagFilter)
   */
  @Override
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter,
          boolean withDelete, boolean dropIfEmpty) {
    BTreeGet get = new BTreeGet(bkey, bkey,
            0, 1, withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopExtendedGet(key, get, false, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, byte[], net.spy.memcached.collection.ElementFlagFilter, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter,
          boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    BTreeGet get = new BTreeGet(bkey, bkey, 0, 1,
            withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopExtendedGet(key, get, false, tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, byte[], byte[], int, int, boolean, boolean, net.spy.memcached.collection.ElementFlagFilter)
   */
  @Override
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(String key,
                                                                           byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset,
                                                                           int count, boolean withDelete, boolean dropIfEmpty) {
    BTreeGet get = new BTreeGet(from, to,
            offset, count, withDelete, dropIfEmpty, eFlagFilter);

    boolean reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;

    return asyncBopExtendedGet(key, get, reverse, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGet(java.lang.String, byte[], byte[], net.spy.memcached.collection.ElementFlagFilter, int, int, boolean, boolean, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset,
          int count, boolean withDelete, boolean dropIfEmpty,
          Transcoder<T> tc) {
    BTreeGet get = new BTreeGet(from, to, offset,
            count, withDelete, dropIfEmpty, eFlagFilter);
    boolean reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;
    return asyncBopExtendedGet(key, get, reverse, tc);
  }

  /**
   * Generic get operation for b+tree items using byte-array type bkeys. Public methods for b+tree items call this method.
   *
   * @param k             b+tree item's key
   * @param collectionGet operation parameters (element key and so on)
   * @param reverse       forward or backward
   * @param tc            transcoder to serialize and unserialize value
   * @return future holding the map of the fetched element and its byte-array bkey
   */
  private <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopExtendedGet(
          final String k, final CollectionGet collectionGet,
          final boolean reverse, final Transcoder<T> tc) {

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<ByteArrayBKey, Element<T>>> rv = new CollectionFuture<Map<ByteArrayBKey, Element<T>>>(
            latch, operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
            new CollectionGetOperation.Callback() {
              TreeMap<ByteArrayBKey, Element<T>> map = new ByteArrayTreeMap<ByteArrayBKey, Element<T>>(
                      (reverse) ? Collections.reverseOrder() : null);

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                if (cstatus.isSuccess()) {
                  rv.set(map, cstatus);
                  return;
                }
                switch (cstatus.getResponse()) {
                  case NOT_FOUND:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) not found : %s", k,
                              cstatus);
                    }
                    break;
                  case NOT_FOUND_ELEMENT:
                    rv.set(map, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Element(%s) not found : %s",
                              k, cstatus);
                    }
                    break;
                  case UNREADABLE:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Collection(%s) is not readable : %s",
                              k, cstatus);
                    }
                    break;
                  default:
                    rv.set(null, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Key(%s) Unknown response : %s",
                              k, cstatus);
                    }
                    break;
                }
              }

              public void complete() {
                latch.countDown();
              }

              public void gotData(String key, String subkey,
                                  int flags, byte[] data) {
                assert key.equals(k) : "Wrong key returned";
                byte[] bkey = BTreeUtil.hexStringToByteArrays(subkey);
                Element<T> element = new Element<T>(bkey, tc
                        .decode(new CachedData(flags, data, tc
                                .getMaxSize())), collectionGet.getElementFlag());
                map.put(new ByteArrayBKey(bkey), element);
              }
            });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int pos) {
    BTreeGetByPosition get = new BTreeGetByPosition(order, pos);
    boolean reverse = false;
    return asyncBopGetByPosition(key, get, reverse, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int pos, Transcoder<T> tc) {
    BTreeGetByPosition get = new BTreeGetByPosition(order, pos);
    boolean reverse = false;
    return asyncBopGetByPosition(key, get, reverse, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int from, int to) {
    BTreeGetByPosition get = new BTreeGetByPosition(order, from, to);
    boolean reverse = from > to;
    return asyncBopGetByPosition(key, get, reverse, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopGetByPosition(
          String key, BTreeOrder order, int from, int to, Transcoder<T> tc) {
    BTreeGetByPosition get = new BTreeGetByPosition(order, from, to);
    boolean reverse = from > to;
    return asyncBopGetByPosition(key, get, reverse, tc);
  }

  /**
   * Generic get operation for b+tree items using positions. Public methods for b+tree items call this method.
   *
   * @param k       b+tree item's key
   * @param get     operation parameters (element position and so on)
   * @param reverse forward or backward
   * @param tc      transcoder to serialize and unserialize value
   * @return future holding the map of the fetched element and its position
   */
  private <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopGetByPosition(
          final String k, final BTreeGetByPosition get,
          final boolean reverse, final Transcoder<T> tc) {
    // Check for invalid arguments (not to get CLIENT_ERROR)
    if (get.getOrder() == null) {
      throw new IllegalArgumentException("BTreeOrder must not be null.");
    }
    if (get.getPosFrom() < 0 || get.getPosTo() < 0) {
      throw new IllegalArgumentException("Position must be 0 or positive integer.");
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<Integer, Element<T>>> rv = new CollectionFuture<Map<Integer, Element<T>>>(
            latch, operationTimeout);

    Operation op = opFact.bopGetByPosition(k, get, new BTreeGetByPositionOperation.Callback() {

      TreeMap<Integer, Element<T>> map = new TreeMap<Integer, Element<T>>(
              (reverse) ? Collections.reverseOrder() : null);

      public void receivedStatus(OperationStatus status) {
        CollectionOperationStatus cstatus;
        if (status instanceof CollectionOperationStatus) {
          cstatus = (CollectionOperationStatus) status;
        } else {
          getLogger().warn("Unhandled state: " + status);
          cstatus = new CollectionOperationStatus(status);
        }
        if (cstatus.isSuccess()) {
          rv.set(map, cstatus);
          return;
        }
        switch (cstatus.getResponse()) {
          case NOT_FOUND:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Key(%s) not found : %s", k,
                      cstatus);
            }
            break;
          case NOT_FOUND_ELEMENT:
            rv.set(map, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Element(%s) not found : %s",
                      k, cstatus);
            }
            break;
          case UNREADABLE:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) is not readable : %s",
                      k, cstatus);
            }
            break;
          case TYPE_MISMATCH:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) is not a B+Tree : %s",
                      k, cstatus);
            }
            break;
          default:
            getLogger().warn("Unhandled state: " + status);
        }
      }

      public void complete() {
        latch.countDown();
      }

      public void gotData(String key, int flags, int pos, BKeyObject bkeyObject, byte[] eflag, byte[] data) {
        assert key.equals(k) : "Wrong key returned";
        Element<T> element = makeBTreeElement(key, flags, bkeyObject, eflag, data, tc);

        if (element != null) {
          map.put(pos, element);
        }
      }
    });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public CollectionFuture<Integer> asyncBopFindPosition(String key, long longBKey,
                                                        BTreeOrder order) {
    if (order == null) {
      throw new IllegalArgumentException("BTreeOrder must not be null.");
    }
    BTreeFindPosition get = new BTreeFindPosition(longBKey, order);
    return asyncBopFindPosition(key, get);
  }

  @Override
  public CollectionFuture<Integer> asyncBopFindPosition(String key, byte[] byteArrayBKey,
                                                        BTreeOrder order) {
    if (order == null) {
      throw new IllegalArgumentException("BTreeOrder must not be null.");
    }
    BTreeFindPosition get = new BTreeFindPosition(byteArrayBKey, order);
    return asyncBopFindPosition(key, get);
  }

  /**
   * Generic find-position operation for b+tree items. Public methods for b+tree items call this method.
   *
   * @param k   b+tree item's key
   * @param get operation parameters (element key and so on)
   * @return future holding the element's position
   */
  private CollectionFuture<Integer> asyncBopFindPosition(final String k, final BTreeFindPosition get) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Integer> rv = new CollectionFuture<Integer>(latch, operationTimeout);

    Operation op = opFact.bopFindPosition(k, get, new BTreeFindPositionOperation.Callback() {

      int position = 0;

      public void receivedStatus(OperationStatus status) {
        CollectionOperationStatus cstatus;
        if (status instanceof CollectionOperationStatus) {
          cstatus = (CollectionOperationStatus) status;
        } else {
          getLogger().warn("Unhandled state: " + status);
          cstatus = new CollectionOperationStatus(status);
        }
        if (cstatus.isSuccess()) {
          rv.set(position, cstatus);
          return;
        }
        switch (cstatus.getResponse()) {
          case NOT_FOUND:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Key(%s) not found : %s", k,
                      cstatus);
            }
            break;
          case NOT_FOUND_ELEMENT:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Element(%s) not found : %s",
                      k, cstatus);
            }
            break;
          case UNREADABLE:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) is not readable : %s",
                      k, cstatus);
            }
            break;
          case BKEY_MISMATCH:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) has wrong bkey : %s(%s)",
                      k, cstatus, get.getBkeyObject().getType());
            }
            break;
          case TYPE_MISMATCH:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) is not a B+Tree : %s",
                      k, cstatus);
            }
            break;
          default:
            getLogger().warn("Unhandled state: " + status);
        }
      }

      public void complete() {
        latch.countDown();
      }

      public void gotData(int position) {
        this.position = position;
      }
    });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopFindPositionWithGet(
          String key, long longBKey, BTreeOrder order, int count) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(longBKey, order, count);
    return asyncBopFindPositionWithGet(key, get, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, long longBKey, BTreeOrder order, int count, Transcoder<T> tc) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(longBKey, order, count);
    return asyncBopFindPositionWithGet(key, get, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopFindPositionWithGet(
          String key, byte[] byteArrayBKey, BTreeOrder order, int count) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(byteArrayBKey, order, count);
    return asyncBopFindPositionWithGet(key, get, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, byte[] byteArrayBKey, BTreeOrder order, int count, Transcoder<T> tc) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(byteArrayBKey, order, count);
    return asyncBopFindPositionWithGet(key, get, tc);
  }


  /**
   * Generic find position with get operation for b+tree items. Public methods for b+tree items call this method.
   *
   * @param k   b+tree item's key
   * @param get operation parameters (element key and so on)
   * @param tc  transcoder to serialize and unserialize value
   * @return future holding the element's position
   */
  private <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          final String k, final BTreeFindPositionWithGet get, final Transcoder<T> tc) {
    if (get.getOrder() == null) {
      throw new IllegalArgumentException("BTreeOrder must not be null.");
    }
    if (get.getCount() < 0 || get.getCount() > 100) {
      throw new IllegalArgumentException("Count must be a value between 0 and 100.");
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<Integer, Element<T>>> rv = new CollectionFuture<Map<Integer, Element<T>>>(
            latch, operationTimeout);

    Operation op = opFact.bopFindPositionWithGet(k, get, new BTreeFindPositionWithGetOperation.Callback() {

      TreeMap<Integer, Element<T>> map = new TreeMap<Integer, Element<T>>();

      public void receivedStatus(OperationStatus status) {
        CollectionOperationStatus cstatus;
        if (status instanceof CollectionOperationStatus) {
          cstatus = (CollectionOperationStatus) status;
        } else {
          getLogger().warn("Unhandled state: " + status);
          cstatus = new CollectionOperationStatus(status);
        }
        if (cstatus.isSuccess()) {
          rv.set(map, cstatus);
          return;
        }
        switch (cstatus.getResponse()) {
          case NOT_FOUND:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Key(%s) not found : %s", k, cstatus);
            }
            break;
          case NOT_FOUND_ELEMENT:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Element(%s) not found : %s", k, cstatus);
            }
            break;
          case UNREADABLE:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) is not readable : %s", k, cstatus);
            }
            break;
          case BKEY_MISMATCH:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) has wrong bkey : %s(%s)",
                      k, cstatus, get.getBkeyObject().getType());
            }
            break;
          case TYPE_MISMATCH:
            rv.set(null, cstatus);
            if (getLogger().isDebugEnabled()) {
              getLogger().debug("Collection(%s) is not a B+Tree : %s", k, cstatus);
            }
            break;
          default:
            getLogger().warn("Unhandled state: " + status);
        }
      }

      public void complete() {
        latch.countDown();
      }

      public void gotData(String key, int flags, int pos, BKeyObject bkeyObject, byte[] eflag, byte[] data) {
        assert key.equals(k) : "Wrong key returned";
        Element<T> element = makeBTreeElement(key, flags, bkeyObject, eflag, data, tc);
        if (element != null) {
          map.put(pos, element);
        }
      }
    });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeStoreAndGet<Object> get = new BTreeStoreAndGet<Object>(
            BTreeStoreAndGet.Command.INSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeStoreAndGet<E> get = new BTreeStoreAndGet<E>(
            BTreeStoreAndGet.Command.INSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeStoreAndGet<Object> get = new BTreeStoreAndGet<Object>(
            BTreeStoreAndGet.Command.INSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeStoreAndGet<E> get = new BTreeStoreAndGet<E>(
            BTreeStoreAndGet.Command.INSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeStoreAndGet<Object> get = new BTreeStoreAndGet<Object>(
            BTreeStoreAndGet.Command.UPSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeStoreAndGet<E> get = new BTreeStoreAndGet<E>(
            BTreeStoreAndGet.Command.UPSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeStoreAndGet<Object> get = new BTreeStoreAndGet<Object>(
            BTreeStoreAndGet.Command.UPSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeStoreAndGet<E> get = new BTreeStoreAndGet<E>(
            BTreeStoreAndGet.Command.UPSERT, bkey,
            eFlag, value, attributesForCreate);
    return asyncBTreeStoreAndGet(key, get, transcoder);
  }

  /**
   * Insert/upsert and get the trimmed element for b+tree items. Public methods call this method.
   *
   * @param k   b+tree item's key
   * @param get operation parameters (element key and so on)
   * @param tc  transcoder to serialize and unserialize value
   * @return future holding the success/failure of the operation and the trimmed element
   */
  private <E> BTreeStoreAndGetFuture<Boolean, E> asyncBTreeStoreAndGet(
          final String k, final BTreeStoreAndGet<E> get,
          final Transcoder<E> tc) {
    CachedData co = tc.encode(get.getValue());
    get.setFlags(co.getFlags());

    final CountDownLatch latch = new CountDownLatch(1);
    final BTreeStoreAndGetFuture<Boolean, E> rv = new BTreeStoreAndGetFuture<Boolean, E>(
            latch, operationTimeout);

    Operation op = opFact.bopStoreAndGet(k, get, co.getData(),
            new BTreeStoreAndGetOperation.Callback() {
              Element<E> element = null;

              public void receivedStatus(OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                if (cstatus.isSuccess()) {
                  rv.set(true, cstatus);
                  rv.setElement(element);
                  return;
                }
                switch (cstatus.getResponse()) {
                  case NOT_FOUND:
                  case ELEMENT_EXISTS:
                  case OVERFLOWED:
                  case OUT_OF_RANGE:
                  case TYPE_MISMATCH:
                  case BKEY_MISMATCH:
                    rv.set(false, cstatus);
                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug("Request for \"%s\" was not successful : %s",
                              k, cstatus);
                    }
                    break;
                  default:
                    getLogger().warn("Unhandled state: " + status);
                }
              }

              public void complete() {
                latch.countDown();
              }

              public void gotData(String key, int flags, BKeyObject bkeyObject,
                                  byte[] eflag, byte[] data) {
                assert key.equals(k) : "Wrong key returned";
                element = makeBTreeElement(key, flags, bkeyObject, eflag, data, tc);
              }
            });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /**
   * Utility method to create a b+tree element from individual parameters.
   *
   * @param key   b+tree item's key
   * @param flags item flags, used when creating the item (see createKeyIfNotExists)
   * @param bkey  element key
   * @param eflag element flags
   * @param value element value
   * @param tc    transcoder to serialize and unserialize value
   * @return element object containing all the parameters and transcoded value
   */
  private <T> Element<T> makeBTreeElement(String key, int flags,
                                          BKeyObject bkey, byte[] eflag, byte[] data, Transcoder<T> tc) {
    Element<T> element = null;
    T value = tc.decode(new CachedData(flags, data, tc.getMaxSize()));

    switch (bkey.getType()) {
      case LONG:
        element = new Element<T>(bkey.getLongBKey(), value, eflag);
        break;
      case BYTEARRAY:
        element = new Element<T>(bkey.getByteArrayBKeyRaw(), value, eflag);
        break;
      default:
        getLogger().error(
                "Unexpected bkey type : (key:" + key + ", bkey:"
                        + bkey.toString() + ")");
    }

    return element;
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopDelete(java.lang.String, byte[], byte[], net.spy.memcached.collection.ElementFlagFilter, int, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key, byte[] from,
                                                  byte[] to, ElementFlagFilter eFlagFilter, int count, boolean dropIfEmpty) {
    BTreeDelete delete = new BTreeDelete(from, to, count,
            false, dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopDelete(java.lang.String, byte[], net.spy.memcached.collection.ElementFlagFilter, boolean)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key,
                                                  byte[] bkey, ElementFlagFilter eFlagFilter, boolean dropIfEmpty) {
    BTreeDelete delete = new BTreeDelete(bkey, false,
            dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpsert(java.lang.String, byte[], byte[], java.lang.Object, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Boolean> asyncBopUpsert(String key,
                                                  byte[] bkey, byte[] elementFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    BTreeUpsert<Object> bTreeStore = new BTreeUpsert<Object>(value,
            elementFlag, (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionUpsert(key, BTreeUtil.toHex(bkey), bTreeStore,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopUpsert(java.lang.String, byte[], byte[], java.lang.Object, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key,
                                                      byte[] bkey, byte[] elementFlag, T value,
                                                      CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    BTreeUpsert<T> bTreeStore = new BTreeUpsert<T>(value, elementFlag,
            (attributesForCreate != null), null, attributesForCreate);
    return asyncCollectionUpsert(key, BTreeUtil.toHex(bkey), bTreeStore,
            tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGetItemCount(java.lang.String, byte[], byte[], net.spy.memcached.collection.ElementFlagFilter)
   */
  @Override
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        byte[] from, byte[] to, ElementFlagFilter eFlagFilter) {
    CollectionCount collectionCount = new BTreeCount(from, to, eFlagFilter);
    return asyncCollectionCount(key, collectionCount);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopPipedExistBulk(java.lang.String, java.util.List)
   */
  @Override
  public CollectionFuture<Map<Object, Boolean>> asyncSopPipedExistBulk(String key,
                                                                       List<Object> values) {
    SetPipedExist<Object> exist = new SetPipedExist<Object>(key, values,
            collectionTranscoder);
    return asyncSetPipedExist(key, exist);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopPipedExistBulk(java.lang.String, java.util.List, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<T, Boolean>> asyncSopPipedExistBulk(String key,
                                                                      List<T> values, Transcoder<T> tc) {
    SetPipedExist<T> exist = new SetPipedExist<T>(key, values, tc);
    return asyncSetPipedExist(key, exist);
  }

  /**
   * Generic pipelined existence operation for set items. Public methods call this method.
   *
   * @param key   collection item's key
   * @param exist operation parameters (element values)
   * @return future holding the map of elements and their existence results
   */
  <T> CollectionFuture<Map<T, Boolean>> asyncSetPipedExist(
          final String key, final SetPipedExist<T> exist) {

    if (exist.getItemCount() == 0) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }
    if (exist.getItemCount() > CollectionPipedStore.MAX_PIPED_ITEM_COUNT) {
      throw new IllegalArgumentException(
              "The number of piped operations must not exceed a maximum of "
                      + CollectionPipedStore.MAX_PIPED_ITEM_COUNT + ".");
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<T, Boolean>> rv = new CollectionFuture<Map<T, Boolean>>(
            latch, operationTimeout);

    Operation op = opFact.collectionPipedExist(key, exist,
            new CollectionPipedExistOperation.Callback() {

              Map<T, Boolean> result = new HashMap<T, Boolean>();

              boolean hasAnError = false;

              public void receivedStatus(OperationStatus status) {
                if (hasAnError)
                  return;

                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  getLogger().warn("Unhandled state: " + status);
                  cstatus = new CollectionOperationStatus(status);
                }
                rv.set(result, cstatus);
              }

              public void complete() {
                latch.countDown();
              }

              public void gotStatus(Integer index, OperationStatus status) {
                CollectionOperationStatus cstatus;
                if (status instanceof CollectionOperationStatus) {
                  cstatus = (CollectionOperationStatus) status;
                } else {
                  cstatus = new CollectionOperationStatus(status);
                }

                switch (cstatus.getResponse()) {
                  case EXIST:
                  case NOT_EXIST:
                    result.put(exist.getValues().get(index),
                            (CollectionResponse.EXIST.equals(cstatus
                                    .getResponse())));
                    break;
                  case UNREADABLE:
                  case TYPE_MISMATCH:
                  case NOT_FOUND:
                    hasAnError = true;
                    rv.set(new HashMap<T, Boolean>(0),
                            (CollectionOperationStatus) status);
                    break;
                  default:
                    getLogger().warn("Unhandled state: " + status);
                }
              }
            });

    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopPipedInsertBulk(java.lang.String, java.util.List, boolean, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<Object>> elements,
          CollectionAttributes attributesForCreate) {
    return asyncBopPipedInsertBulk(key, elements, attributesForCreate,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopPipedInsertBulk(java.lang.String, java.util.List, boolean, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<T>> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    if (elements.size() <= CollectionPipedStore.MAX_PIPED_ITEM_COUNT) {
      CollectionPipedStore<T> store = new ByteArraysBTreePipedStore<T>(
              key, elements, (attributesForCreate != null),
              attributesForCreate, tc);
      return asyncCollectionPipedStore(key, store);
    } else {
      PartitionedList<Element<T>> list = new PartitionedList<Element<T>>(
              elements, CollectionPipedStore.MAX_PIPED_ITEM_COUNT);

      List<CollectionPipedStore<T>> storeList = new ArrayList<CollectionPipedStore<T>>(
              list.size());

      for (int i = 0; i < list.size(); i++) {
        storeList.add(new ByteArraysBTreePipedStore<T>(key,
                list.get(i), (attributesForCreate != null),
                attributesForCreate, tc));
      }

      return asyncCollectionPipedStore(key, storeList);
    }
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopSortMergeGet(java.util.List, byte[], byte[], net.spy.memcached.collection.ElementFlagFilter, int, int)
   */
  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset, int count) {
    if (keyList == null || keyList.isEmpty()) {
      throw new IllegalArgumentException("Key list is empty.");
    }
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (offset + count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException(
              "The sum of offset and count must not exceed a maximum of " + MAX_SMGET_COUNT + ".");
    }

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<BTreeSMGet<Object>>(
            arrangedKey.size());
    for (List<String> v : arrangedKey.values()) {
      if (arrangedKey.size() > 1) {
        smGetList.add(new BTreeSMGetWithByteTypeBkeyOld<Object>(v, from, to, eFlagFilter, 0, offset + count));
      } else {
        smGetList.add(new BTreeSMGetWithByteTypeBkeyOld<Object>(v, from, to, eFlagFilter, offset, count));
      }
    }

    return smget(smGetList, offset, count, (BTreeUtil.compareByteArraysInLexOrder(from, to) > 0),
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopSortMergeGet(java.util.List, byte[], byte[], net.spy.memcached.collection.ElementFlagFilter, int, int, boolean)
   */
  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
          int count, SMGetMode smgetMode) {
    if (keyList == null || keyList.isEmpty()) {
      throw new IllegalArgumentException("Key list is empty.");
    }
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException("The count must not exceed a maximum of "
              + MAX_SMGET_COUNT + ".");
    }

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<BTreeSMGet<Object>>(
            arrangedKey.size());
    for (List<String> v : arrangedKey.values()) {
      smGetList.add(new BTreeSMGetWithByteTypeBkey<Object>(v, from, to, eFlagFilter, count, smgetMode));
    }

    return smget(smGetList, count, (BTreeUtil.compareByteArraysInLexOrder(from, to) > 0),
            collectionTranscoder, smgetMode);
  }

  /**
   * Generic pipelined store operation for collection items. Public methods for collection items call this method.
   *
   * @param key       collection item's key
   * @param storeList list of operation parameters (element values and so on)
   * @return future holding the map of element index and the result of its store operation
   */
  <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncCollectionPipedStore(
          final String key, final List<CollectionPipedStore<T>> storeList) {

    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();

    final CountDownLatch latch = new CountDownLatch(storeList.size());

    final List<OperationStatus> mergedOperationStatus = Collections
            .synchronizedList(new ArrayList<OperationStatus>(1));

    final Map<Integer, CollectionOperationStatus> mergedResult = new ConcurrentHashMap<Integer, CollectionOperationStatus>();

    for (int i = 0; i < storeList.size(); i++) {
      final CollectionPipedStore<T> store = storeList.get(i);
      final int idx = i;

      Operation op = opFact.collectionPipedStore(key, store,
              new CollectionPipedStoreOperation.Callback() {
                // each result status
                public void receivedStatus(OperationStatus status) {
                  CollectionOperationStatus cstatus;

                  if (status instanceof CollectionOperationStatus) {
                    cstatus = (CollectionOperationStatus) status;
                  } else {
                    getLogger().warn("Unhandled state: " + status);
                    cstatus = new CollectionOperationStatus(status);
                  }
                  mergedOperationStatus.add(cstatus);
                }

                // complete
                public void complete() {
                  latch.countDown();
                }

                // got status
                public void gotStatus(Integer index,
                                      OperationStatus status) {
                  if (status instanceof CollectionOperationStatus) {
                    mergedResult
                            .put(index
                                            + (idx * CollectionPipedStore.MAX_PIPED_ITEM_COUNT),
                                    (CollectionOperationStatus) status);
                  } else {
                    mergedResult
                            .put(index
                                            + (idx * CollectionPipedStore.MAX_PIPED_ITEM_COUNT),
                                    new CollectionOperationStatus(
                                            status));
                  }
                }
              });
      addOp(key, op);
      ops.add(op);
    }

    return new CollectionFuture<Map<Integer, CollectionOperationStatus>>(
            latch, operationTimeout) {

      @Override
      public boolean cancel(boolean ign) {
        boolean rv = false;
        for (Operation op : ops) {
          op.cancel("by application.");
          rv |= op.getState() == OperationState.WRITING;
        }
        return rv;
      }

      @Override
      public boolean isCancelled() {
        for (Operation op : ops) {
          if (op.isCancelled())
            return true;
        }
        return false;
      }

      @Override
      public Map<Integer, CollectionOperationStatus> get(long duration,
                                                         TimeUnit units) throws InterruptedException,
              TimeoutException, ExecutionException {

        if (!latch.await(duration, units)) {
          for (Operation op : ops) {
            MemcachedConnection.opTimedOut(op);
          }
          throw new CheckedOperationTimeoutException(
                  "Timed out waiting for operation >" + duration + " " + units, ops);
        } else {
          // continuous timeout counter will be reset
          for (Operation op : ops) {
            MemcachedConnection.opSucceeded(op);
          }
        }

        for (Operation op : ops) {
          if (op != null && op.hasErrored()) {
            throw new ExecutionException(op.getException());
          }

          if (op.isCancelled()) {
            throw new ExecutionException(new RuntimeException(op.getCancelCause()));
          }
        }

        return mergedResult;
      }

      @Override
      public CollectionOperationStatus getOperationStatus() {
        for (OperationStatus status : mergedOperationStatus) {
          if (!status.isSuccess()) {
            return new CollectionOperationStatus(status);
          }
        }
        return new CollectionOperationStatus(true, "END",
                CollectionResponse.END);
      }
    };
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopInsertBulk(java.util.List, long, byte[], java.lang.Object, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncBopInsertBulk(keyList, bkey, eFlag, value,
            attributesForCreate, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopInsertBulk(java.util.List, long, byte[], java.lang.Object, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);

    List<CollectionBulkStore<T>> storeList = new ArrayList<CollectionBulkStore<T>>(
            arrangedKey.size());

    for (List<String> eachKeyList : arrangedKey.values()) {
      storeList.add(new CollectionBulkStore.BTreeBulkStore<T>(
              eachKeyList, bkey, eFlag, value, attributesForCreate, tc));
    }

    return asyncCollectionInsertBulk2(storeList);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncBopInsertBulk(keyList, bkey, eFlag, value,
            attributesForCreate, collectionTranscoder);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, byte[] bkey, byte[] eFlag, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);
    List<CollectionBulkStore<T>> storeList = new ArrayList<CollectionBulkStore<T>>(
            arrangedKey.size());

    for (List<String> eachKeyList : arrangedKey.values()) {
      storeList.add(new CollectionBulkStore.BTreeBulkStore<T>(
              eachKeyList, bkey, eFlag, value, attributesForCreate, tc));
    }

    return asyncCollectionInsertBulk2(storeList);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopInsertBulk(java.util.List, java.lang.String, java.lang.Object, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncMopInsertBulk(keyList, mkey, value,
            attributesForCreate, collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncMopInsertBulk(java.util.List, java.lang.String, java.lang.Object, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    validateMKey(mkey);
    Map<String, List<String>> arrangedKey = groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);

    List<CollectionBulkStore<T>> storeList = new ArrayList<CollectionBulkStore<T>>(
            arrangedKey.size());

    for (List<String> eachKeyList : arrangedKey.values()) {
      storeList.add(new CollectionBulkStore.MapBulkStore<T>(
              eachKeyList, mkey, value, attributesForCreate, tc));
    }

    return asyncCollectionInsertBulk2(storeList);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopInsertBulk(java.util.List, java.lang.Object, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncSopInsertBulk(keyList, value, attributesForCreate,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncSopInsertBulk(java.util.List, java.lang.Object, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);
    List<CollectionBulkStore<T>> storeList = new ArrayList<CollectionBulkStore<T>>(
            arrangedKey.size());

    for (List<String> eachKeyList : arrangedKey.values()) {
      storeList.add(new CollectionBulkStore.SetBulkStore<T>(
              eachKeyList, value, attributesForCreate, tc));
    }

    return asyncCollectionInsertBulk2(storeList);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopInsertBulk(java.util.List, int, java.lang.Object, net.spy.memcached.collection.CollectionAttributes)
   */
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncLopInsertBulk(keyList, index, value, attributesForCreate,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncLopInsertBulk(java.util.List, int, java.lang.Object, net.spy.memcached.collection.CollectionAttributes, net.spy.memcached.transcoders.Transcoder)
   */
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    Map<String, List<String>> arrangedKey = groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);
    List<CollectionBulkStore<T>> storeList = new ArrayList<CollectionBulkStore<T>>(
            arrangedKey.size());

    for (List<String> eachKeyList : arrangedKey.values()) {
      storeList.add(new CollectionBulkStore.ListBulkStore<T>(
              eachKeyList, index, value, attributesForCreate, tc));
    }

    return asyncCollectionInsertBulk2(storeList);
  }

  /**
   * Generic bulk store operation for collection items. Public methods for collection items call this method.
   *
   * @param storeList list of operation parameters (item keys, element values, and so on)
   * @return future holding the map of item key and the result of the store operation on that key
   */
  private <T> Future<Map<String, CollectionOperationStatus>> asyncCollectionInsertBulk2(
          List<CollectionBulkStore<T>> storeList) {

    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();

    final Map<String, CollectionOperationStatus> failedResult = new ConcurrentHashMap<String, CollectionOperationStatus>();

    final CountDownLatch latch = new CountDownLatch(storeList.size());

    for (final CollectionBulkStore<T> store : storeList) {
      Operation op = opFact.collectionBulkStore(store.getKeyList(),
              store, new CollectionBulkStoreOperation.Callback() {
                public void receivedStatus(OperationStatus status) {

                }

                public void complete() {
                  latch.countDown();
                }

                public void gotStatus(Integer index,
                                      OperationStatus status) {
                  if (!status.isSuccess()) {
                    if (status instanceof CollectionOperationStatus) {
                      failedResult.put(
                              store.getKeyList().get(index),
                              (CollectionOperationStatus) status);
                    } else {
                      failedResult.put(
                              store.getKeyList().get(index),
                              new CollectionOperationStatus(
                                      status));
                    }
                  }
                }
              });
      ops.add(op);
      addOp(store.getKeyList().get(0), op);
    }

    // return future
    return new CollectionFuture<Map<String, CollectionOperationStatus>>(
            latch, operationTimeout) {

      @Override
      public boolean cancel(boolean ign) {
        boolean rv = false;
        for (Operation op : ops) {
          op.cancel("by application.");
          rv |= op.getState() == OperationState.WRITING;
        }
        return rv;
      }

      @Override
      public boolean isCancelled() {
        for (Operation op : ops) {
          if (op.isCancelled())
            return true;
        }
        return false;
      }

      @Override
      public Map<String, CollectionOperationStatus> get(long duration,
                                                        TimeUnit units) throws InterruptedException,
              TimeoutException, ExecutionException {
        if (!latch.await(duration, units)) {
          for (Operation op : ops) {
            MemcachedConnection.opTimedOut(op);
          }
          throw new CheckedOperationTimeoutException(
                  "Timed out waiting for bulk operation >" + duration + " " + units, ops);
        } else {
          // continuous timeout counter will be reset
          for (Operation op : ops) {
            MemcachedConnection.opSucceeded(op);
          }
        }

        for (Operation op : ops) {
          if (op != null && op.hasErrored()) {
            throw new ExecutionException(op.getException());
          }

          if (op.isCancelled()) {
            throw new ExecutionException(new RuntimeException(op.getCancelCause()));
          }
        }

        return failedResult;
      }

      @Override
      public CollectionOperationStatus getOperationStatus() {
        return null;
      }
    };
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGetBulk(java.util.List, long, long, net.spy.memcached.collection.ElementFlagFilter, int, int)
   */
  public CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    return asyncBopGetBulk(keyList, from, to, eFlagFilter, offset, count,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGetBulk(java.util.List, long, long, net.spy.memcached.collection.ElementFlagFilter, int, int, net.spy.memcached.transcoders.Transcoder)
   */
  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, T>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count,
          Transcoder<T> tc) {
    if (keyList == null) {
      throw new IllegalArgumentException("Key list is null.");
    }
    if (keyList.size() > MAX_GETBULK_KEY_COUNT) {
      throw new IllegalArgumentException("Key count must not exceed a maximum of " + MAX_GETBULK_KEY_COUNT + ".");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Offset must be 0 or positive integet.");
    }
    if (count > MAX_GETBULK_ELEMENT_COUNT) {
      throw new IllegalArgumentException("Count must not exceed a maximum of " + MAX_GETBULK_ELEMENT_COUNT + ".");
    }

    Map<String, List<String>> rearrangedKeys = groupingKeys(keyList, BOPGET_BULK_CHUNK_SIZE);

    List<BTreeGetBulk<T>> getBulkList = new ArrayList<BTreeGetBulk<T>>(
            rearrangedKeys.size());

    for (Entry<String, List<String>> entry : rearrangedKeys.entrySet()) {
      getBulkList.add(new BTreeGetBulkWithLongTypeBkey<T>(entry
              .getValue(), from, to, eFlagFilter, offset, count));
    }

    return btreeGetBulk(getBulkList, offset, count, (from > to), tc);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGetBulk(java.util.List, byte[], byte[], net.spy.memcached.collection.ElementFlagFilter, int, int)
   */
  public CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>> asyncBopGetBulk(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    return asyncBopGetBulk(keyList, from, to, eFlagFilter, offset, count,
            collectionTranscoder);
  }

  /*
   * (non-Javadoc)
   * @see net.spy.memcached.ArcusClientIF#asyncBopGetBulk(java.util.List, byte[], byte[], net.spy.memcached.collection.ElementFlagFilter, int, int, net.spy.memcached.transcoders.Transcoder)
   */
  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, T>>> asyncBopGetBulk(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count,
          Transcoder<T> tc) {
    if (keyList == null) {
      throw new IllegalArgumentException("Key list is null.");
    }
    if (keyList.size() > MAX_GETBULK_KEY_COUNT) {
      throw new IllegalArgumentException("Key count must not exceed a maximum of " + MAX_GETBULK_KEY_COUNT + ".");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Offset must be 0 or positive integet.");
    }
    if (count > MAX_GETBULK_ELEMENT_COUNT) {
      throw new IllegalArgumentException("Count must not exceed a maximum of " + MAX_GETBULK_ELEMENT_COUNT + ".");
    }

    Map<String, List<String>> rearrangedKeys = groupingKeys(keyList, BOPGET_BULK_CHUNK_SIZE);

    List<BTreeGetBulk<T>> getBulkList = new ArrayList<BTreeGetBulk<T>>(
            rearrangedKeys.size());

    for (Entry<String, List<String>> entry : rearrangedKeys.entrySet()) {
      getBulkList.add(new BTreeGetBulkWithByteTypeBkey<T>(entry
              .getValue(), from, to, eFlagFilter, offset, count));
    }

    boolean reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;

    return btreeGetBulkByteArrayBKey(getBulkList, offset, count, reverse, tc);
  }

  /**
   * Generic bulk get operation for b+tree items. Public methods call this method.
   *
   * @param getBulkList list of operation parameters (item keys, element key range, and so on)
   * @param offset      start index of the elements
   * @param count       number of elements to fetch
   * @param reverse     forward or backward
   * @param tc          transcoder to serialize and unserialize value
   * @return future holding the map of item key and the fetched elements from that key
   */
  private <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, T>>> btreeGetBulk(
          final List<BTreeGetBulk<T>> getBulkList, final int offset,
          final int count, final boolean reverse, final Transcoder<T> tc) {

    final CountDownLatch latch = new CountDownLatch(getBulkList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();
    final Map<String, BTreeGetResult<Long, T>> result = new ConcurrentHashMap<String, BTreeGetResult<Long, T>>();

    for (BTreeGetBulk<T> getBulk : getBulkList) {
      Operation op = opFact.bopGetBulk(getBulk, new BTreeGetBulkOperation.Callback<T>() {
        @Override
        public void receivedStatus(OperationStatus status) {
        }

        @Override
        public void complete() {
          latch.countDown();
        }

        @Override
        public void gotKey(String key, int elementCount, OperationStatus status) {
          TreeMap<Long, BTreeElement<Long, T>> tree = null;
          if (elementCount > 0) {
            tree = new TreeMap<Long, BTreeElement<Long, T>>(
                    (reverse) ? Collections.reverseOrder() : null);
          }
          result.put(key, new BTreeGetResult<Long, T>(tree, new CollectionOperationStatus(status)));
        }

        @Override
        public void gotElement(String key, Object subkey, int flags, byte[] eflag, byte[] data) {
          result.get(key).addElement(
                  new BTreeElement<Long, T>((Long) subkey, eflag,
                          tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
        }
      });
      ops.add(op);
      addOp(getBulk.getRepresentKey(), op);
    }

    return new CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, T>>>(latch, ops, result, operationTimeout);
  }

  /**
   * Generic bulk get operation for b+tree items using byte-array type bkeys. Public methods call this method.
   *
   * @param getBulkList list of operation parameters (item keys, element key range, and so on)
   * @param offset      start index of the elements
   * @param count       number of elements to fetch
   * @param reverse     forward or backward
   * @param tc          transcoder to serialize and unserialize value
   * @return future holding the map of item key and the fetched elements from that key
   */
  private <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, T>>> btreeGetBulkByteArrayBKey(
          final List<BTreeGetBulk<T>> getBulkList, final int offset,
          final int count, final boolean reverse, final Transcoder<T> tc) {

    final CountDownLatch latch = new CountDownLatch(getBulkList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<Operation>();
    final Map<String, BTreeGetResult<ByteArrayBKey, T>> result = new ConcurrentHashMap<String, BTreeGetResult<ByteArrayBKey, T>>();

    for (BTreeGetBulk<T> getBulk : getBulkList) {
      Operation op = opFact.bopGetBulk(getBulk, new BTreeGetBulkOperation.Callback<T>() {
        @Override
        public void receivedStatus(OperationStatus status) {
        }

        @Override
        public void complete() {
          latch.countDown();
        }

        @Override
        public void gotKey(String key, int elementCount, OperationStatus status) {
          TreeMap<ByteArrayBKey, BTreeElement<ByteArrayBKey, T>> tree = null;
          if (elementCount > 0) {
            tree = new ByteArrayTreeMap<ByteArrayBKey, BTreeElement<ByteArrayBKey, T>>(
                    (reverse) ? Collections.reverseOrder() : null);
          }
          result.put(key, new BTreeGetResult<ByteArrayBKey, T>(tree, new CollectionOperationStatus(status)));
        }

        @Override
        public void gotElement(String key, Object subkey, int flags, byte[] eflag, byte[] data) {
          result.get(key).addElement(
                  new BTreeElement<ByteArrayBKey, T>(
                          new ByteArrayBKey((byte[]) subkey),
                          eflag, tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
        }
      });
      ops.add(op);
      addOp(getBulk.getRepresentKey(), op);
    }

    return new CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, T>>>(latch, ops, result, operationTimeout);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, long subkey,
                                             int by) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by);
    return asyncCollectionMutate(key, String.valueOf(subkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] subkey,
                                             int by) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by);
    return asyncCollectionMutate(key, BTreeUtil.toHex(subkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, long subkey,
                                             int by, long initial, byte[] eFlag) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by, initial, eFlag);
    return asyncCollectionMutate(key, String.valueOf(subkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] subkey,
                                             int by, long initial, byte[] eFlag) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by, initial, eFlag);
    return asyncCollectionMutate(key, BTreeUtil.toHex(subkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, long subkey,
                                             int by) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by);
    return asyncCollectionMutate(key, String.valueOf(subkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] subkey,
                                             int by) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by);
    return asyncCollectionMutate(key, BTreeUtil.toHex(subkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, long subkey,
                                             int by, long initial, byte[] eFlag) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by, initial, eFlag);
    return asyncCollectionMutate(key, String.valueOf(subkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] subkey,
                                             int by, long initial, byte[] eFlag) {
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by, initial, eFlag);
    return asyncCollectionMutate(key, BTreeUtil.toHex(subkey), collectionMutate);
  }

  /**
   * Generic increment/decrement operation for b+tree items. Public methods call this method.
   *
   * @param k                b+tree item's key
   * @param subkey           element key
   * @param collectionMutate operation parameters (increment amount and so on)
   * @return future holding the incremented or decremented value
   */
  private CollectionFuture<Long> asyncCollectionMutate(final String k, final String subkey,
                                                       final CollectionMutate collectionMutate) {

    final CountDownLatch latch = new CountDownLatch(1);

    final CollectionFuture<Long> rv = new CollectionFuture<Long>(latch,
            operationTimeout);

    Operation op = opFact.collectionMutate(k, subkey, collectionMutate,
            new OperationCallback() {

              @Override
              public void receivedStatus(OperationStatus status) {
                if (status.isSuccess()) {
                  try {
                    rv.set(new Long(status.getMessage()),
                            new CollectionOperationStatus(
                                    new OperationStatus(true, "END")));
                  } catch (NumberFormatException e) {
                    rv.set(null, new CollectionOperationStatus(
                            new OperationStatus(false,
                                    status.getMessage())));

                    if (getLogger().isDebugEnabled()) {
                      getLogger().debug(
                              "Key(%s), Bkey(%s) Unknown response : %s",
                              k, subkey, status);
                    }
                  }
                  return;
                }

                rv.set(null, new CollectionOperationStatus(status));

                if (getLogger().isDebugEnabled()) {
                  getLogger().debug(
                          "Key(%s), Bkey(%s) Unknown response : %s",
                          k, subkey, status);
                }
              }

              @Override
              public void complete() {
                latch.countDown();
              }
            });

    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /**
   * Get the client version.
   *
   * @return version string
   */
  private static String getVersion() {
    Enumeration<URL> resEnum;
    try {
      resEnum = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
      while (resEnum.hasMoreElements()) {
        try {
          URL url = resEnum.nextElement();
          InputStream is = url.openStream();
          if (is != null) {
            Manifest manifest = new Manifest(is);
            java.util.jar.Attributes mainAttribs = manifest.getMainAttributes();
            String version = mainAttribs.getValue("Arcusclient-Version");
            if (version != null) {
              return version;
            }
          }
        } catch (Exception e) {

        }
      }
    } catch (IOException e1) {
      return "NONE";
    }
    return "NONE";
  }
}
