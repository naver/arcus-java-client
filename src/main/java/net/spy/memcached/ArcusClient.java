/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
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
import java.security.Security;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import net.spy.memcached.collection.BTreeInsert;
import net.spy.memcached.collection.BTreeInsertAndGet;
import net.spy.memcached.collection.BTreeMutate;
import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkeyOld;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkeyOld;
import net.spy.memcached.collection.BTreeUpdate;
import net.spy.memcached.collection.BTreeUpsert;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionBulkInsert;
import net.spy.memcached.collection.CollectionCount;
import net.spy.memcached.collection.CollectionCreate;
import net.spy.memcached.collection.CollectionDelete;
import net.spy.memcached.collection.CollectionExist;
import net.spy.memcached.collection.CollectionGet;
import net.spy.memcached.collection.CollectionInsert;
import net.spy.memcached.collection.CollectionMutate;
import net.spy.memcached.collection.CollectionPipedInsert;
import net.spy.memcached.collection.CollectionPipedInsert.BTreePipedInsert;
import net.spy.memcached.collection.CollectionPipedInsert.ByteArraysBTreePipedInsert;
import net.spy.memcached.collection.CollectionPipedInsert.ListPipedInsert;
import net.spy.memcached.collection.CollectionPipedInsert.MapPipedInsert;
import net.spy.memcached.collection.CollectionPipedInsert.SetPipedInsert;
import net.spy.memcached.collection.CollectionPipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.BTreePipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.MapPipedUpdate;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.CollectionUpdate;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementFlagUpdate;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.ListCreate;
import net.spy.memcached.collection.ListDelete;
import net.spy.memcached.collection.ListGet;
import net.spy.memcached.collection.ListInsert;
import net.spy.memcached.collection.MapCreate;
import net.spy.memcached.collection.MapDelete;
import net.spy.memcached.collection.MapGet;
import net.spy.memcached.collection.MapInsert;
import net.spy.memcached.collection.MapUpdate;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.collection.SetCreate;
import net.spy.memcached.collection.SetDelete;
import net.spy.memcached.collection.SetExist;
import net.spy.memcached.collection.SetGet;
import net.spy.memcached.collection.SetInsert;
import net.spy.memcached.collection.SetPipedExist;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;
import net.spy.memcached.internal.BTreeStoreAndGetFuture;
import net.spy.memcached.internal.BroadcastFuture;
import net.spy.memcached.internal.BulkOperationFuture;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.CollectionGetBulkFuture;
import net.spy.memcached.internal.CollectionGetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.PipedCollectionFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.internal.result.BopGetBulkResultImpl;
import net.spy.memcached.internal.result.BopGetByPositionResultImpl;
import net.spy.memcached.internal.result.BopGetResultImpl;
import net.spy.memcached.internal.result.BopStoreAndGetResultImpl;
import net.spy.memcached.internal.result.GetResult;
import net.spy.memcached.internal.result.LopGetResultImpl;
import net.spy.memcached.internal.result.MopGetResultImpl;
import net.spy.memcached.internal.result.SMGetResultImpl;
import net.spy.memcached.internal.result.SMGetResultOldImpl;
import net.spy.memcached.internal.result.SopGetResultImpl;
import net.spy.memcached.ops.BTreeFindPositionOperation;
import net.spy.memcached.ops.BTreeFindPositionWithGetOperation;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.BTreeGetByPositionOperation;
import net.spy.memcached.ops.BTreeInsertAndGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperationOld;
import net.spy.memcached.ops.CollectionBulkInsertOperation;
import net.spy.memcached.ops.CollectionGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.CollectionPipedExistOperation;
import net.spy.memcached.ops.CollectionPipedInsertOperation;
import net.spy.memcached.ops.CollectionPipedUpdateOperation;
import net.spy.memcached.ops.GetAttrOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
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
 * <pre>{@code
 * final static String arcusAdminAddrs = "127.0.0.1:2181";
 * final static String serviceCode = "cafe";
 *
 * ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
 *
 * ArcusClient c = ArcusClient.createArcusClient(arcusAdminAddrs, serviceCode, cfb);
 *
 * // Store a value (async) for one hour
 * c.set("someKey", 3600, someObject);
 * // Retrieve a value.
 * Future<Object> myFuture = c.asyncGet("someKey");
 *
 * If pool style is needed, it will be used as follows
 *
 * int poolSize = 4;
 * ArcusClientPool pool =
 *     ArcusClient.createArcusClientPool(arcusAdminAddrs, serviceCode, cfb, poolSize);
 *
 * // Store a value
 * pool.set("someKey", 3600, someObject);
 * // Retrieve a value
 * Future<Object> myFuture = pool.asyncGet("someKey");
 *
 * }</pre>
 */
public class ArcusClient extends FrontCacheMemcachedClient implements ArcusClientIF {
  private static String VERSION = null;
  private static final Object VERSION_LOCK = new Object();
  private static final Logger arcusLogger = LoggerFactory.getLogger(ArcusClient.class);
  private static final String ARCUS_ADMIN_ADDR = "127.0.0.1:2181";
  private static final String DEFAULT_ARCUS_CLIENT_NAME = "ArcusClient";

  private final Transcoder<Object> collectionTranscoder;

  private final int smgetKeyChunkSize;

  private static final int BOPGET_BULK_CHUNK_SIZE = 200;
  private static final int NON_PIPED_BULK_INSERT_CHUNK_SIZE = 500;

  private static final int MAX_GETBULK_ELEMENT_COUNT = 50;
  private static final int MAX_SMGET_COUNT = 1000; // server configuration is 2000.
  private static final int MAX_MKEY_LENGTH = 250;

  private static final int SHUTDOWN_TIMEOUT_MILLISECONDS = 2000;
  private static final AtomicInteger CLIENT_ID = new AtomicInteger(1);

  private static final int MAX_DNS_CACHE_TTL = 300;

  private CacheManager cacheManager;

  public void setCacheManager(CacheManager cacheManager) {
    this.cacheManager = cacheManager;
  }

  /**
   * @param hostPorts   arcus admin addresses
   * @param serviceCode service code
   * @return a single ArcusClient
   */
  public static ArcusClient createArcusClient(String hostPorts, String serviceCode) {

    return ArcusClient.createArcusClient(hostPorts, serviceCode, new ConnectionFactoryBuilder(), 1, 10000).getClient();

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
   * @deprecated because service code must be along with admin addresses.
   * Use {@link #createArcusClient(String, String, ConnectionFactoryBuilder)} instead.
   */
  @Deprecated
  public static ArcusClient createArcusClient(String serviceCode,
                                              ConnectionFactoryBuilder cfb) {

    return ArcusClient.createArcusClient(ARCUS_ADMIN_ADDR, serviceCode, cfb, 1, 10000).getClient();

  }

  /**
   * @param hostPorts   arcus admin addresses
   * @param serviceCode service code
   * @param poolSize    Arcus client pool size
   * @return multiple ArcusClient
   */
  public static ArcusClientPool createArcusClientPool(String hostPorts, String serviceCode, int poolSize) {

    return ArcusClient.createArcusClient(hostPorts, serviceCode, new ConnectionFactoryBuilder(), poolSize, 0);

  }

  /**
   * @param hostPorts   arcus admin addresses
   * @param serviceCode service code
   * @param poolSize    Arcus client pool size
   * @param cfb         ConnectionFactoryBuilder
   * @return multiple ArcusClient
   */
  public static ArcusClientPool createArcusClientPool(String hostPorts, String serviceCode,
                                                      ConnectionFactoryBuilder cfb, int poolSize) {

    return ArcusClient.createArcusClient(hostPorts, serviceCode, cfb, poolSize, 0);

  }

  /**
   * @param serviceCode service code
   * @param poolSize    Arcus client pool size
   * @param cfb         ConnectionFactoryBuilder
   * @return multiple ArcusClient
   * @deprecated because service code must be along with admin addresses.
   * Use {@link #createArcusClientPool(String, String, ConnectionFactoryBuilder, int)} instead.
   */
  @Deprecated
  public static ArcusClientPool createArcusClientPool(String serviceCode,
                                                      ConnectionFactoryBuilder cfb, int poolSize) {

    return ArcusClient.createArcusClient(ARCUS_ADMIN_ADDR, serviceCode, cfb, poolSize, 0);

  }

  /**
   * @param hostPorts          arcus admin addresses
   * @param serviceCode        service code
   * @param cfb                ConnectionFactoryBuilder
   * @param poolSize           Arcus client pool size
   * @param waitTimeForConnect Connect waiting time for connection establishment(milliseconds)
   * @return multiple ArcusClient
   */
  private static ArcusClientPool createArcusClient(String hostPorts, String serviceCode,
                                                   ConnectionFactoryBuilder cfb, int poolSize,
                                                   int waitTimeForConnect) {

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

    CacheManager exe = new CacheManager(hostPorts, serviceCode, cfb, poolSize, waitTimeForConnect);
    return new ArcusClientPool(poolSize, exe.getAC());
  }

  /**
   * Create an Arcus client for the given memcached server addresses.
   * Only invoked by initArcusClient in CacheManger.
   *
   * @param cf    connection factory to configure connections for this client
   * @param name  client name
   * @param addrs socket addresses for the memcached servers
   * @return Arcus client
   */
  protected static ArcusClient getInstance(ConnectionFactory cf,
                                           String name,
                                           List<InetSocketAddress> addrs) throws IOException {
    return new ArcusClient(cf, name, addrs);
  }

  /**
   * Create an Arcus client for the given memcached server addresses.
   *
   * @param cf    connection factory to configure connections for this client
   * @param name  client name
   * @param addrs socket addresses for the memcached servers
   * @throws IOException if connections cannot be established
   */
  public ArcusClient(ConnectionFactory cf, String name, List<InetSocketAddress> addrs)
          throws IOException {
    super(cf, name, addrs);

    if (cf.getDnsCacheTtlCheck() && !validateDnsCacheTtl()) {
      getLogger().warn("DNS cache TTL must be between 0 and " + MAX_DNS_CACHE_TTL +
              ". Invoke ConnectionFactoryBuilder.setDnsCacheTtlCheck(false) to avoid this constraint.");
      throw new IllegalStateException("DNS cache TTL is out of range from 0 to " + MAX_DNS_CACHE_TTL);
    }
    collectionTranscoder = new CollectionTranscoder();
    smgetKeyChunkSize = cf.getDefaultMaxSMGetKeyChunkSize();
    registerMbean(name);
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
    this(cf, DEFAULT_ARCUS_CLIENT_NAME + "-" + CLIENT_ID.getAndIncrement(), addrs);
  }

  @SuppressWarnings("removal")
  private static boolean validateDnsCacheTtl() {
    String securityDnsTtl = Security.getProperty("networkaddress.cache.ttl");
    try {
      if (securityDnsTtl != null) {
        return isValidTtl(Integer.valueOf(securityDnsTtl));
      }
    } catch (NumberFormatException e) {
      arcusLogger.warn("Invalid numeric type was set in property.");
    }

    String systemDnsTtl = System.getProperty("sun.net.inetaddr.ttl");
    try {
      if (systemDnsTtl != null) {
        return isValidTtl(Integer.decode(systemDnsTtl));
      }
    } catch (NumberFormatException e) {
      arcusLogger.warn("Invalid numeric type was set in property.");
    }

    /**
     * If SecurityManager was null, dns cache ttl was set positive value.
     * If is not null, dns cache ttl may be determined by <pre>java.policy</pre> file.
     * See also {@link InetAddressCachePolicy}.
     */
    return System.getSecurityManager() == null;
  }

  private static boolean isValidTtl(int ttl) {
    return ttl >= 0 && ttl <= MAX_DNS_CACHE_TTL;
  }

  /**
   * Register mbean for Arcus client statistics.
   */
  private void registerMbean(String name) {
    if ("false".equals(System.getProperty("arcus.mbean", "false").toLowerCase())) {
      getLogger().info("Arcus client statistics MBean is NOT registered.");
      return;
    }

    try {
      StatisticsHandler mbean = new StatisticsHandler(this);
      ArcusMBeanServer.getInstance().registMBean(
              mbean,
              mbean.getClass().getPackage().getName()
                      + ":type=" + mbean.getClass().getSimpleName()
                      + ",name=" + name);

      getLogger().info("Arcus client statistics MBean is registered.");
    } catch (Exception e) {
      getLogger().warn("Failed to initialize statistics mbean.", e);
    }
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit unit) {
    final boolean result = super.shutdown(timeout, unit);
    // Connect to Arcus server directly, cache manager may be null.
    if (cacheManager != null) {
      cacheManager.shutdown();
    }
    return result;
  }

  @Override
  public void shutdown() {
    this.shutdown(SHUTDOWN_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
  }

  private void validateMKey(String mkey) {
    byte[] keyBytes = KeyUtil.getKeyBytes(mkey);
    if (keyBytes.length > MAX_MKEY_LENGTH) {
      throw new IllegalArgumentException("MKey is too long (maxlen = "
              + MAX_MKEY_LENGTH + ")");
    }
    if (keyBytes.length == 0) {
      throw new IllegalArgumentException("MKey must contain at least one character.");
    }
    // Validate the mkey
    for (byte b : keyBytes) {
      if (b == ' ' || b == '\n' || b == '\r' || b == 0) {
        throw new IllegalArgumentException("MKey contains invalid characters:  ``"
                + mkey + "''");
      }
    }
  }

  @Override
  public CollectionFuture<Boolean> asyncSetAttr(String key, Attributes attrs) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<>(
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

  @Override
  public CollectionFuture<CollectionAttributes> asyncGetAttr(final String key) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<CollectionAttributes> rv = new CollectionFuture<>(
            latch, operationTimeout);
    Operation op = opFact.getAttr(key, new GetAttrOperation.Callback() {
      private final CollectionAttributes attrs = new CollectionAttributes();

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

  /**
   * Generic get operation for list items. Public methods for list items call this method.
   *
   * @param k             list item's key
   * @param collectionGet operation parameters (element key and so on)
   * @param tc            transcoder to serialize and unserialize value
   * @return future holding the fetched value
   */
  private <T> CollectionFuture<List<T>> asyncLopGet(final String k,
                                                    final CollectionGet collectionGet,
                                                    final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionGetFuture<List<T>> rv =
            new CollectionGetFuture<>(latch, operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
        new CollectionGetOperation.Callback() {
          private final List<CachedData> cachedDataList = new ArrayList<>();
          private final GetResult<List<T>> result = new LopGetResultImpl<>(cachedDataList, tc);

          public void receivedStatus(OperationStatus status) {
            CollectionOperationStatus cstatus;
            if (status instanceof CollectionOperationStatus) {
              cstatus = (CollectionOperationStatus) status;
            } else {
              getLogger().warn("Unhandled state: " + status);
              cstatus = new CollectionOperationStatus(status);
            }
            if (cstatus.isSuccess() || cstatus.getResponse() == CollectionResponse.NOT_FOUND_ELEMENT) {
              rv.setResult(result, cstatus);
              return;
            }

            rv.setResult(null, cstatus);
            getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
          }

          public void complete() {
            latch.countDown();
          }

          public void gotData(String subkey, int flags, byte[] data, byte[] eflag) {
            cachedDataList.add(new CachedData(flags, data, tc.getMaxSize()));
          }
        });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncSopExist(String key, T value,
                                                     Transcoder<T> tc) {
    SetExist<T> exist = new SetExist<>(value, tc);
    return asyncCollectionExist(key, "", exist);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopExist(String key, Object value) {
    SetExist<Object> exist = new SetExist<>(value, collectionTranscoder);
    return asyncCollectionExist(key, "", exist);
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
                                                   final CollectionGet collectionGet,
                                                   final Transcoder<T> tc) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionGetFuture<Set<T>> rv =
            new CollectionGetFuture<>(latch, operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
        new CollectionGetOperation.Callback() {
          private final HashSet<CachedData> cachedDataSet = new HashSet<>();
          private final GetResult<Set<T>> result = new SopGetResultImpl<>(cachedDataSet, tc);

          public void receivedStatus(OperationStatus status) {
            CollectionOperationStatus cstatus;
            if (status instanceof CollectionOperationStatus) {
              cstatus = (CollectionOperationStatus) status;
            } else {
              getLogger().warn("Unhandled state: " + status);
              cstatus = new CollectionOperationStatus(status);
            }
            if (cstatus.isSuccess() || cstatus.getResponse() == CollectionResponse.NOT_FOUND_ELEMENT) {
              rv.setResult(result, cstatus);
              return;
            }

            rv.setResult(null, cstatus);
            getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
          }

          public void complete() {
            latch.countDown();
          }

          public void gotData(String subkey, int flags, byte[] data, byte[] eflag) {
            cachedDataSet.add(new CachedData(flags, data, tc.getMaxSize()));
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
    final CollectionGetFuture<Map<Long, Element<T>>> rv =
            new CollectionGetFuture<>(latch, operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
        new CollectionGetOperation.Callback() {
          private final HashMap<Long, CachedData> cachedDataMap = new HashMap<>();
          private final GetResult<Map<Long, Element<T>>> result =
                  new BopGetResultImpl<>(cachedDataMap, reverse, tc);

          public void receivedStatus(OperationStatus status) {
            CollectionOperationStatus cstatus;
            if (status instanceof CollectionOperationStatus) {
              cstatus = (CollectionOperationStatus) status;
            } else {
              getLogger().warn("Unhandled state: " + status);
              cstatus = new CollectionOperationStatus(status);
            }
            if (cstatus.isSuccess() || cstatus.getResponse() == CollectionResponse.NOT_FOUND_ELEMENT) {
              rv.setResult(result, cstatus);
              return;
            }

            rv.setResult(null, cstatus);
            getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
          }

          public void complete() {
            latch.countDown();
          }

          public void gotData(String bKey, int flags, byte[] data, byte[] eflag) {
            cachedDataMap.put(Long.parseLong(bKey), new CachedData(flags, data, eflag, tc.getMaxSize()));
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
    final CollectionGetFuture<Map<String, T>> rv =
            new CollectionGetFuture<>(latch, operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
        new CollectionGetOperation.Callback() {
          private final HashMap<String, CachedData> cachedDataMap = new HashMap<>();
          private final GetResult<Map<String, T>> result
                  = new MopGetResultImpl<>(cachedDataMap, tc);

          public void receivedStatus(OperationStatus status) {
            CollectionOperationStatus cstatus;
            if (status instanceof CollectionOperationStatus) {
              cstatus = (CollectionOperationStatus) status;
            } else {
              getLogger().warn("Unhandled state: " + status);
              cstatus = new CollectionOperationStatus(status);
            }
            if (cstatus.isSuccess() || cstatus.getResponse() == CollectionResponse.NOT_FOUND_ELEMENT) {
              rv.setResult(result, cstatus);
              return;
            }

            rv.setResult(null, cstatus);
            getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
          }

          public void complete() {
            latch.countDown();
          }

          public void gotData(String mkey, int flags, byte[] data, byte[] eflag) {
            cachedDataMap.put(mkey, new CachedData(flags, data, eflag, tc.getMaxSize()));
          }
        });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  /**
   * Generic insert operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param key             collection item's key
   * @param subkey          element key (list index, b+tree bkey)
   * @param collectionInsert operation parameters (value, eflags, attributes, and so on)
   * @param tc              transcoder to serialize and unserialize value
   * @return future holding the success/failure of the operation
   */
  private <T> CollectionFuture<Boolean> asyncCollectionInsert(String key,
                                                              String subkey,
                                                              CollectionInsert<T> collectionInsert,
                                                              Transcoder<T> tc) {
    CachedData co = tc.encode(collectionInsert.getValue());
    collectionInsert.setFlags(co.getFlags());
    return asyncCollectionInsert(key, subkey, collectionInsert, co);
  }

  /**
   * Generic insert operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param key             collection item's key
   * @param subkey          element key (list index, b+tree bkey)
   * @param collectionInsert operation parameters (value, eflags, attributes, and so on)
   * @param co              transcoded value
   * @return future holding the success/failure of the operation
   */
  <T> CollectionFuture<Boolean> asyncCollectionInsert(final String key,
                                                      final String subkey,
                                                      final CollectionInsert<T> collectionInsert,
                                                      final CachedData co) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<>(
            latch, operationTimeout);
    Operation op = opFact.collectionInsert(key, subkey, collectionInsert,
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
                if (!cstatus.isSuccess()) {
                  getLogger().debug(
                          "Insertion to the collection failed : %s (type=%s, key=%s, subkey=%s, value=%s)",
                          cstatus.getMessage(),
                          collectionInsert.getClass().getName(),
                          key,
                          subkey,
                          collectionInsert.getValue());
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
   * Generic pipelined update operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param key        collection item's key
   * @param updateList list of operation parameters (values and so on)
   * @return future holding the success/failure codes of individual operations and their index
   */
  <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncCollectionPipedUpdate(
          final String key, final List<CollectionPipedUpdate<T>> updateList) {

    final CountDownLatch latch = new CountDownLatch(updateList.size());
    final PipedCollectionFuture<Integer, CollectionOperationStatus> rv =
            new PipedCollectionFuture<>(latch, operationTimeout);

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
              rv.setOperationStatus(cstatus);
            }

            // complete
            public void complete() {
              latch.countDown();
            }

            // got status
            public void gotStatus(Integer index, OperationStatus status) {
              if (status instanceof CollectionOperationStatus) {
                rv.addEachResult(index + (idx * CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT),
                                (CollectionOperationStatus) status);
              } else {
                rv.addEachResult(index + (idx * CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT),
                                new CollectionOperationStatus(status));
              }
            }
          });
      rv.addOperation(op);
      addOp(key, op);
    }
    return rv;
  }

  /**
   * Generic delete operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param key              collection item's key
   * @param collectionDelete operation parameters (element index/key, value, and so on)
   * @return future holding the success/failure of the operation
   */
  private CollectionFuture<Boolean> asyncCollectionDelete(
          final String key, final CollectionDelete collectionDelete) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<>(
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
            if (!cstatus.isSuccess()) {
              getLogger().debug("Deletion to the collection failed : %s (type=%s, key=%s)",
                      cstatus.getMessage(),
                      collectionDelete.getClass().getName(),
                      key);
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
   * Generic existence operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param key             collection item's key
   * @param subkey          element key (list index, b+tree bkey)
   * @param collectionExist operation parameters (element value and so on)
   * @return future holding the success/failure of the operation
   */
  private <T> CollectionFuture<Boolean> asyncCollectionExist(final String key, final String subkey,
                                                             final CollectionExist collectionExist) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<>(
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
            boolean isExist = CollectionResponse.EXIST == cstatus.getResponse();
            rv.set(isExist, cstatus);
            if (!cstatus.isSuccess()) {
              getLogger().debug("Exist command to the collection failed : %s (type=%s, key=%s, subkey=%s)",
                      cstatus.getMessage(),
                      collectionExist.getClass().getName(),
                      key,
                      subkey);
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

  @Deprecated
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(final List<String> keyList,
                                                                         final int exp, final T o,
                                                                         final Transcoder<T> tc) {
    validateKeys(keyList);

    final CachedData co = tc.encode(o);
    final CountDownLatch blatch = new CountDownLatch(keyList.size());
    final BulkOperationFuture<CollectionOperationStatus> rv = new BulkOperationFuture<>(blatch, operationTimeout);
    final Map<String, Operation> opMap = new HashMap<>();

    for (final String key : keyList) {
      Operation op = opFact.store(StoreType.set, key, co.getFlags(), exp, co.getData(),
          new OperationCallback() {
            public void receivedStatus(OperationStatus val) {
              if (!val.isSuccess()) {
                rv.addFailedResult(key, new CollectionOperationStatus(false, String
                        .valueOf(val.isSuccess()), CollectionResponse.END));
              }
            }

            public void complete() {
              blatch.countDown();
            }
          });
      opMap.put(key, op);
    }
    rv.addOperations(opMap.values());
    addOpMap(opMap);
    return rv;
  }

  @Deprecated
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSetBulk(List<String> key,
                                                                     int exp, Object o) {
    return asyncSetBulk(key, exp, o, transcoder);
  }

  @Deprecated
  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSetBulk(final Map<String, T> o,
                                                                         final int exp,
                                                                         final Transcoder<T> tc) {
    if (o == null) {
      throw new IllegalArgumentException("Map is null.");
    } else if (o.isEmpty()) {
      throw new IllegalArgumentException("Map is empty.");
    }

    for (final String key : o.keySet()) {
      validateKey(key);
    }

    final CountDownLatch latch = new CountDownLatch(o.size());
    final BulkOperationFuture<CollectionOperationStatus> rv = new BulkOperationFuture<>(latch, operationTimeout);
    final Map<String, Operation> opMap = new HashMap<>();

    for (final String key : o.keySet()) {
      final CachedData co = tc.encode(o.get(key));
      Operation op = opFact.store(StoreType.set, key, co.getFlags(), exp, co.getData(),
            new OperationCallback() {
              public void receivedStatus(OperationStatus val) {
                if (!val.isSuccess()) {
                  rv.addFailedResult(key, new CollectionOperationStatus(false, String
                          .valueOf(val.isSuccess()), CollectionResponse.END));
                }
              }

              public void complete() {
                latch.countDown();
              }
            });
      opMap.put(key, op);
    }
    rv.addOperations(opMap.values());
    addOpMap(opMap);
    return rv;
  }

  @Deprecated
  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSetBulk(Map<String, Object> o,
                                                                     int exp) {
    return asyncSetBulk(o, exp, transcoder);
  }


  @Override
  public <T> Future<Map<String, OperationStatus>> asyncStoreBulk(final StoreType type,
                                                                 final List<String> keyList,
                                                                 final int exp, final T o,
                                                                 final Transcoder<T> tc) {
    validateKeys(keyList);

    final CachedData co = tc.encode(o);
    final CountDownLatch latch = new CountDownLatch(keyList.size());
    final BulkOperationFuture<OperationStatus> rv = new BulkOperationFuture<>(latch, operationTimeout);
    final Map<String, Operation> opMap = new HashMap<>();

    for (final String key : keyList) {
      Operation op = opFact.store(type, key, co.getFlags(), exp, co.getData(),
            new OperationCallback() {
              public void receivedStatus(OperationStatus val) {
                if (!val.isSuccess()) {
                  rv.addFailedResult(key, val);
                }
              }

              public void complete() {
                latch.countDown();
              }
            });
      opMap.put(key, op);
    }
    rv.addOperations(opMap.values());
    addOpMap(opMap);
    return rv;
  }

  @Override
  public Future<Map<String, OperationStatus>> asyncStoreBulk(StoreType type,
                                                             List<String> key,
                                                             int exp, Object o) {
    return asyncStoreBulk(type, key, exp, o, transcoder);
  }

  @Override
  public <T> Future<Map<String, OperationStatus>> asyncStoreBulk(final StoreType type,
                                                                 final Map<String, T> o,
                                                                 final int exp,
                                                                 final Transcoder<T> tc) {
    if (o == null) {
      throw new IllegalArgumentException("Map is null.");
    } else if (o.isEmpty()) {
      throw new IllegalArgumentException("Map is empty.");
    }

    for (final String key : o.keySet()) {
      validateKey(key);
    }

    final CountDownLatch latch = new CountDownLatch(o.size());
    final BulkOperationFuture<OperationStatus> rv = new BulkOperationFuture<>(latch, operationTimeout);
    final Map<String, Operation> opMap = new HashMap<>();

    for (final String key : o.keySet()) {
      CachedData co = tc.encode(o.get(key));
      Operation op = opFact.store(type, key, co.getFlags(), exp, co.getData(),
          new OperationCallback() {
            public void receivedStatus(OperationStatus val) {
              if (!val.isSuccess()) {
                rv.addFailedResult(key, val);
              }
            }

            public void complete() {
              latch.countDown();
            }
          });
      opMap.put(key, op);
    }
    rv.addOperations(opMap.values());
    addOpMap(opMap);
    return rv;
  }

  @Override
  public Future<Map<String, OperationStatus>> asyncStoreBulk(StoreType type,
                                                             Map<String, Object> o,
                                                             int exp) {
    return asyncStoreBulk(type, o, exp, transcoder);
  }

  @Override
  public Future<Map<String, OperationStatus>> asyncDeleteBulk(List<String> keyList) {
    validateKeys(keyList);

    final CountDownLatch latch = new CountDownLatch(keyList.size());
    final BulkOperationFuture<OperationStatus> rv = new BulkOperationFuture<>(latch, operationTimeout);
    final Map<String, Operation> opMap = new HashMap<>();

    for (final String key : keyList) {
      Operation op = opFact.delete(key, new OperationCallback() {
          public void receivedStatus(OperationStatus val) {
            if (!val.isSuccess()) {
              rv.addFailedResult(key, val);
            }
          }

          public void complete() {
            latch.countDown();
          }
      });
      opMap.put(key, op);
    }
    rv.addOperations(opMap.values());
    addOpMap(opMap);
    return rv;
  }

  /**
   * @deprecated no longer support varargs as collection has the same role.
   * use {@link ArcusClient#asyncDeleteBulk(List)} instead.
   */
  @Deprecated
  @Override
  public Future<Map<String, OperationStatus>> asyncDeleteBulk(String... key) {
    if (key == null) {
      throw new IllegalArgumentException("Key list is null.");
    }
    return asyncDeleteBulk(Arrays.asList(key));
  }

  @Override
  public int getMaxPipedItemCount() {
    return CollectionPipedInsert.MAX_PIPED_ITEM_COUNT;
  }

  @Override
  public CollectionFuture<Boolean> asyncBopCreate(String key,
                                                  ElementValueType valueType,
                                                  CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(valueType);
    boolean noreply = false;
    CollectionCreate bTreeCreate = new BTreeCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getOverflowAction(), attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, bTreeCreate);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopCreate(String key,
                                                  ElementValueType type,
                                                  CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(type);
    boolean noreply = false;
    CollectionCreate mapCreate = new MapCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, mapCreate);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopCreate(String key,
                                                  ElementValueType type,
                                                  CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(type);
    boolean noreply = false;
    CollectionCreate setCreate = new SetCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, setCreate);
  }

  @Override
  public CollectionFuture<Boolean> asyncLopCreate(String key,
                                                  ElementValueType type,
                                                  CollectionAttributes attributes) {
    int flag = CollectionTranscoder.examineFlags(type);
    boolean noreply = false;
    CollectionCreate listCreate = new ListCreate(flag,
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getOverflowAction(), attributes.getReadable(), noreply);
    return asyncCollectionCreate(key, listCreate);
  }

  /**
   * Generic create operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param key              collection item's key
   * @param collectionCreate operation parameters (flags, expiration time, and so on)
   * @return future holding the success/failure of the operation
   */
  CollectionFuture<Boolean> asyncCollectionCreate(final String key,
                                                  final CollectionCreate collectionCreate) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Boolean> rv = new CollectionFuture<>(
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
            if (!cstatus.isSuccess()) {
              getLogger().debug("Insertion to the collection failed : %s (type=%s, key=%s, attribute=%s)",
                              cstatus.getMessage(),
                              collectionCreate.getClass().getName(),
                              key,
                              collectionCreate.toString());
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

//  @Override
//  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key, long bkey,
//                                                                  ElementFlagFilter eFlagFilter) {
//    return asyncBopGet(key, bkey, eFlagFilter, false, false);
//  }

  @Override
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long bkey,
                                                                  ElementFlagFilter eFlagFilter,
                                                                  boolean withDelete,
                                                                  boolean dropIfEmpty) {
    BTreeUtil.validateBkey(bkey);
    BTreeGet get = new BTreeGet(bkey, withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopGet(key, get, false, collectionTranscoder);
  }

//  @Override
//  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
//                                                                  long from, long to,
//                                                                  ElementFlagFilter eFlagFilter,
//                                                                  int offset, int count) {
//    return asyncBopGet(key, from, to, eFlagFilter, offset, count, false, false);
//  }

  @Override
  public CollectionFuture<Map<Long, Element<Object>>> asyncBopGet(String key,
                                                                  long from, long to,
                                                                  ElementFlagFilter eFlagFilter,
                                                                  int offset, int count,
                                                                  boolean withDelete,
                                                                  boolean dropIfEmpty) {
    BTreeUtil.validateBkey(from, to);
    BTreeGet get = new BTreeGet(from, to, offset, count, withDelete, dropIfEmpty, eFlagFilter);
    boolean reverse = from > to;
    return asyncBopGet(key, get, reverse, collectionTranscoder);
  }

//  @Override
//  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
//                                                                 long bkey,
//                                                                 ElementFlagFilter eFlagFilter,
//                                                                 Transcoder<T> tc) {
//    return asyncBopGet(key, bkey, eFlagFilter, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long bkey,
                                                                 ElementFlagFilter eFlagFilter,
                                                                 boolean withDelete,
                                                                 boolean dropIfEmpty,
                                                                 Transcoder<T> tc) {
    BTreeUtil.validateBkey(bkey);
    BTreeGet get = new BTreeGet(bkey, withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopGet(key, get, false, tc);
  }

//  @Override
//  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
//                                                                 long from, long to,
//                                                                 ElementFlagFilter eFlagFilter,
//                                                                 int offset, int count,
//                                                                 Transcoder<T> tc) {
//    return asyncBopGet(key, from, to, eFlagFilter, offset, count, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<Long, Element<T>>> asyncBopGet(String key,
                                                                 long from, long to,
                                                                 ElementFlagFilter eFlagFilter,
                                                                 int offset, int count,
                                                                 boolean withDelete,
                                                                 boolean dropIfEmpty,
                                                                 Transcoder<T> tc) {
    BTreeUtil.validateBkey(from, to);
    BTreeGet get = new BTreeGet(from, to, offset, count, withDelete, dropIfEmpty, eFlagFilter);
    boolean reverse = from > to;
    return asyncBopGet(key, get, reverse, tc);
  }

//  @Override
//  public CollectionFuture<Map<String, Object>> asyncMopGet(String key) {
//    return asyncMopGet(key, false, false);
//  }

  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           boolean withDelete,
                                                           boolean dropIfEmpty) {
    List<String> mkeyList = new ArrayList<>();
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, collectionTranscoder);
  }

//  @Override
//  public CollectionFuture<Map<String, Object>> asyncMopGet(String key, String mkey) {
//    return asyncMopGet(key, mkey, false, false);
//  }

  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           String mkey,
                                                           boolean withDelete,
                                                           boolean dropIfEmpty) {
    if (mkey == null) {
      throw new IllegalArgumentException("mkey is null");
    }
    validateMKey(mkey);
    List<String> mkeyList = new ArrayList<>(1);
    mkeyList.add(mkey);
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, collectionTranscoder);
  }

//  @Override
//  public CollectionFuture<Map<String, Object>> asyncMopGet(String key, List<String> mkeyList) {
//    return asyncMopGet(key, mkeyList, false, false);
//  }

  @Override
  public CollectionFuture<Map<String, Object>> asyncMopGet(String key,
                                                           List<String> mkeyList,
                                                           boolean withDelete,
                                                           boolean dropIfEmpty) {
    if (mkeyList == null) {
      throw new IllegalArgumentException("mkeyList is null");
    }
    for (int i = 0; i < mkeyList.size(); i++) {
      validateMKey(mkeyList.get(i));
    }
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, collectionTranscoder);
  }

//  @Override
//  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, Transcoder<T> tc) {
//    return asyncMopGet(key, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          boolean withDelete, boolean dropIfEmpty,
                                                          Transcoder<T> tc) {
    List<String> mkeyList = new ArrayList<>();
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, tc);
  }

//  @Override
//  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, String mkey, Transcoder<T> tc) {
//    return asyncMopGet(key, mkey, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          String mkey,
                                                          boolean withDelete, boolean dropIfEmpty,
                                                          Transcoder<T> tc) {
    if (mkey == null) {
      throw new IllegalArgumentException("mkey is null");
    }
    validateMKey(mkey);
    List<String> mkeyList = new ArrayList<>(1);
    mkeyList.add(mkey);
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, tc);
  }

//  @Override
//  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key, List<String> mkeyList, Transcoder<T> tc) {
//    return asyncMopGet(key, mkeyList, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<String, T>> asyncMopGet(String key,
                                                          List<String> mkeyList,
                                                          boolean withDelete, boolean dropIfEmpty,
                                                          Transcoder<T> tc) {
    if (mkeyList == null) {
      throw new IllegalArgumentException("mkeyList is null");
    }
    for (int i = 0; i < mkeyList.size(); i++) {
      validateMKey(mkeyList.get(i));
    }
    MapGet get = new MapGet(mkeyList, withDelete, dropIfEmpty);
    return asyncMopGet(key, get, tc);
  }

//  @Override
//  public CollectionFuture<List<Object>> asyncLopGet(String key, int index) {
//    return asyncLopGet(key, index, false, false);
//  }

  @Override
  public CollectionFuture<List<Object>> asyncLopGet(String key, int index,
                                                    boolean withDelete, boolean dropIfEmpty) {
    ListGet get = new ListGet(index, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, collectionTranscoder);
  }

//  @Override
//  public CollectionFuture<List<Object>> asyncLopGet(String key, int from, int to) {
//    return asyncLopGet(key, from, to, false, false);
//  }

  @Override
  public CollectionFuture<List<Object>> asyncLopGet(String key,
                                                    int from, int to,
                                                    boolean withDelete, boolean dropIfEmpty) {
    ListGet get = new ListGet(from, to, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, collectionTranscoder);
  }

//  @Override
//  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int index, Transcoder<T> tc) {
//    return asyncLopGet(key, index, false, false, tc);
//  }
  @Override
  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int index,
                                                   boolean withDelete, boolean dropIfEmpty,
                                                   Transcoder<T> tc) {
    ListGet get = new ListGet(index, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, tc);
  }

//  @Override
//  public <T> CollectionFuture<List<T>> asyncLopGet(String key, int from, int to, Transcoder<T> tc) {
//    return asyncLopGet(key, from, to, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<List<T>> asyncLopGet(String key,
                                                   int from, int to,
                                                   boolean withDelete, boolean dropIfEmpty,
                                                   Transcoder<T> tc) {
    ListGet get = new ListGet(from, to, withDelete, dropIfEmpty);
    return asyncLopGet(key, get, tc);
  }

//  @Override
//  public CollectionFuture<Set<Object>> asyncSopGet(String key, int count) {
//    return asyncSopGet(key, count, false, false);
//  }

  @Override
  public CollectionFuture<Set<Object>> asyncSopGet(String key, int count,
                                                   boolean withDelete, boolean dropIfEmpty) {
    SetGet get = new SetGet(count, withDelete, dropIfEmpty);
    return asyncSopGet(key, get, collectionTranscoder);
  }

//  @Override
//  public <T> CollectionFuture<Set<T>> asyncSopGet(String key, int count, Transcoder<T> tc) {
//    return asyncSopGet(key, count, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Set<T>> asyncSopGet(String key, int count,
                                                  boolean withDelete, boolean dropIfEmpty,
                                                  Transcoder<T> tc) {
    SetGet get = new SetGet(count, withDelete, dropIfEmpty);
    return asyncSopGet(key, get, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key, long bkey,
                                                  ElementFlagFilter eFlagFilter,
                                                  boolean dropIfEmpty) {
    BTreeUtil.validateBkey(bkey);
    BTreeDelete delete = new BTreeDelete(bkey, false, dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key,
                                                  long from, long to,
                                                  ElementFlagFilter eFlagFilter, int count,
                                                  boolean dropIfEmpty) {
    BTreeUtil.validateBkey(from, to);
    BTreeDelete delete = new BTreeDelete(from, to, count, false, dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopDelete(String key,
                                                  boolean dropIfEmpty) {
    List<String> mkeyList = new ArrayList<>();
    MapDelete delete = new MapDelete(mkeyList, false, dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopDelete(String key, String mkey,
                                                  boolean dropIfEmpty) {
    if (mkey == null) {
      throw new IllegalArgumentException("mkey is null");
    }
    validateMKey(mkey);
    List<String> mkeyList = new ArrayList<>(1);
    mkeyList.add(mkey);
    MapDelete delete = new MapDelete(mkeyList, false, dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

//  @Override
//  public CollectionFuture<Boolean> asyncMopDelete(String key,
//                                                  List<String> mkeyList,
//                                                  boolean dropIfEmpty) {
//    if (mkeyList == null) {
//      throw new IllegalArgumentException("mkeyList is null");
//    }
//    for (int i = 0; i < mkeyList.size(); i++) {
//      validateMKey(mkeyList.get(i));
//    }
//    MapDelete delete = new MapDelete(mkeyList, false, dropIfEmpty);
//    return asyncCollectionDelete(key, delete);
//  }

  @Override
  public CollectionFuture<Boolean> asyncLopDelete(String key, int index,
                                                  boolean dropIfEmpty) {
    ListDelete delete = new ListDelete(index, false, dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public CollectionFuture<Boolean> asyncLopDelete(String key, int from,
                                                  int to, boolean dropIfEmpty) {
    ListDelete delete = new ListDelete(from, to, false, dropIfEmpty);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopDelete(String key, Object value,
                                                  boolean dropIfEmpty) {
    SetDelete<Object> delete = new SetDelete<>(value, false, dropIfEmpty,
            collectionTranscoder);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncSopDelete(String key, T value,
                                                      boolean dropIfEmpty, Transcoder<T> tc) {
    SetDelete<T> delete = new SetDelete<>(value, false, dropIfEmpty, tc);
    return asyncCollectionDelete(key, delete);
  }

  /**
   * Generic count operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param k               collection item's key
   * @param collectionCount operation parameters (element key range, eflags, and so on)
   * @return future holding the element count
   */
  private CollectionFuture<Integer> asyncCollectionCount(final String k,
                                                         final CollectionCount collectionCount) {

    final CountDownLatch latch = new CountDownLatch(1);

    final CollectionFuture<Integer> rv = new CollectionFuture<>(
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
              rv.set(Integer.valueOf(cstatus.getMessage()),
                      new CollectionOperationStatus(new OperationStatus(true, "END")));
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

  @Override
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        long from, long to,
                                                        ElementFlagFilter eFlagFilter) {
    BTreeUtil.validateBkey(from, to);
    CollectionCount collectionCount = new BTreeCount(from, to, eFlagFilter);
    return asyncCollectionCount(key, collectionCount);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                  byte[] eFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    BTreeUtil.validateBkey(bkey);
    BTreeInsert<Object> bTreeInsert = new BTreeInsert<>(value, eFlag, null, attributesForCreate);
    return asyncCollectionInsert(key, String.valueOf(bkey), bTreeInsert, collectionTranscoder);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                  Object value,
                                                  CollectionAttributes attributesForCreate) {
    validateMKey(mkey);
    MapInsert<Object> mapInsert = new MapInsert<>(value, null, attributesForCreate);
    return asyncCollectionInsert(key, mkey, mapInsert, collectionTranscoder);
  }

  @Override
  public CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                  Object value,
                                                  CollectionAttributes attributesForCreate) {
    ListInsert<Object> listInsert = new ListInsert<>(value, null, attributesForCreate);
    return asyncCollectionInsert(key, String.valueOf(index), listInsert, collectionTranscoder);
  }

  @Override
  public CollectionFuture<Boolean> asyncSopInsert(String key, Object value,
                                                  CollectionAttributes attributesForCreate) {
    SetInsert<Object> setInsert = new SetInsert<>(value, null, attributesForCreate);
    return asyncCollectionInsert(key, "", setInsert, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key, long bkey,
                                                      byte[] eFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    BTreeUtil.validateBkey(bkey);
    BTreeInsert<T> bTreeInsert = new BTreeInsert<>(value, eFlag, null, attributesForCreate);
    return asyncCollectionInsert(key, String.valueOf(bkey), bTreeInsert, tc);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncMopInsert(String key, String mkey,
                                                      T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    validateMKey(mkey);
    MapInsert<T> mapInsert = new MapInsert<>(value, null, attributesForCreate);
    return asyncCollectionInsert(key, mkey, mapInsert, tc);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncLopInsert(String key, int index,
                                                      T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    ListInsert<T> listInsert = new ListInsert<>(value, null, attributesForCreate);
    return asyncCollectionInsert(key, String.valueOf(index), listInsert, tc);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncSopInsert(String key, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    SetInsert<T> setInsert = new SetInsert<>(value, null, attributesForCreate);
    return asyncCollectionInsert(key, "", setInsert, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, Object> elements,
          CollectionAttributes attributesForCreate) {
    return asyncBopPipedInsertBulk(key, elements, attributesForCreate, collectionTranscoder);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<Object>> elements,
          CollectionAttributes attributesForCreate) {
    return asyncBopPipedInsertBulk(key, elements, attributesForCreate, collectionTranscoder);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, Object> elements,
          CollectionAttributes attributesForCreate) {
    return asyncMopPipedInsertBulk(key, elements, attributesForCreate, collectionTranscoder);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<Object> valueList, CollectionAttributes attributesForCreate) {
    return asyncLopPipedInsertBulk(key, index, valueList, attributesForCreate,
            collectionTranscoder);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<Object> valueList, CollectionAttributes attributesForCreate) {
    return asyncSopPipedInsertBulk(key, valueList, attributesForCreate,
            collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, Map<Long, T> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    if (elements.isEmpty()) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }

    List<CollectionPipedInsert<T>> insertList = new ArrayList<>();

    if (elements.size() <= CollectionPipedInsert.MAX_PIPED_ITEM_COUNT) {
      insertList.add(new BTreePipedInsert<>(key, elements, attributesForCreate, tc));
    } else {
      PartitionedMap<Long, T> list = new PartitionedMap<>(
              elements, CollectionPipedInsert.MAX_PIPED_ITEM_COUNT);
      for (Map<Long, T> elementMap : list) {
        insertList.add(new BTreePipedInsert<>(key, elementMap, attributesForCreate, tc));
      }
    }
    return asyncCollectionPipedInsert(key, insertList);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedInsertBulk(
          String key, List<Element<T>> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    if (elements.isEmpty()) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }

    List<CollectionPipedInsert<T>> insertList = new ArrayList<>();

    if (elements.size() <= CollectionPipedInsert.MAX_PIPED_ITEM_COUNT) {
      insertList.add(new ByteArraysBTreePipedInsert<>(key, elements, attributesForCreate, tc));
    } else {
      PartitionedList<Element<T>> list = new PartitionedList<>(
              elements, CollectionPipedInsert.MAX_PIPED_ITEM_COUNT);
      for (List<Element<T>> elementList : list) {
        insertList.add(new ByteArraysBTreePipedInsert<>(key, elementList, attributesForCreate, tc));
      }
    }
    return asyncCollectionPipedInsert(key, insertList);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedInsertBulk(
          String key, Map<String, T> elements,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    if (elements.isEmpty()) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }

    for (Map.Entry<String, T> checkMKey : elements.entrySet()) {
      validateMKey(checkMKey.getKey());
    }

    List<CollectionPipedInsert<T>> insertList = new ArrayList<>();

    if (elements.size() <= CollectionPipedInsert.MAX_PIPED_ITEM_COUNT) {
      insertList.add(new MapPipedInsert<>(key, elements, attributesForCreate, tc));
    } else {
      PartitionedMap<String, T> list = new PartitionedMap<>(
              elements, CollectionPipedInsert.MAX_PIPED_ITEM_COUNT);
      for (Map<String, T> elementMap : list) {
        insertList.add(new MapPipedInsert<>(key, elementMap, attributesForCreate, tc));
      }
    }
    return asyncCollectionPipedInsert(key, insertList);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncLopPipedInsertBulk(
          String key, int index, List<T> valueList,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    if (valueList.isEmpty()) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }

    List<CollectionPipedInsert<T>> insertList = new ArrayList<>();

    if (valueList.size() <= CollectionPipedInsert.MAX_PIPED_ITEM_COUNT) {
      insertList.add(new ListPipedInsert<>(key, index, valueList, attributesForCreate, tc));
    } else {
      PartitionedList<T> list = new PartitionedList<>(valueList,
              CollectionPipedInsert.MAX_PIPED_ITEM_COUNT);
      for (List<T> elementList : list) {
        insertList.add(new ListPipedInsert<>(key, index, elementList, attributesForCreate, tc));
      }
    }
    return asyncCollectionPipedInsert(key, insertList);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncSopPipedInsertBulk(
          String key, List<T> valueList,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    if (valueList.isEmpty()) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }

    List<CollectionPipedInsert<T>> insertList = new ArrayList<>();

    if (valueList.size() <= CollectionPipedInsert.MAX_PIPED_ITEM_COUNT) {
      insertList.add(new SetPipedInsert<>(key, valueList, attributesForCreate, tc));
    } else {
      PartitionedList<T> list = new PartitionedList<>(valueList,
              CollectionPipedInsert.MAX_PIPED_ITEM_COUNT);
      for (List<T> elementList : list) {
        insertList.add(new SetPipedInsert<>(key, elementList, attributesForCreate, tc));
      }
    }
    return asyncCollectionPipedInsert(key, insertList);
  }

  @Override
  public OperationFuture<Boolean> flush(final String prefix) {
    return flush(prefix, -1);
  }

  @Override
  public OperationFuture<Boolean> flush(final String prefix, final int delay) {
    Collection<MemcachedNode> nodes = getAllNodes();
    final BroadcastFuture<Boolean> rv
            = new BroadcastFuture<>(operationTimeout, Boolean.TRUE, nodes.size());
    final Map<MemcachedNode, Operation> opsMap = new HashMap<>();

    checkState();
    for (MemcachedNode node : nodes) {
      Operation op = opFact.flush(prefix, delay, false, new OperationCallback() {
        @Override
        public void receivedStatus(OperationStatus status) {
          if (!status.isSuccess()) {
            rv.set(Boolean.FALSE, status);
          }
        }

        @Override
        public void complete() {
          rv.complete();
        }
      });
      opsMap.put(node, op);
    }
    rv.addOperations(opsMap.values());
    getMemcachedConnection().addOperations(opsMap);
    return rv;
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter,
          int offset, int count) {
    validateKeys(keyList);
    checkDupKey(keyList);
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

    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<>(
            arrangedKey.size());
    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      if (arrangedKey.size() > 1) {
        smGetList.add(new BTreeSMGetWithLongTypeBkeyOld<>(entry.getKey(),
                entry.getValue(), from, to, eFlagFilter, 0, offset + count));
      } else {
        smGetList.add(new BTreeSMGetWithLongTypeBkeyOld<>(entry.getKey(),
                entry.getValue(), from, to, eFlagFilter, offset, count));
      }
    }
    return smget(smGetList, offset, count, (from > to), collectionTranscoder);
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter,
          int count, SMGetMode smgetMode) {
    validateKeys(keyList);
    checkDupKey(keyList);
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException("The count must not exceed a maximum of "
              + MAX_SMGET_COUNT + ".");
    }

    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<>(
            arrangedKey.size());
    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      smGetList.add(new BTreeSMGetWithLongTypeBkey<>(entry.getKey(),
              entry.getValue(), from, to, eFlagFilter, count, smgetMode));
    }
    return smget(smGetList, count, smgetMode == SMGetMode.UNIQUE, (from > to), collectionTranscoder);
  }

  /**
   * Turn the list of keys into groups of keys.
   * All keys in a group belong to the same memcached server.
   *
   * @param keyList   list of keys
   * @param maxKeyCountPerGroup max size of the key group (number of keys)
   * @return list of grouped (memcached node + keys) in the group
   */
  private Collection<Entry<MemcachedNode, List<String>>> groupingKeys(List<String> keyList, int maxKeyCountPerGroup) {
    List<Entry<MemcachedNode, List<String>>> resultList = new ArrayList<>();
    Map<MemcachedNode, List<String>> nodeMap = new HashMap<>();
    MemcachedConnection conn = getMemcachedConnection();

    for (String key : keyList) {
      MemcachedNode qa = conn.findNodeByKey(key);
      List<String> keyGroup = nodeMap.get(qa);

      if (keyGroup == null) {
        keyGroup = new ArrayList<>();
        nodeMap.put(qa, keyGroup);
      } else if (keyGroup.size() >= maxKeyCountPerGroup) {
        resultList.add(new AbstractMap.SimpleEntry<>(qa, keyGroup));
        keyGroup = new ArrayList<>();
        nodeMap.put(qa, keyGroup);
      }
      keyGroup.add(key);
    }
    // Add the Entry instance which is not full(smaller than groupSize) to the result.
    resultList.addAll(nodeMap.entrySet());
    return resultList;
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
  @Deprecated
  private <T> SMGetFuture<List<SMGetElement<T>>> smget(
          final List<BTreeSMGet<T>> smGetList, final int offset,
          final int count, final boolean reverse, final Transcoder<T> tc) {

    final CountDownLatch blatch = new CountDownLatch(smGetList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<>();
    final SMGetResultOldImpl<T> result = new SMGetResultOldImpl<>(offset, count, reverse, smGetList.size() > 1);

    // if processedSMGetCount is 0, then all smget is done.
    final AtomicInteger processedSMGetCount = new AtomicInteger(smGetList.size());
    final AtomicBoolean stopCollect = new AtomicBoolean(false);

    for (BTreeSMGet<T> smGet : smGetList) {
      Operation op = opFact.bopsmget(smGet, new BTreeSortMergeGetOperationOld.Callback() {
        private final List<SMGetElement<T>> eachResult = new ArrayList<>();

        @Override
        public void receivedStatus(OperationStatus status) {
          final int processed = processedSMGetCount.decrementAndGet();

          if (status.isSuccess()) {
            boolean isTrimmed = ("TRIMMED".equals(status.getMessage()) ||
                    "DUPLICATED_TRIMMED".equals(status.getMessage()));

            result.mergeSMGetElements(eachResult, isTrimmed);
            if (processed == 0) {
              result.makeResultOperationStatus();
            }
          } else {
            stopCollect.set(true);
            result.setFailedOperationStatus(status);
            getLogger().warn("SMGetFailed. status=%s", status);
          }
        }

        @Override
        public void complete() {
          blatch.countDown();
        }

        @Override
        public void gotData(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
          if (stopCollect.get()) {
            return;
          }

          if (bkey instanceof Long) {
            eachResult.add(new SMGetElement<>(key, (Long) bkey, eflag,
                    tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          } else if (bkey instanceof byte[]) {
            eachResult.add(new SMGetElement<>(key, (byte[]) bkey, eflag,
                    tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          }
        }

        @Override
        public void gotMissedKey(byte[] data) {
          OperationStatus status = new OperationStatus(false, "UNDEFINED");
          result.addMissedKey(new String(data), new CollectionOperationStatus(status));
        }
      });
      ops.add(op);
      addOp(smGet.getMemcachedNode(), op);
    }

    return new SMGetFuture<>(ops, result, blatch, operationTimeout);
  }

  private <T> SMGetFuture<List<SMGetElement<T>>> smget(
          final List<BTreeSMGet<T>> smGetList, final int count, final boolean unique,
          final boolean reverse, final Transcoder<T> tc) {

    final CountDownLatch blatch = new CountDownLatch(smGetList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<>();
    final SMGetResultImpl<T> result = new SMGetResultImpl<>(count, unique, reverse);

    // if processedSMGetCount is 0, then all smget is done.
    final AtomicInteger processedSMGetCount = new AtomicInteger(smGetList.size());
    final AtomicBoolean stopCollect = new AtomicBoolean(false);

    for (BTreeSMGet<T> smGet : smGetList) {
      Operation op = opFact.bopsmget(smGet, new BTreeSortMergeGetOperation.Callback() {
        private final List<SMGetElement<T>> eachResult = new ArrayList<>();

        @Override
        public void receivedStatus(OperationStatus status) {
          final int processed = processedSMGetCount.decrementAndGet();

          if (status.isSuccess()) {
            result.mergeSMGetElements(eachResult);
            if (processed == 0) {
              result.makeResultOperationStatus();
            }
          } else {
            stopCollect.set(true);
            result.setFailedOperationStatus(status);
            getLogger().warn("SMGetFailed. status=%s", status);
          }
        }

        @Override
        public void complete() {
          blatch.countDown();
        }

        @Override
        public void gotData(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
          if (stopCollect.get()) {
            return;
          }

          if (bkey instanceof Long) {
            eachResult.add(new SMGetElement<>(key, (Long) bkey, eflag,
                    tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          } else {
            eachResult.add(new SMGetElement<>(key, (byte[]) bkey, eflag,
                    tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
          }
        }

        @Override
        public void gotMissedKey(String key, OperationStatus cause) {
          if (stopCollect.get()) {
            return;
          }

          result.addMissedKey(key, new CollectionOperationStatus(cause));
        }

        @Override
        public void gotTrimmedKey(String key, Object bkey) {
          if (stopCollect.get()) {
            return;
          }

          if (bkey instanceof Long) {
            result.addTrimmedKey(key, new BKeyObject((Long) bkey));
          } else {
            result.addTrimmedKey(key, new BKeyObject((byte[]) bkey));
          }
        }
      });
      ops.add(op);
      addOp(smGet.getMemcachedNode(), op);
    }

    return new SMGetFuture<>(ops, result, blatch, operationTimeout);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                  byte[] elementFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {

    BTreeUtil.validateBkey(bkey);
    BTreeUpsert<Object> bTreeUpsert = new BTreeUpsert<>(value, elementFlag, null, attributesForCreate);

    return asyncCollectionInsert(key, String.valueOf(bkey), bTreeUpsert,
            collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key, long bkey,
                                                      byte[] elementFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {

    BTreeUtil.validateBkey(bkey);
    BTreeUpsert<T> bTreeUpsert = new BTreeUpsert<>(value, elementFlag, null, attributesForCreate);

    return asyncCollectionInsert(key, String.valueOf(bkey), bTreeUpsert, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                  ElementFlagUpdate eFlagUpdate, Object value) {
    BTreeUtil.validateBkey(bkey);
    BTreeUpdate<Object> collectionUpdate = new BTreeUpdate<>(value, eFlagUpdate, false);
    return asyncCollectionUpdate(key, String.valueOf(bkey), collectionUpdate,
            collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey,
                                                      ElementFlagUpdate eFlagUpdate, T value,
                                                      Transcoder<T> tc) {
    BTreeUtil.validateBkey(bkey);
    BTreeUpdate<T> collectionUpdate = new BTreeUpdate<>(value, eFlagUpdate, false);
    return asyncCollectionUpdate(key, String.valueOf(bkey), collectionUpdate, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpdate(String key,
                                                  byte[] bkey, ElementFlagUpdate eFlagUpdate,
                                                  Object value) {
    BTreeUtil.validateBkey(bkey);
    BTreeUpdate<Object> collectionUpdate = new BTreeUpdate<>(value, eFlagUpdate, false);
    return asyncCollectionUpdate(key, BTreeUtil.toHex(bkey), collectionUpdate,
            collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpdate(String key,
                                                      byte[] bkey,
                                                      ElementFlagUpdate eFlagUpdate, T value,
                                                      Transcoder<T> tc) {
    BTreeUtil.validateBkey(bkey);
    BTreeUpdate<T> collectionUpdate = new BTreeUpdate<>(value, eFlagUpdate, false);
    return asyncCollectionUpdate(key, BTreeUtil.toHex(bkey), collectionUpdate, tc);
  }

  @Override
  public CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                  Object value) {
    validateMKey(mkey);
    MapUpdate<Object> collectionUpdate = new MapUpdate<>(value, false);
    return asyncCollectionUpdate(key, mkey, collectionUpdate,
            collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey,
                                                      T value, Transcoder<T> tc) {
    validateMKey(mkey);
    MapUpdate<T> collectionUpdate = new MapUpdate<>(value, false);
    return asyncCollectionUpdate(key, mkey, collectionUpdate, tc);
  }

  /**
   * Generic update operation for collection items.
   * Public methods for collection items call this method.
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
    final CollectionFuture<Boolean> rv = new CollectionFuture<>(
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
                if (!cstatus.isSuccess()) {
                  getLogger().debug(
                          "Insertion to the collection failed : %s (type=%s, key=%s, subkey=%s, value=%s)",
                          cstatus.getMessage(),
                          collectionUpdate.getClass().getName(),
                          key,
                          subkey,
                          collectionUpdate.getNewValue());
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

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<Object>> elements) {
    return asyncBopPipedUpdateBulk(key, elements, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncBopPipedUpdateBulk(
          String key, List<Element<T>> elements, Transcoder<T> tc) {

    if (elements.isEmpty()) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }

    List<CollectionPipedUpdate<T>> updateList = new ArrayList<>();

    if (elements.size() <= CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT) {
      updateList.add(new BTreePipedUpdate<>(key, elements, tc));
    } else {
      PartitionedList<Element<T>> list = new PartitionedList<>(
              elements, CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT);
      for (List<Element<T>> elementList : list) {
        updateList.add(new BTreePipedUpdate<>(key, elementList, tc));
      }
    }
    return asyncCollectionPipedUpdate(key, updateList);
  }

  @Override
  public CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, Object> elements) {
    return asyncMopPipedUpdateBulk(key, elements, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncMopPipedUpdateBulk(
          String key, Map<String, T> elements, Transcoder<T> tc) {

    if (elements.isEmpty()) {
      throw new IllegalArgumentException(
              "The number of piped operations must be larger than 0.");
    }

    for (Map.Entry<String, T> checkMKey : elements.entrySet()) {
      validateMKey(checkMKey.getKey());
    }

    List<CollectionPipedUpdate<T>> updateList = new ArrayList<>();

    if (elements.size() <= CollectionPipedInsert.MAX_PIPED_ITEM_COUNT) {
      updateList.add(new MapPipedUpdate<>(key, elements, tc));
    } else {
      PartitionedMap<String, T> list = new PartitionedMap<>(
              elements, CollectionPipedUpdate.MAX_PIPED_ITEM_COUNT);

      for (Map<String, T> elementMap : list) {
        updateList.add(new MapPipedUpdate<>(key, elementMap, tc));
      }
    }
    return asyncCollectionPipedUpdate(key, updateList);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopInsert(String key, byte[] bkey,
                                                  byte[] eFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    BTreeUtil.validateBkey(bkey);
    BTreeInsert<Object> bTreeInsert = new BTreeInsert<>(value, eFlag, null, attributesForCreate);
    return asyncCollectionInsert(key, BTreeUtil.toHex(bkey), bTreeInsert,
            collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopInsert(String key,
                                                      byte[] bkey, byte[] eFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    BTreeUtil.validateBkey(bkey);
    BTreeInsert<T> bTreeInsert = new BTreeInsert<>(value, eFlag, null, attributesForCreate);
    return asyncCollectionInsert(key, BTreeUtil.toHex(bkey), bTreeInsert, tc);
  }

//  @Override
//  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
//          String key, byte[] bkey, ElementFlagFilter eFlagFilter) {
//    return asyncBopGet(key, bkey, eFlagFilter, false, false);
//  }

  @Override
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter,
          boolean withDelete, boolean dropIfEmpty) {
    BTreeUtil.validateBkey(bkey);
    BTreeGet get = new BTreeGet(bkey, withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopExtendedGet(key, get, false, collectionTranscoder);
  }

//  @Override
//  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
//          String key, byte[] bkey, ElementFlagFilter eFlagFilter, Transcoder<T> tc) {
//    return asyncBopGet(key, bkey, eFlagFilter, false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] bkey, ElementFlagFilter eFlagFilter,
          boolean withDelete, boolean dropIfEmpty, Transcoder<T> tc) {
    BTreeUtil.validateBkey(bkey);
    BTreeGet get = new BTreeGet(bkey, withDelete, dropIfEmpty, eFlagFilter);
    return asyncBopExtendedGet(key, get, false, tc);
  }

//  @Override
//  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
//          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
//          int offset, int count) {
//    return asyncBopGet(key, from, to, eFlagFilter, offset, count, false, false);
//  }

  @Override
  public CollectionFuture<Map<ByteArrayBKey, Element<Object>>> asyncBopGet(
          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
          int offset, int count, boolean withDelete, boolean dropIfEmpty) {
    BTreeUtil.validateBkey(from, to);
    BTreeGet get = new BTreeGet(from, to, offset, count, withDelete, dropIfEmpty, eFlagFilter);
    boolean reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;
    return asyncBopExtendedGet(key, get, reverse, collectionTranscoder);
  }

//  @Override
//  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
//          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset,
//          int count, Transcoder<T> tc) {
//    return asyncBopGet(key, from, to, eFlagFilter, offset, count,
//            false, false, tc);
//  }

  @Override
  public <T> CollectionFuture<Map<ByteArrayBKey, Element<T>>> asyncBopGet(
          String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset,
          int count, boolean withDelete, boolean dropIfEmpty,
          Transcoder<T> tc) {
    BTreeUtil.validateBkey(from, to);
    BTreeGet get = new BTreeGet(from, to, offset, count, withDelete, dropIfEmpty, eFlagFilter);
    boolean reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;
    return asyncBopExtendedGet(key, get, reverse, tc);
  }

  /**
   * Generic get operation for b+tree items using byte-array type bkeys.
   * Public methods for b+tree items call this method.
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
    final CollectionGetFuture<Map<ByteArrayBKey, Element<T>>> rv
            = new CollectionGetFuture<>(latch, operationTimeout);

    Operation op = opFact.collectionGet(k, collectionGet,
        new CollectionGetOperation.Callback() {
          private final HashMap<ByteArrayBKey, CachedData> cachedDataMap = new HashMap<>();
          private final GetResult<Map<ByteArrayBKey, Element<T>>> result =
                  new BopGetResultImpl<>(cachedDataMap, reverse, tc);

          public void receivedStatus(OperationStatus status) {
            CollectionOperationStatus cstatus;
            if (status instanceof CollectionOperationStatus) {
              cstatus = (CollectionOperationStatus) status;
            } else {
              getLogger().warn("Unhandled state: " + status);
              cstatus = new CollectionOperationStatus(status);
            }
            if (cstatus.isSuccess() || cstatus.getResponse() == CollectionResponse.NOT_FOUND_ELEMENT) {
              rv.setResult(result, cstatus);
              return;
            }

            rv.setResult(null, cstatus);
            getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
          }

          public void complete() {
            latch.countDown();
          }

          public void gotData(String bkey, int flags, byte[] data, byte[] eflag) {
            cachedDataMap.put(new ByteArrayBKey(BTreeUtil.hexStringToByteArrays(bkey)),
                    new CachedData(flags, data, eflag, tc.getMaxSize()));
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
   * Generic get operation for b+tree items using positions.
   * Public methods for b+tree items call this method.
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
    final CollectionGetFuture<Map<Integer, Element<T>>> rv =
            new CollectionGetFuture<>(latch, operationTimeout);

    Operation op = opFact.bopGetByPosition(k, get, new BTreeGetByPositionOperation.Callback() {
      private final HashMap<Integer, Entry<BKeyObject, CachedData>> cachedDataMap =
              new HashMap<>();
      private final GetResult<Map<Integer, Element<T>>> result =
              new BopGetByPositionResultImpl<>(cachedDataMap, reverse, tc);

      public void receivedStatus(OperationStatus status) {
        CollectionOperationStatus cstatus;
        if (status instanceof CollectionOperationStatus) {
          cstatus = (CollectionOperationStatus) status;
        } else {
          getLogger().warn("Unhandled state: " + status);
          cstatus = new CollectionOperationStatus(status);
        }
        if (cstatus.isSuccess() || cstatus.getResponse() == CollectionResponse.NOT_FOUND_ELEMENT) {
          rv.setResult(result, cstatus);
          return;
        }

        rv.setResult(null, cstatus);
        getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
      }

      @Override
      public void complete() {
        latch.countDown();
      }

      @Override
      public void gotData(int pos, int flags, BKeyObject bkeyObject, byte[] eflag, byte[] data) {
        CachedData cachedData = new CachedData(flags, data, eflag, tc.getMaxSize());
        cachedDataMap.put(pos, new AbstractMap.SimpleEntry<>(bkeyObject, cachedData));
      }
    });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public CollectionFuture<Integer> asyncBopFindPosition(String key, long bkey,
                                                        BTreeOrder order) {
    BTreeUtil.validateBkey(bkey);
    if (order == null) {
      throw new IllegalArgumentException("BTreeOrder must not be null.");
    }
    BTreeFindPosition get = new BTreeFindPosition(bkey, order);
    return asyncBopFindPosition(key, get);
  }

  @Override
  public CollectionFuture<Integer> asyncBopFindPosition(String key, byte[] bkey,
                                                        BTreeOrder order) {
    BTreeUtil.validateBkey(bkey);
    if (order == null) {
      throw new IllegalArgumentException("BTreeOrder must not be null.");
    }
    BTreeFindPosition get = new BTreeFindPosition(bkey, order);
    return asyncBopFindPosition(key, get);
  }

  /**
   * Generic find-position operation for b+tree items.
   * Public methods for b+tree items call this method.
   *
   * @param k   b+tree item's key
   * @param get operation parameters (element key and so on)
   * @return future holding the element's position
   */
  private CollectionFuture<Integer> asyncBopFindPosition(final String k,
                                                         final BTreeFindPosition get) {
    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Integer> rv = new CollectionFuture<>(latch, operationTimeout);

    Operation op = opFact.bopFindPosition(k, get, new BTreeFindPositionOperation.Callback() {

      private int position = 0;

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

        rv.set(null, cstatus);
        getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
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
          String key, long bkey, BTreeOrder order, int count) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(bkey, order, count);
    return asyncBopFindPositionWithGet(key, get, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, long bkey, BTreeOrder order, int count, Transcoder<T> tc) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(bkey, order, count);
    return asyncBopFindPositionWithGet(key, get, tc);
  }

  @Override
  public CollectionFuture<Map<Integer, Element<Object>>> asyncBopFindPositionWithGet(
          String key, byte[] bkey, BTreeOrder order, int count) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(bkey, order, count);
    return asyncBopFindPositionWithGet(key, get, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Map<Integer, Element<T>>> asyncBopFindPositionWithGet(
          String key, byte[] bkey, BTreeOrder order, int count, Transcoder<T> tc) {
    BTreeFindPositionWithGet get = new BTreeFindPositionWithGet(bkey, order, count);
    return asyncBopFindPositionWithGet(key, get, tc);
  }


  /**
   * Generic find position with get operation for b+tree items.
   * Public methods for b+tree items call this method.
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
    final CollectionGetFuture<Map<Integer, Element<T>>> rv
            = new CollectionGetFuture<>(latch, operationTimeout);

    Operation op = opFact.bopFindPositionWithGet(k, get,
        new BTreeFindPositionWithGetOperation.Callback() {
          private final HashMap<Integer, Entry<BKeyObject, CachedData>> cachedDataMap
                  = new HashMap<>();
          private final GetResult<Map<Integer, Element<T>>> result
                  = new BopGetByPositionResultImpl<>(cachedDataMap, false, tc);

          public void receivedStatus(OperationStatus status) {
            CollectionOperationStatus cstatus;
            if (status instanceof CollectionOperationStatus) {
              cstatus = (CollectionOperationStatus) status;
            } else {
              getLogger().warn("Unhandled state: " + status);
              cstatus = new CollectionOperationStatus(status);
            }
            if (cstatus.isSuccess()) {
              rv.setResult(result, cstatus);
              return;
            }

            rv.setResult(null, cstatus);
            getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
          }

          public void complete() {
            latch.countDown();
          }

          @Override
          public void gotData(int pos, int flags, BKeyObject bkeyObject, byte[] eflag, byte[] data) {
            CachedData cachedData = new CachedData(flags, data, eflag, tc.getMaxSize());
            cachedDataMap.put(pos, new AbstractMap.SimpleEntry<>(bkeyObject, cachedData));
          }
        }
    );
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeInsertAndGet<Object> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, false, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeInsertAndGet<E> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, false, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeInsertAndGet<Object> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, false, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopInsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeInsertAndGet<E> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, false, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeInsertAndGet<Object> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, true, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, long bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeInsertAndGet<E> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, true, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, transcoder);
  }

  @Override
  public BTreeStoreAndGetFuture<Boolean, Object> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    BTreeInsertAndGet<Object> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, true, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, collectionTranscoder);
  }

  @Override
  public <E> BTreeStoreAndGetFuture<Boolean, E> asyncBopUpsertAndGetTrimmed(
          String key, byte[] bkey, byte[] eFlag, E value,
          CollectionAttributes attributesForCreate, Transcoder<E> transcoder) {
    BTreeInsertAndGet<E> insertAndGet
            = new BTreeInsertAndGet<>(bkey, eFlag, value, true, attributesForCreate);
    return asyncBTreeInsertAndGet(key, insertAndGet, transcoder);
  }

  /**
   * Insert/upsert and get the trimmed element for b+tree items. Public methods call this method.
   *
   * @param k   b+tree item's key
   * @param get operation parameters (element key and so on)
   * @param tc  transcoder to serialize and unserialize value
   * @return future holding the success/failure of the operation and the trimmed element
   */
  private <E> BTreeStoreAndGetFuture<Boolean, E> asyncBTreeInsertAndGet(
          final String k, final BTreeInsertAndGet<E> get, final Transcoder<E> tc) {
    CachedData co = tc.encode(get.getValue());
    get.setFlags(co.getFlags());

    final CountDownLatch latch = new CountDownLatch(1);
    final BTreeStoreAndGetFuture<Boolean, E> rv =
            new BTreeStoreAndGetFuture<>(latch, operationTimeout);

    Operation op = opFact.bopInsertAndGet(k, get, co.getData(),
        new BTreeInsertAndGetOperation.Callback() {

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
              return;
            }

            rv.set(false, cstatus);
            getLogger().debug("Operation failed for key(%s): %s", k, cstatus);
          }

          public void complete() {
            latch.countDown();
          }

          @Override
          public void gotData(int flags, BKeyObject bkeyObject, byte[] eflag, byte[] data) {
            rv.setElement(new BopStoreAndGetResultImpl<>(bkeyObject,
                    new CachedData(flags, data, eflag, tc.getMaxSize()), tc));
          }
        });
    rv.setOperation(op);
    addOp(k, op);
    return rv;
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key,
                                                  byte[] from, byte[] to,
                                                  ElementFlagFilter eFlagFilter, int count,
                                                  boolean dropIfEmpty) {
    BTreeUtil.validateBkey(from, to);
    BTreeDelete delete = new BTreeDelete(from, to, count, false, dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopDelete(String key,
                                                  byte[] bkey, ElementFlagFilter eFlagFilter,
                                                  boolean dropIfEmpty) {
    BTreeUtil.validateBkey(bkey);
    BTreeDelete delete = new BTreeDelete(bkey, false, dropIfEmpty, eFlagFilter);
    return asyncCollectionDelete(key, delete);
  }

  @Override
  public CollectionFuture<Boolean> asyncBopUpsert(String key,
                                                  byte[] bkey, byte[] elementFlag, Object value,
                                                  CollectionAttributes attributesForCreate) {
    BTreeUtil.validateBkey(bkey);
    BTreeUpsert<Object> bTreeUpsert = new BTreeUpsert<>(value, elementFlag, null, attributesForCreate);
    return asyncCollectionInsert(key, BTreeUtil.toHex(bkey), bTreeUpsert, collectionTranscoder);
  }

  @Override
  public <T> CollectionFuture<Boolean> asyncBopUpsert(String key,
                                                      byte[] bkey, byte[] elementFlag, T value,
                                                      CollectionAttributes attributesForCreate,
                                                      Transcoder<T> tc) {
    BTreeUtil.validateBkey(bkey);
    BTreeUpsert<T> bTreeUpsert = new BTreeUpsert<>(value, elementFlag, null, attributesForCreate);
    return asyncCollectionInsert(key, BTreeUtil.toHex(bkey), bTreeUpsert, tc);
  }

  @Override
  public CollectionFuture<Integer> asyncBopGetItemCount(String key,
                                                        byte[] from, byte[] to,
                                                        ElementFlagFilter eFlagFilter) {
    BTreeUtil.validateBkey(from, to);
    CollectionCount collectionCount = new BTreeCount(from, to, eFlagFilter);
    return asyncCollectionCount(key, collectionCount);
  }

  @Override
  public CollectionFuture<Map<Object, Boolean>> asyncSopPipedExistBulk(String key,
                                                                       List<Object> values) {
    SetPipedExist<Object> exist = new SetPipedExist<>(key, values, collectionTranscoder);
    return asyncSetPipedExist(key, exist);
  }

  @Override
  public <T> CollectionFuture<Map<T, Boolean>> asyncSopPipedExistBulk(String key,
                                                                      List<T> values,
                                                                      Transcoder<T> tc) {
    SetPipedExist<T> exist = new SetPipedExist<>(key, values, tc);
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
    if (exist.getItemCount() > CollectionPipedInsert.MAX_PIPED_ITEM_COUNT) {
      throw new IllegalArgumentException(
              "The number of piped operations must not exceed a maximum of "
                      + CollectionPipedInsert.MAX_PIPED_ITEM_COUNT + ".");
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final CollectionFuture<Map<T, Boolean>> rv = new CollectionFuture<>(
            latch, operationTimeout);

    Operation op = opFact.collectionPipedExist(key, exist,
        new CollectionPipedExistOperation.Callback() {

          private final Map<T, Boolean> result = new HashMap<>();
          private boolean hasAnError = false;

          public void receivedStatus(OperationStatus status) {
            if (hasAnError) {
              return;
            }

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

            if (cstatus.isSuccess()) {
              result.put(exist.getValues().get(index),
                      (CollectionResponse.EXIST.equals(cstatus
                              .getResponse())));
              return;
            }

            hasAnError = true;
            rv.set(new HashMap<>(0), cstatus);
            getLogger().debug("Operation failed for key(%s), index(%d): %s", key, index, cstatus);
          }
        });

    rv.setOperation(op);
    addOp(key, op);
    return rv;
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    BTreeUtil.validateBkey(from, to);
    validateKeys(keyList);
    checkDupKey(keyList);
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (offset + count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException(
              "The sum of offset and count must not exceed a maximum of " + MAX_SMGET_COUNT + ".");
    }

    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<>(
            arrangedKey.size());
    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      if (arrangedKey.size() > 1) {
        smGetList.add(new BTreeSMGetWithByteTypeBkeyOld<>(entry.getKey(),
                entry.getValue(), from, to, eFlagFilter, 0, offset + count));
      } else {
        smGetList.add(new BTreeSMGetWithByteTypeBkeyOld<>(entry.getKey(),
                entry.getValue(), from, to, eFlagFilter, offset, count));
      }
    }

    return smget(smGetList, offset, count, (BTreeUtil.compareByteArraysInLexOrder(from, to) > 0),
            collectionTranscoder);
  }

  @Override
  public SMGetFuture<List<SMGetElement<Object>>> asyncBopSortMergeGet(
          List<String> keyList, byte[] from, byte[] to, ElementFlagFilter eFlagFilter,
          int count, SMGetMode smgetMode) {
    BTreeUtil.validateBkey(from, to);
    validateKeys(keyList);
    checkDupKey(keyList);
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (count > MAX_SMGET_COUNT) {
      throw new IllegalArgumentException("The count must not exceed a maximum of "
              + MAX_SMGET_COUNT + ".");
    }

    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, smgetKeyChunkSize);
    List<BTreeSMGet<Object>> smGetList = new ArrayList<>(
            arrangedKey.size());
    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      smGetList.add(new BTreeSMGetWithByteTypeBkey<>(entry.getKey(),
              entry.getValue(), from, to, eFlagFilter, count, smgetMode));
    }

    return smget(smGetList, count, smgetMode == SMGetMode.UNIQUE,
            (BTreeUtil.compareByteArraysInLexOrder(from, to) > 0), collectionTranscoder);
  }

  /**
   * Generic pipelined insert operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param key       collection item's key
   * @param insertList list of operation parameters (element values and so on)
   * @return future holding the map of element index and the result of its insert operation
   */
  <T> CollectionFuture<Map<Integer, CollectionOperationStatus>> asyncCollectionPipedInsert(
          final String key, final List<CollectionPipedInsert<T>> insertList) {

    final CountDownLatch latch = new CountDownLatch(insertList.size());
    final PipedCollectionFuture<Integer, CollectionOperationStatus> rv =
            new PipedCollectionFuture<>(latch, operationTimeout);

    for (int i = 0; i < insertList.size(); i++) {
      final CollectionPipedInsert<T> insert = insertList.get(i);
      final int idx = i;

      Operation op = opFact.collectionPipedInsert(key, insert,
          new CollectionPipedInsertOperation.Callback() {
            // each result status
            public void receivedStatus(OperationStatus status) {
              CollectionOperationStatus cstatus;

              if (status instanceof CollectionOperationStatus) {
                cstatus = (CollectionOperationStatus) status;
              } else {
                getLogger().warn("Unhandled state: " + status);
                cstatus = new CollectionOperationStatus(status);
              }
              rv.setOperationStatus(cstatus);
            }

            // complete
            public void complete() {
              latch.countDown();
            }

            // got status
            public void gotStatus(Integer index, OperationStatus status) {
              if (status instanceof CollectionOperationStatus) {
                rv.addEachResult(index + (idx * CollectionPipedInsert.MAX_PIPED_ITEM_COUNT),
                                (CollectionOperationStatus) status);
              } else {
                rv.addEachResult(index + (idx * CollectionPipedInsert.MAX_PIPED_ITEM_COUNT),
                                new CollectionOperationStatus(status));
              }
            }
          });
      rv.addOperation(op);
      addOp(key, op);
    }
    return rv;
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, Object value,
          CollectionAttributes attributesForCreate) {
    return asyncBopInsertBulk(keyList, bkey, eFlag, value,
            attributesForCreate, collectionTranscoder);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncBopInsertBulk(
          List<String> keyList, long bkey, byte[] eFlag, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    BTreeUtil.validateBkey(bkey);
    validateKeys(keyList);
    checkDupKey(keyList);
    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);

    List<CollectionBulkInsert<T>> insertList = new ArrayList<>(
            arrangedKey.size());

    CachedData insertValue = tc.encode(value);

    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      insertList.add(new CollectionBulkInsert.BTreeBulkInsert<>(
              entry.getKey(), entry.getValue(),
              String.valueOf(bkey), BTreeUtil.toHex(eFlag),
              insertValue, attributesForCreate));
    }

    return asyncCollectionInsertBulk2(insertList);
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

    BTreeUtil.validateBkey(bkey);
    validateKeys(keyList);
    checkDupKey(keyList);
    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);
    List<CollectionBulkInsert<T>> insertList = new ArrayList<>(
            arrangedKey.size());

    CachedData insertValue = tc.encode(value);

    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      insertList.add(new CollectionBulkInsert.BTreeBulkInsert<>(
              entry.getKey(), entry.getValue(),
              BTreeUtil.toHex(bkey), BTreeUtil.toHex(eFlag),
              insertValue, attributesForCreate));
    }

    return asyncCollectionInsertBulk2(insertList);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncMopInsertBulk(keyList, mkey, value, attributesForCreate, collectionTranscoder);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncMopInsertBulk(
          List<String> keyList, String mkey, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {

    validateMKey(mkey);
    validateKeys(keyList);
    checkDupKey(keyList);
    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);

    List<CollectionBulkInsert<T>> insertList = new ArrayList<>(
            arrangedKey.size());

    CachedData insertValue = tc.encode(value);

    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      insertList.add(new CollectionBulkInsert.MapBulkInsert<>(
              entry.getKey(), entry.getValue(),
              mkey, insertValue, attributesForCreate));
    }

    return asyncCollectionInsertBulk2(insertList);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncSopInsertBulk(keyList, value, attributesForCreate, collectionTranscoder);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncSopInsertBulk(
          List<String> keyList, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    validateKeys(keyList);
    checkDupKey(keyList);

    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);
    List<CollectionBulkInsert<T>> insertList = new ArrayList<>(
            arrangedKey.size());

    CachedData insertValue = tc.encode(value);

    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      insertList.add(new CollectionBulkInsert.SetBulkInsert<>(
              entry.getKey(), entry.getValue(),
              insertValue, attributesForCreate));
    }

    return asyncCollectionInsertBulk2(insertList);
  }

  @Override
  public Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, Object value,
          CollectionAttributes attributesForCreate) {

    return asyncLopInsertBulk(keyList, index, value, attributesForCreate, collectionTranscoder);
  }

  @Override
  public <T> Future<Map<String, CollectionOperationStatus>> asyncLopInsertBulk(
          List<String> keyList, int index, T value,
          CollectionAttributes attributesForCreate, Transcoder<T> tc) {
    validateKeys(keyList);
    checkDupKey(keyList);

    Collection<Entry<MemcachedNode, List<String>>> arrangedKey =
            groupingKeys(keyList, NON_PIPED_BULK_INSERT_CHUNK_SIZE);
    List<CollectionBulkInsert<T>> insertList = new ArrayList<>(
            arrangedKey.size());

    CachedData insertValue = tc.encode(value);

    for (Entry<MemcachedNode, List<String>> entry : arrangedKey) {
      insertList.add(new CollectionBulkInsert.ListBulkInsert<>(
              entry.getKey(), entry.getValue(),
              index, insertValue, attributesForCreate));
    }

    return asyncCollectionInsertBulk2(insertList);
  }

  /**
   * Generic bulk insert operation for collection items.
   * Public methods for collection items call this method.
   *
   * @param insertList list of operation parameters (item keys, element values, and so on)
   * @return future holding the map of item key and the result of the insert operation on that key
   */
  private <T> Future<Map<String, CollectionOperationStatus>> asyncCollectionInsertBulk2(
          List<CollectionBulkInsert<T>> insertList) {

    final CountDownLatch latch = new CountDownLatch(insertList.size());

    final BulkOperationFuture<CollectionOperationStatus> rv =
            new BulkOperationFuture<>(latch, operationTimeout);

    for (final CollectionBulkInsert<T> insert : insertList) {
      Operation op = opFact.collectionBulkInsert(
              insert, new CollectionBulkInsertOperation.Callback() {
                public void receivedStatus(OperationStatus status) {
                  // Nothing to do here because the user MUST search the result Map instance.
                }

                public void complete() {
                  latch.countDown();
                }

                public void gotStatus(String key, OperationStatus status) {
                  if (!status.isSuccess()) {
                    if (status instanceof CollectionOperationStatus) {
                      rv.addFailedResult(key, (CollectionOperationStatus) status);
                    } else {
                      rv.addFailedResult(key, new CollectionOperationStatus(status));
                    }
                  }
                }
              });
      rv.addOperation(op);
      addOp(insert.getMemcachedNode(), op);
    }
    return rv;
  }

  public CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    return asyncBopGetBulk(keyList, from, to, eFlagFilter, offset, count, collectionTranscoder);
  }

  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, T>>> asyncBopGetBulk(
          List<String> keyList, long from, long to,
          ElementFlagFilter eFlagFilter, int offset, int count, Transcoder<T> tc) {
    validateKeys(keyList);
    checkDupKey(keyList);
    if (offset < 0) {
      throw new IllegalArgumentException("Offset must be 0 or positive integer.");
    }
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (count > MAX_GETBULK_ELEMENT_COUNT) {
      throw new IllegalArgumentException("Count must not exceed a maximum of "
          + MAX_GETBULK_ELEMENT_COUNT + ".");
    }

    Collection<Entry<MemcachedNode, List<String>>> rearrangedKeys =
            groupingKeys(keyList, BOPGET_BULK_CHUNK_SIZE);

    List<BTreeGetBulk<T>> getBulkList = new ArrayList<>(
            rearrangedKeys.size());

    for (Entry<MemcachedNode, List<String>> entry : rearrangedKeys) {
      getBulkList.add(new BTreeGetBulkWithLongTypeBkey<>(entry.getKey(),
              entry.getValue(), from, to, eFlagFilter, offset, count));
    }

    return btreeGetBulk(getBulkList, (from > to), tc);
  }

  public CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>>
      asyncBopGetBulk(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count) {
    BTreeUtil.validateBkey(from, to);
    return asyncBopGetBulk(keyList, from, to, eFlagFilter, offset, count, collectionTranscoder);
  }

  public <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, T>>> asyncBopGetBulk(
          List<String> keyList, byte[] from, byte[] to,
          ElementFlagFilter eFlagFilter, int offset, int count,
          Transcoder<T> tc) {
    BTreeUtil.validateBkey(from, to);
    validateKeys(keyList);
    checkDupKey(keyList);

    if (offset < 0) {
      throw new IllegalArgumentException("Offset must be 0 or positive integer.");
    }
    if (count < 1) {
      throw new IllegalArgumentException("Count must be larger than 0.");
    }
    if (count > MAX_GETBULK_ELEMENT_COUNT) {
      throw new IllegalArgumentException("Count must not exceed a maximum of "
          + MAX_GETBULK_ELEMENT_COUNT + ".");
    }

    Collection<Entry<MemcachedNode, List<String>>> rearrangedKeys =
            groupingKeys(keyList, BOPGET_BULK_CHUNK_SIZE);

    List<BTreeGetBulk<T>> getBulkList = new ArrayList<>(
            rearrangedKeys.size());

    for (Entry<MemcachedNode, List<String>> entry : rearrangedKeys) {
      getBulkList.add(new BTreeGetBulkWithByteTypeBkey<>(entry.getKey(),
              entry.getValue(), from, to, eFlagFilter, offset, count));
    }

    boolean reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;

    return btreeGetBulkByteArrayBKey(getBulkList, reverse, tc);
  }

  /**
   * Generic bulk get operation for b+tree items. Public methods call this method.
   *
   * @param getBulkList list of operation parameters (item keys, element key range, and so on)
   * @param reverse     forward or backward
   * @param tc          transcoder to serialize and unserialize value
   * @return future holding the map of item key and the fetched elements from that key
   */
  private <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, T>>> btreeGetBulk(
          final List<BTreeGetBulk<T>> getBulkList,
          final boolean reverse, final Transcoder<T> tc) {

    final CountDownLatch latch = new CountDownLatch(getBulkList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<>();
    final Map<String, List<BTreeElement<Long, CachedData>>> cachedDataMap =
            new HashMap<>();
    final Map<String, CollectionOperationStatus> opStatusMap =
            new HashMap<>();
    final GetResult<Map<String, BTreeGetResult<Long, T>>> result =
            new BopGetBulkResultImpl<>(cachedDataMap, opStatusMap, reverse, tc);

    for (BTreeGetBulk<T> getBulk : getBulkList) {
      final Operation op = opFact.bopGetBulk(getBulk, new BTreeGetBulkOperation.Callback() {

        @Override
        public void receivedStatus(OperationStatus status) {
          // Nothing to do here because the user MUST search the result Map instance.
        }

        @Override
        public void complete() {
          latch.countDown();
        }

        @Override
        public void gotKey(String key, int elementCount, OperationStatus status) {
          if (elementCount > 0) {
            cachedDataMap.put(key, new ArrayList<>(elementCount));
          }
          opStatusMap.put(key, (CollectionOperationStatus) status);
        }

        @Override
        public void gotElement(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
          List<BTreeElement<Long, CachedData>> elems = cachedDataMap.get(key);
          assert elems != null : "Element list not prepared in bopGetBulk";
          elems.add(new BTreeElement<>((Long) bkey, eflag,
                  new CachedData(flags, data, tc.getMaxSize())));
        }
      });
      ops.add(op);
      addOp(getBulk.getMemcachedNode(), op);
    }

    return new CollectionGetBulkFuture<>(
            latch, ops, result, operationTimeout);
  }

  /**
   * Generic bulk get operation for b+tree items using byte-array type bkeys.
   * Public methods call this method.
   *
   * @param getBulkList list of operation parameters (item keys, element key range, and so on)
   * @param reverse     forward or backward
   * @param tc          transcoder to serialize and unserialize value
   * @return future holding the map of item key and the fetched elements from that key
   */
  private <T> CollectionGetBulkFuture<Map<String, BTreeGetResult<ByteArrayBKey, T>>>
      btreeGetBulkByteArrayBKey(
          final List<BTreeGetBulk<T>> getBulkList,
          final boolean reverse, final Transcoder<T> tc) {

    final CountDownLatch latch = new CountDownLatch(getBulkList.size());
    final ConcurrentLinkedQueue<Operation> ops = new ConcurrentLinkedQueue<>();
    final Map<String, List<BTreeElement<ByteArrayBKey, CachedData>>> cachedDataMap =
            new HashMap<>();
    final Map<String, CollectionOperationStatus> opStatusMap =
            new HashMap<>();
    final GetResult<Map<String, BTreeGetResult<ByteArrayBKey, T>>> result =
            new BopGetBulkResultImpl<>(cachedDataMap, opStatusMap, reverse, tc);

    for (BTreeGetBulk<T> getBulk : getBulkList) {
      Operation op = opFact.bopGetBulk(getBulk, new BTreeGetBulkOperation.Callback() {
        @Override
        public void receivedStatus(OperationStatus status) {
        }

        @Override
        public void complete() {
          latch.countDown();
        }

        @Override
        public void gotKey(String key, int elementCount, OperationStatus status) {
          if (elementCount > 0) {
            cachedDataMap.put(key, new ArrayList<>(elementCount));
          }
          opStatusMap.put(key, (CollectionOperationStatus) status);
        }

        @Override
        public void gotElement(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
          List<BTreeElement<ByteArrayBKey, CachedData>> elems = cachedDataMap.get(key);
          assert elems != null : "Element list not prepared in bopGetBulk";
          elems.add(new BTreeElement<>(
                  new ByteArrayBKey((byte[]) bkey), eflag,
                  new CachedData(flags, data, tc.getMaxSize())));
        }
      });
      ops.add(op);
      addOp(getBulk.getMemcachedNode(), op);
    }

    return new CollectionGetBulkFuture<>(
            latch, ops, result, operationTimeout);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, long bkey,
                                             int by) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by);
    return asyncCollectionMutate(key, String.valueOf(bkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] bkey,
                                             int by) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by);
    return asyncCollectionMutate(key, BTreeUtil.toHex(bkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, long bkey,
                                             int by, long initial, byte[] eFlag) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by, initial, eFlag);
    return asyncCollectionMutate(key, String.valueOf(bkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopIncr(String key, byte[] bkey,
                                             int by, long initial, byte[] eFlag) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.incr, by, initial, eFlag);
    return asyncCollectionMutate(key, BTreeUtil.toHex(bkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, long bkey,
                                             int by) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by);
    return asyncCollectionMutate(key, String.valueOf(bkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] bkey,
                                             int by) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by);
    return asyncCollectionMutate(key, BTreeUtil.toHex(bkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, long bkey,
                                             int by, long initial, byte[] eFlag) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by, initial, eFlag);
    return asyncCollectionMutate(key, String.valueOf(bkey), collectionMutate);
  }

  @Override
  public CollectionFuture<Long> asyncBopDecr(String key, byte[] bkey,
                                             int by, long initial, byte[] eFlag) {
    BTreeUtil.validateBkey(bkey);
    CollectionMutate collectionMutate = new BTreeMutate(Mutator.decr, by, initial, eFlag);
    return asyncCollectionMutate(key, BTreeUtil.toHex(bkey), collectionMutate);
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

    final CollectionFuture<Long> rv = new CollectionFuture<>(latch, operationTimeout);

    Operation op = opFact.collectionMutate(k, subkey, collectionMutate,
        new OperationCallback() {

          @Override
          public void receivedStatus(OperationStatus status) {
            if (status.isSuccess()) {
              try {
                rv.set(Long.valueOf(status.getMessage()),
                        new CollectionOperationStatus(new OperationStatus(true, "END")));
              } catch (NumberFormatException e) {
                rv.set(null, new CollectionOperationStatus(
                        new OperationStatus(false, status.getMessage())));

                getLogger().debug("Key(%s), Bkey(%s) Unknown response : %s", k, subkey, status);
              }
              return;
            }

            rv.set(null, new CollectionOperationStatus(status));

            getLogger().debug("Key(%s), Bkey(%s) Unknown response : %s", k, subkey, status);
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

  private void validateKeys(Collection<String> keyList) {
    if (keyList == null) {
      throw new IllegalArgumentException("Key list is null.");
    } else if (keyList.isEmpty()) {
      throw new IllegalArgumentException("Key list is empty.");
    }

    for (String key : keyList) {
      validateKey(key);
    }
  }

  private void checkDupKey(Collection<String> keyList) {
    /*
     * Dup Check -> insure elements sequentially added to keyList
     * */
    HashSet<String> keySet = new HashSet<>();
    for (String key : keyList) {
      if (!keySet.add(key)) {
        throw new IllegalArgumentException("Duplicate keys exist in key list.");
      }
    }
  }

  /**
   * Get the client version.
   *
   * @return version string
   */
  public static String getVersion() {
    if (VERSION != null) {
      return VERSION;
    }
    synchronized (VERSION_LOCK) {
      if (VERSION == null) {
        Enumeration<URL> resEnum;
        try {
          resEnum = Thread.currentThread()
                  .getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
          while (resEnum.hasMoreElements()) {
            URL url = resEnum.nextElement();
            InputStream is = url.openStream();
            if (is != null) {
              Manifest manifest = new Manifest(is);
              java.util.jar.Attributes mainAttribs = manifest.getMainAttributes();
              String version = mainAttribs.getValue("Arcusclient-Version");
              if (version != null) {
                VERSION = version;
                break;
              }
            }
          }
        } catch (Exception e) {
          // Failed to get version.
        } finally {
          if (VERSION == null) {
            VERSION = "NONE";
          }
        }
      }
    }
    return VERSION;
  }
}
