/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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
package net.spy.memcached.v2;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.CachedData;
import net.spy.memcached.KeyValidator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.BTreeCreate;
import net.spy.memcached.collection.BTreeGet;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeGetBulkWithLongTypeBkey;
import net.spy.memcached.collection.BTreeGetBulkWithByteTypeBkey;
import net.spy.memcached.collection.BTreeUpsert;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkey;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.collection.BTreeInsert;
import net.spy.memcached.collection.BTreeInsertAndGet;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionCreate;
import net.spy.memcached.collection.CollectionInsert;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeInsertAndGetOperation;
import net.spy.memcached.ops.CollectionCreateOperation;
import net.spy.memcached.ops.CollectionGetOperation;
import net.spy.memcached.ops.CollectionInsertOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.transcoders.TranscoderUtils;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;
import net.spy.memcached.v2.vo.BTreeElements;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.SMGetElements;

public class AsyncArcusCommands<T> implements AsyncArcusCommandsIF<T> {

  private final Transcoder<T> tc;
  private final Transcoder<T> tcForCollection;
  private final KeyValidator keyValidator;
  private final Supplier<ArcusClient> arcusClientSupplier;

  @SuppressWarnings("unchecked")
  public AsyncArcusCommands(Supplier<ArcusClient> arcusClientSupplier) {
    this.tc = (Transcoder<T>) arcusClientSupplier.get().getTranscoder();
    this.tcForCollection = (Transcoder<T>) arcusClientSupplier.get().getCollectionTranscoder();
    this.keyValidator = arcusClientSupplier.get().getKeyValidator();
    this.arcusClientSupplier = arcusClientSupplier;
  }

  public ArcusFuture<Boolean> set(String key, int exp, T value) {
    return store(StoreType.set, key, exp, value);
  }

  public ArcusFuture<Boolean> add(String key, int exp, T value) {
    return store(StoreType.add, key, exp, value);
  }

  public ArcusFuture<Boolean> replace(String key, int exp, T value) {
    return store(StoreType.replace, key, exp, value);
  }

  private ArcusFuture<Boolean> store(StoreType type, String key, int exp, T value) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);
    CachedData co = tc.encode(value);
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        StatusCode code = status.getStatusCode();
        switch (code) {
          case SUCCESS:
            result.set(true);
            break;
          case ERR_NOT_STORED:
            result.set(false);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            // TYPE_MISMATCH or unknown statement
            result.addError(key, status);
            break;
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact()
        .store(type, key, co.getFlags(), exp, co.getData(), cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Map<String, Boolean>> multiSet(List<String> keys, int exp, T value) {
    return multiStore(StoreType.set, keys, exp, value);
  }

  public ArcusFuture<Map<String, Boolean>> multiAdd(List<String> keys, int exp, T value) {
    return multiStore(StoreType.add, keys, exp, value);
  }

  public ArcusFuture<Map<String, Boolean>> multiReplace(List<String> keys, int exp, T value) {
    return multiStore(StoreType.replace, keys, exp, value);
  }

  /**
   * @param type  store type
   * @param keys  key list to store
   * @param exp   expiration time
   * @param value value to store for whole keys
   * @return ArcusFuture with Map of key to Boolean result. If an operation fails exceptionally,
   * the corresponding value in the map will be null.
   */
  private ArcusFuture<Map<String, Boolean>> multiStore(StoreType type,
                                                       List<String> keys, int exp, T value) {
    Map<String, CompletableFuture<?>> keyToFuture = new HashMap<>(keys.size());

    for (String key : keys) {
      CompletableFuture<Boolean> future = store(type, key, exp, value).toCompletableFuture();
      keyToFuture.put(key, future);
    }

    return new ArcusMultiFuture<>(keyToFuture.values(), () -> {
      Map<String, Boolean> results = new HashMap<>();
      for (Map.Entry<String, CompletableFuture<?>> entry : keyToFuture.entrySet()) {
        if (entry.getValue().isCompletedExceptionally()) {
          results.put(entry.getKey(), null);
        } else {
          results.put(entry.getKey(), (Boolean) entry.getValue().join());
        }
      }
      return results;
    });
  }

  public ArcusFuture<T> get(String key) {
    AbstractArcusResult<CachedData> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<T> future = new ArcusFutureImpl<>(result,
        r -> r == null ? null : tc.decode((CachedData) r));
    ArcusClient client = arcusClientSupplier.get();

    GetOperation.Callback cb = new GetOperation.Callback() {
      @Override
      public void gotData(String key, int flags, byte[] data) {
        result.set(new CachedData(flags, data, tc.getMaxSize()));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        /*
         * For propagating internal cancel to the future.
         */
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          future.internalCancel();
        } else if (!status.isSuccess()) {
          // unknown statement
          result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().get(key, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Map<String, T>> multiGet(List<String> keys) {
    ArcusClient client = arcusClientSupplier.get();
    Collection<Map.Entry<MemcachedNode, List<String>>> arrangedKeys
        = client.groupingKeys(keys, MemcachedClient.GET_BULK_CHUNK_SIZE, APIType.GET);

    Collection<CompletableFuture<?>> futures = new ArrayList<>();
    Map<CompletableFuture<Map<String, T>>, List<String>> futureToKeys = new HashMap<>();

    for (Map.Entry<MemcachedNode, List<String>> entry : arrangedKeys) {
      MemcachedNode node = entry.getKey();
      List<String> keyList = entry.getValue();
      CompletableFuture<Map<String, T>> future = get(client, node, keyList).toCompletableFuture();
      futureToKeys.put(future, keyList);
      futures.add(future);
    }

    /*
     * Combine all futures. If any future fails exceptionally,
     * the corresponding keys will have null values in the result map.
     * If cache miss occurs, the corresponding key will not be present in the result map.
     */
    return new ArcusMultiFuture<>(futures, () -> {
      Map<String, T> results = new HashMap<>();
      for (Map.Entry<CompletableFuture<Map<String, T>>, List<String>> entry
          : futureToKeys.entrySet()) {
        if (entry.getKey().isCompletedExceptionally()) {
          for (String s : entry.getValue()) {
            results.put(s, null);
          }
        } else {
          Map<String, T> result = entry.getKey().join();
          if (result != null) {
            results.putAll(result);
          }
        }
      }
      return results;
    });
  }

  /**
   * Use only in multiGet method.
   *
   * @param keyList key list to get from single node
   * @return ArcusFuture with results
   */
  private ArcusFuture<Map<String, T>> get(ArcusClient client, MemcachedNode node,
                                          List<String> keyList) {
    AbstractArcusResult<Map<String, CachedData>> result
        = new AbstractArcusResult<>((new AtomicReference<>(new HashMap<>())));
    @SuppressWarnings("unchecked")
    ArcusFutureImpl<Map<String, T>> future = new ArcusFutureImpl<>(result,
        r -> {
          Map<String, T> decodedMap = new HashMap<>();
          for (Map.Entry<String, CachedData> entry
              : ((Map<String, CachedData>) r).entrySet()) {
            decodedMap.put(entry.getKey(), tc.decode(entry.getValue()));
          }
          return decodedMap;
        });

    GetOperation.Callback cb = new GetOperation.Callback() {
      @Override
      public void gotData(String key, int flags, byte[] data) {
        Map<String, CachedData> map = result.get();
        map.put(key, new CachedData(flags, data, tc.getMaxSize()));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        /*
         * For propagating internal cancel to the future.
         */
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          future.internalCancel();
        } else if (!status.isSuccess()) {
          // unknown statement
          for (String key : keyList) {
            result.addError(key, status);
          }
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().get(keyList, cb, node.enabledMGetOp());
    future.setOp(op);
    client.addOp(node, op);

    return future;
  }

  public ArcusFuture<Boolean> flush(int delay) {
    ArcusClient client = arcusClientSupplier.get();
    Collection<MemcachedNode> nodes = client.getFlushNodes();

    Collection<CompletableFuture<?>> futures = new ArrayList<>();

    for (MemcachedNode node : nodes) {
      CompletableFuture<Boolean> future = flush(client, node, delay).toCompletableFuture();
      futures.add(future);
    }

    /*
     * Combine all futures. Returns true if all flush operations succeed.
     * Returns false if any flush operation fails.
     */
    return new ArcusMultiFuture<>(futures, () -> {
      for (CompletableFuture<?> future : futures) {
        if (future.isCompletedExceptionally()) {
          return false;
        }
        Boolean result = (Boolean) future.join();
        if (result == null || !result) {
          return false;
        }
      }
      return true;
    });
  }

  /**
   * Use only in flush method.
   *
   * @param client the ArcusClient instance to use
   * @param node   the MemcachedNode to flush
   * @param delay  flush delay
   * @return ArcusFuture with flush result
   */
  private ArcusFuture<Boolean> flush(ArcusClient client, MemcachedNode node, int delay) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          future.internalCancel();
          return;
        }
        result.set(status.isSuccess());
      }

      @Override
      public void complete() {
        future.complete();
      }
    };

    Operation op = client.getOpFact().flush(delay, cb);
    future.setOp(op);
    client.addOp(node, op);

    return future;
  }

  public ArcusFuture<Boolean> bopCreate(String key, ElementValueType type,
                                        CollectionAttributes attributes) {
    if (attributes == null) {
      throw new IllegalArgumentException("CollectionAttributes cannot be null");
    }

    CollectionCreate create = new BTreeCreate(TranscoderUtils.examineFlags(type),
        attributes.getExpireTime(), attributes.getMaxCount(),
        attributes.getOverflowAction(), attributes.getReadable(), false);

    return collectionCreate(key, create);
  }

  private ArcusFuture<Boolean> collectionCreate(String key, CollectionCreate collectionCreate) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            result.set(true);
            break;
          case ERR_EXISTS:
            result.set(false);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            // NOT_SUPPORTED or unknown statement
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    CollectionCreateOperation op = client.getOpFact()
        .collectionCreate(key, collectionCreate, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Boolean> bopInsert(String key, BTreeElement<T> element,
                                        CollectionAttributes attributes) {
    BTreeInsert<T> insert = new BTreeInsert<>(element.getValue(), element.getEFlag(),
        null, attributes);
    return collectionInsert(key, element.getBkey().toString(), insert);
  }

  public ArcusFuture<Boolean> bopInsert(String key, BTreeElement<T> element) {
    return bopInsert(key, element, null);
  }

  @Override
  public ArcusFuture<Boolean> bopUpsert(String key, BTreeElement<T> element,
                                        CollectionAttributes attributes) {
    BTreeUpsert<T> upsert = new BTreeUpsert<>(element.getValue(), element.getEFlag(),
        null, attributes);
    return collectionInsert(key, element.getBkey().toString(), upsert);
  }

  @Override
  public ArcusFuture<Boolean> bopUpsert(String key, BTreeElement<T> element) {
    return bopUpsert(key, element, null);
  }

  private ArcusFuture<Boolean> collectionInsert(String key,
                                                String internalKey,
                                                CollectionInsert<T> collectionInsert) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);
    CachedData co = tcForCollection.encode(collectionInsert.getValue());
    collectionInsert.setFlags(co.getFlags());
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_ELEMENT_EXISTS:
            case ERR_NOT_FOUND:
              break;
            case CANCELLED:
              future.internalCancel();
              return;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OVERFLOWED / OUT_OF_RANGE / NOT_SUPPORTED
               * or unknown statement
               */
              result.addError(key, status);
              return;
          }
        }
        result.set(status.isSuccess());
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    CollectionInsertOperation op = client.getOpFact()
        .collectionInsert(key, internalKey, collectionInsert, co.getData(), cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopInsertAndGetTrimmed(
      String key, BTreeElement<T> element, CollectionAttributes attributes) {
    return bopInsertOrUpsertAndGetTrimmed(key, element, false, attributes);
  }

  public ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopInsertAndGetTrimmed(
      String key, BTreeElement<T> element) {
    return bopInsertOrUpsertAndGetTrimmed(key, element, false, null);
  }

  public ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopUpsertAndGetTrimmed(
      String key, BTreeElement<T> element, CollectionAttributes attributes) {
    return bopInsertOrUpsertAndGetTrimmed(key, element, true, attributes);
  }

  public ArcusFuture<Map.Entry<Boolean, BTreeElement<T>>> bopUpsertAndGetTrimmed(
      String key, BTreeElement<T> element) {
    return bopInsertOrUpsertAndGetTrimmed(key, element, true, null);
  }

  private ArcusFutureImpl<Map.Entry<Boolean, BTreeElement<T>>> bopInsertOrUpsertAndGetTrimmed(
      String key, BTreeElement<T> element, boolean isUpsert, CollectionAttributes attributes) {
    AbstractArcusResult<Map.Entry<Boolean, BTreeElement<T>>> result =
        new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Map.Entry<Boolean, BTreeElement<T>>> future = new ArcusFutureImpl<>(result);
    BTreeInsertAndGet<T> insertAndGet = createBTreeInsertAndGet(element, isUpsert, attributes);
    CachedData co = tcForCollection.encode(insertAndGet.getValue());
    insertAndGet.setFlags(co.getFlags());
    ArcusClient client = arcusClientSupplier.get();

    BTreeInsertAndGetOperation.Callback cb = new BTreeInsertAndGetOperation.Callback() {
      private BTreeElement<T> trimmedElement = null;

      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_ELEMENT_EXISTS:
            case ERR_NOT_FOUND:
              break;
            case CANCELLED:
              future.internalCancel();
              return;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OVERFLOWED / OUT_OF_RANGE / NOT_SUPPORTED
               * or unknown statement
               */
              result.addError(key, status);
              return;
          }
        }
        result.set(new AbstractMap.SimpleEntry<>(status.isSuccess(), trimmedElement));
      }

      public void complete() {
        future.complete();
      }

      @Override
      public void gotData(int flags, BKeyObject bKeyObject, byte[] eFlag, byte[] data) {
        trimmedElement = new BTreeElement<>(BKey.of(bKeyObject),
            tcForCollection.decode(new CachedData(flags, data, tc.getMaxSize())), eFlag);
      }
    };
    Operation op = client.getOpFact()
        .bopInsertAndGet(key, insertAndGet, co.getData(), cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  private static <T> BTreeInsertAndGet<T> createBTreeInsertAndGet(BTreeElement<T> element,
                                                                  boolean isUpsert,
                                                                  CollectionAttributes attributes) {
    BTreeInsertAndGet<T> insertAndGet;
    if (element.getBkey().getType() == BKey.BKeyType.LONG) {
      insertAndGet = new BTreeInsertAndGet<>((Long) element.getBkey().getData(),
          element.getEFlag(), element.getValue(), isUpsert, attributes);
    } else {
      insertAndGet = new BTreeInsertAndGet<>((byte[]) element.getBkey().getData(),
          element.getEFlag(), element.getValue(), isUpsert, attributes);
    }
    return insertAndGet;
  }

  public ArcusFuture<BTreeElement<T>> bopGet(String key, BKey bKey, BopGetArgs args) {
    AbstractArcusResult<BTreeElement<T>> result =
        new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<BTreeElement<T>> future = new ArcusFutureImpl<>(result);
    BTreeGet get = createBTreeGet(bKey, args);
    ArcusClient client = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_NOT_FOUND:
              result.set(null);
              break;
            case ERR_NOT_FOUND_ELEMENT:
              result.set(new BTreeElement<>(bKey, null, null));
              break;
            case CANCELLED:
              future.internalCancel();
              break;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / UNREADABLE / NOT_SUPPORTED
               * or unknown statement
               */
              result.addError(key, status);
          }
        }
      }

      public void complete() {
        future.complete();
      }

      public void gotData(String bKey, int flags, byte[] data, byte[] eflag) {
        result.set(new BTreeElement<>(BKey.of(bKey),
            tcForCollection.decode(new CachedData(flags, data, tc.getMaxSize())), eflag));
      }
    };
    Operation op = client.getOpFact().collectionGet(key, get, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  private static BTreeGet createBTreeGet(BKey bKey, BopGetArgs args) {
    BTreeGet get;
    if (bKey.getType() == BKey.BKeyType.LONG) {
      get = new BTreeGet((long) bKey.getData(), args.getElementFlagFilter(),
          args.isWithDelete(), args.isDropIfEmpty());
    } else {
      get = new BTreeGet((byte[]) bKey.getData(), args.getElementFlagFilter(),
          args.isWithDelete(), args.isDropIfEmpty());
    }
    return get;
  }

  public ArcusFuture<BTreeElements<T>> bopGet(String key, BKey from, BKey to, BopGetArgs args) {
    verifyBKeyRange(from, to);

    AbstractArcusResult<BTreeElements<T>> result =
        new AbstractArcusResult<>(new AtomicReference<>(new BTreeElements<>(new ArrayList<>())));
    ArcusFutureImpl<BTreeElements<T>> future = new ArcusFutureImpl<>(result);
    BTreeGet get = createBTreeGet(from, to, args);
    ArcusClient client = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      public void receivedStatus(OperationStatus status) {
        if (status.getStatusCode() == StatusCode.TRIMMED) {
          result.get().trimmed();
        } else if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_NOT_FOUND:
              result.set(null);
              break;
            case ERR_NOT_FOUND_ELEMENT:
              break;
            case CANCELLED:
              future.internalCancel();
              break;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / UNREADABLE / NOT_SUPPORTED
               * or unknown statement
               */
              result.addError(key, status);
          }
        }
      }

      public void complete() {
        future.complete();
      }

      public void gotData(String bKey, int flags, byte[] data, byte[] eflag) {
        result.get().addElement(new BTreeElement<>(BKey.of(bKey), tcForCollection.decode(
            new CachedData(flags, data, tc.getMaxSize())), eflag));
      }
    };
    Operation op = client.getOpFact().collectionGet(key, get, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  private static BTreeGet createBTreeGet(BKey from, BKey to, BopGetArgs args) {
    BTreeGet get;
    if (from.getType() == BKey.BKeyType.LONG) {
      get = new BTreeGet((Long) from.getData(), (Long) to.getData(),
          args.getElementFlagFilter(), args.getOffset(), args.getCount(),
          args.isWithDelete(), args.isDropIfEmpty());
    } else {
      get = new BTreeGet((byte[]) from.getData(), (byte[]) to.getData(),
          args.getElementFlagFilter(), args.getOffset(), args.getCount(),
          args.isWithDelete(), args.isDropIfEmpty());
    }
    return get;
  }

  public ArcusFuture<Map<String, BTreeElements<T>>> bopMultiGet(List<String> keys,
                                                                BKey from, BKey to,
                                                                BopGetArgs args) {
    verifyBKeyRange(from, to);
    verifyPositiveCountArg(args, ArcusClient.MAX_GETBULK_ELEMENT_COUNT);

    ArcusClient client = arcusClientSupplier.get();
    keyValidator.validateKey(keys);
    keyValidator.checkDupKey(keys);
    Collection<Map.Entry<MemcachedNode, List<String>>> arrangedKeys =
        client.groupingKeys(keys, ArcusClient.BOPGET_BULK_CHUNK_SIZE, APIType.BOP_GET);

    Collection<CompletableFuture<?>> futures = new ArrayList<>();
    Map<CompletableFuture<Map<String, BTreeElements<T>>>, List<String>> futureToKeys =
        new HashMap<>();

    for (Map.Entry<MemcachedNode, List<String>> entry : arrangedKeys) {
      BTreeGetBulk<T> getBulk =
              createBTreeGetBulk(entry.getKey(), entry.getValue(), from, to, args);
      CompletableFuture<Map<String, BTreeElements<T>>> future =
          bopMultiGetPerNode(client, getBulk).toCompletableFuture();
      futureToKeys.put(future, entry.getValue());
      futures.add(future);
    }

    /*
     * Combine all futures. If any future fails exceptionally,
     * the corresponding keys will have null values in the result map.
     * If key not found, the corresponding key will not be present in the result map.
     */
    return new ArcusMultiFuture<>(futures, () -> {
      Map<String, BTreeElements<T>> results = new HashMap<>();
      for (Map.Entry<CompletableFuture<Map<String, BTreeElements<T>>>, List<String>> entry
          : futureToKeys.entrySet()) {
        if (entry.getKey().isCompletedExceptionally()) {
          for (String key : entry.getValue()) {
            results.put(key, null);
          }
        } else {
          Map<String, BTreeElements<T>> result = entry.getKey().join();
          if (result != null) {
            results.putAll(result);
          }
        }
      }
      return results;
    });
  }

  private void verifyPositiveCountArg(BopGetArgs args, int maxCount) {
    int count = args.getCount();
    if (count <= 0 || count > maxCount) {
      throw new IllegalArgumentException("Count should be between 1 to " + maxCount);
    }
  }

  /**
   * Use only in bopMultiGet method.
   *
   * @param getBulk get bulk parameters for single node
   * @return ArcusFuture with results
   */
  private ArcusFuture<Map<String, BTreeElements<T>>> bopMultiGetPerNode(ArcusClient client,
                                                                        BTreeGetBulk<T> getBulk) {
    AbstractArcusResult<Map<String, BTreeElements<T>>> result =
        new AbstractArcusResult<>(new AtomicReference<>(new HashMap<>()));
    ArcusFutureImpl<Map<String, BTreeElements<T>>> future = new ArcusFutureImpl<>(result);

    BTreeGetBulkOperation.Callback cb = new BTreeGetBulkOperation.Callback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          future.internalCancel();
        } else if (!status.isSuccess()) {
          /*
           * NOT_SUPPORTED or unknown statement
           */
          for (String key : getBulk.getKeyList()) {
            result.addError(key, status);
          }
        }
      }

      @Override
      public void complete() {
        future.complete();
      }

      @Override
      public void gotKey(String key, int elementCount, OperationStatus status) {
        if (status.isSuccess()) {
          BTreeElements<T> elements = new BTreeElements<>(new ArrayList<>());
          result.get().put(key, elements);
          if (status.getStatusCode() == StatusCode.TRIMMED) {
            elements.trimmed();
          }
          return;
        }
        switch (status.getStatusCode()) {
          case ERR_NOT_FOUND:
            break;
          case ERR_NOT_FOUND_ELEMENT:
            // Put empty BTreeElements for the BTree item key
            result.get().put(key, new BTreeElements<>(new ArrayList<>()));
            break;
          default:
            /*
             * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / UNREADABLE
             * or unknown statement
             */
            result.addError(key, status);
        }
      }

      @Override
      public void gotElement(String key, int flags, Object bKey, byte[] eFlag, byte[] data) {
        BTreeElements<T> elements = result.get().get(key);
        elements.addElement(new BTreeElement<>(BKey.of(bKey), tcForCollection.decode(
            new CachedData(flags, data, tc.getMaxSize())), eFlag));
      }
    };
    Operation op = client.getOpFact().bopGetBulk(getBulk, cb);
    future.setOp(op);
    client.addOp(getBulk.getMemcachedNode(), op);

    return future;
  }

  private static void verifyBKeyRange(BKey from, BKey to) {
    if (from.getType() != to.getType()) {
      throw new IllegalArgumentException("Two BKey types(from, to) must be the same.");
    }
  }

  private BTreeGetBulk<T> createBTreeGetBulk(MemcachedNode node, List<String> keys,
                                             BKey from, BKey to, BopGetArgs args) {
    if (from.getType() == BKey.BKeyType.LONG) {
      return new BTreeGetBulkWithLongTypeBkey<>(node, keys,
          (Long) from.getData(), (Long) to.getData(), args.getElementFlagFilter(),
          args.getOffset(), args.getCount());
    } else {
      return new BTreeGetBulkWithByteTypeBkey<>(node, keys,
          (byte[]) from.getData(), (byte[]) to.getData(), args.getElementFlagFilter(),
          args.getOffset(), args.getCount());
    }
  }

  public ArcusFuture<SMGetElements<T>> bopSortMergeGet(List<String> keys, BKey from, BKey to,
                                                       boolean unique, BopGetArgs args) {
    verifyBKeyRange(from, to);
    verifyPositiveCountArg(args, ArcusClient.MAX_SMGET_COUNT);

    ArcusClient client = arcusClientSupplier.get();
    keyValidator.validateKey(keys);
    keyValidator.checkDupKey(keys);

    Collection<Map.Entry<MemcachedNode, List<String>>> arrangedKeys =
        client.groupingKeys(keys, ArcusClient.SMGET_CHUNK_SIZE, APIType.BOP_SMGET);

    List<CompletableFuture<SMGetElements<T>>> smGetFutures = new ArrayList<>();

    for (Map.Entry<MemcachedNode, List<String>> entry : arrangedKeys) {
      BTreeSMGet<T> smGet = createBTreeSMGet(from, to, args, unique, entry);
      CompletableFuture<SMGetElements<T>> future =
          bopSortMergeGetPerNode(client, smGet).toCompletableFuture();
      smGetFutures.add(future);
    }

    /*
     * Combine all futures and merge results from multiple nodes.
     */
    @SuppressWarnings("unchecked")
    Collection<CompletableFuture<?>> futures =
        (Collection<CompletableFuture<?>>) (Collection<?>) smGetFutures;
    return new ArcusMultiFuture<>(futures, () -> {
      List<SMGetElements<T>> results = new ArrayList<>();
      for (CompletableFuture<SMGetElements<T>> future : smGetFutures) {
        if (!future.isCompletedExceptionally()) {
          results.add(future.join());
        }
      }
      return SMGetElements.mergeSMGetElements(results, from.compareTo(to) <= 0, unique,
          args.getCount());
    });
  }

  /**
   * Use only in bopSortMergeGet method.
   *
   * @param smGet sort-merge get parameters for single node
   * @return ArcusFuture with results
   */
  private ArcusFuture<SMGetElements<T>> bopSortMergeGetPerNode(ArcusClient client,
                                                               BTreeSMGet<T> smGet) {
    List<SMGetElements.Element<T>> elementList = new ArrayList<>();
    List<SMGetElements.MissedKey> missedKeys = new ArrayList<>();
    List<SMGetElements.TrimmedKey> trimmedKeys = new ArrayList<>();
    SMGetElements<T> smGetElements = new SMGetElements<>(elementList, missedKeys, trimmedKeys);

    AtomicReference<SMGetElements<T>> atomicReference = new AtomicReference<>(smGetElements);
    AbstractArcusResult<SMGetElements<T>> result =
        new AbstractArcusResult<>(atomicReference);

    ArcusFutureImpl<SMGetElements<T>> future = new ArcusFutureImpl<>(result);

    BTreeSortMergeGetOperation.Callback cb = new BTreeSortMergeGetOperation.Callback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          future.internalCancel();
        } else if (!status.isSuccess()) {
          /*
           * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / NOT_SUPPORTED or unknown statement
           */
          for (String key : smGet.getKeyList()) {
            result.addError(key, status);
          }
        }
      }

      @Override
      public void complete() {
        future.complete();
      }

      @Override
      public void gotData(String key, int flags, Object bKey, byte[] eFlag, byte[] data) {
        BTreeElement<T> btreeElement = new BTreeElement<>(BKey.of(bKey),
            tcForCollection.decode(new CachedData(flags, data, tc.getMaxSize())), eFlag);
        elementList.add(new SMGetElements.Element<>(key, btreeElement));
      }

      @Override
      public void gotMissedKey(String key, OperationStatus cause) {
        missedKeys.add(new SMGetElements.MissedKey(key, cause.getStatusCode()));
      }

      @Override
      public void gotTrimmedKey(String key, Object bKey) {
        trimmedKeys.add(new SMGetElements.TrimmedKey(key, BKey.of(bKey)));
      }
    };
    Operation op = client.getOpFact().bopsmget(smGet, cb);
    future.setOp(op);
    client.addOp(smGet.getMemcachedNode(), op);

    return future;
  }

  private BTreeSMGet<T> createBTreeSMGet(BKey from, BKey to, BopGetArgs args,
                                         boolean unique,
                                         Map.Entry<MemcachedNode, List<String>> entry) {

    if (from.getType() == BKey.BKeyType.LONG) {
      return new BTreeSMGetWithLongTypeBkey<>(entry.getKey(), entry.getValue(),
          (Long) from.getData(), (Long) to.getData(), args.getElementFlagFilter(),
          args.getCount(), unique);
    } else {
      return new BTreeSMGetWithByteTypeBkey<>(entry.getKey(), entry.getValue(),
          (byte[]) from.getData(), (byte[]) to.getData(), args.getElementFlagFilter(),
          args.getCount(), unique);
    }
  }
}
