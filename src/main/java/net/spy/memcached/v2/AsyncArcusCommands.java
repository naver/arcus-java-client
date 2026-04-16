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

import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.CASValue;
import net.spy.memcached.CachedData;
import net.spy.memcached.KeyValidator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.BTreeCount;
import net.spy.memcached.collection.BTreeCreate;
import net.spy.memcached.collection.BTreeFindPosition;
import net.spy.memcached.collection.BTreeFindPositionWithGet;
import net.spy.memcached.collection.BTreeDelete;
import net.spy.memcached.collection.BTreeGet;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeGetBulkWithByteTypeBkey;
import net.spy.memcached.collection.BTreeGetBulkWithLongTypeBkey;
import net.spy.memcached.collection.BTreeGetByPosition;
import net.spy.memcached.collection.BTreeInsert;
import net.spy.memcached.collection.BTreeInsertAndGet;
import net.spy.memcached.collection.BTreeMutate;
import net.spy.memcached.collection.BTreeOrder;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkey;
import net.spy.memcached.collection.BTreeUpdate;
import net.spy.memcached.collection.BTreeUpsert;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionCount;
import net.spy.memcached.collection.CollectionCreate;
import net.spy.memcached.collection.CollectionDelete;
import net.spy.memcached.collection.CollectionInsert;
import net.spy.memcached.collection.CollectionMutate;
import net.spy.memcached.collection.CollectionUpdate;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.collection.ListCreate;
import net.spy.memcached.collection.ListDelete;
import net.spy.memcached.collection.ListGet;
import net.spy.memcached.collection.ListInsert;
import net.spy.memcached.collection.SetCreate;
import net.spy.memcached.collection.SetDelete;
import net.spy.memcached.collection.SetExist;
import net.spy.memcached.collection.SetGet;
import net.spy.memcached.collection.SetInsert;
import net.spy.memcached.collection.MapCreate;
import net.spy.memcached.collection.MapDelete;
import net.spy.memcached.collection.MapGet;
import net.spy.memcached.collection.MapInsert;
import net.spy.memcached.collection.MapUpdate;
import net.spy.memcached.collection.MapUpsert;
import net.spy.memcached.internal.result.GetsResultImpl;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeFindPositionOperation;
import net.spy.memcached.ops.BTreeFindPositionWithGetOperation;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.BTreeGetByPositionOperation;
import net.spy.memcached.ops.BTreeInsertAndGetOperation;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.CollectionCreateOperation;
import net.spy.memcached.ops.CollectionGetOperation;
import net.spy.memcached.ops.CollectionInsertOperation;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatsOperation;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.transcoders.TranscoderUtils;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;
import net.spy.memcached.v2.vo.BTreePositionElement;
import net.spy.memcached.v2.vo.BTreeElements;
import net.spy.memcached.v2.vo.BTreeUpdateElement;
import net.spy.memcached.v2.vo.BopDeleteArgs;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.GetArgs;
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

  public ArcusFuture<Boolean> append(String key, T val) {
    return concat(ConcatenationType.append, key, val);
  }

  public ArcusFuture<Boolean> prepend(String key, T val) {
    return concat(ConcatenationType.prepend, key, val);
  }

  private ArcusFuture<Boolean> concat(ConcatenationType catType, String key, T val) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);
    CachedData co = tc.encode(val);
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
    Operation op = client.getOpFact().cat(catType, 0L, key, co.getData(), cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Boolean> cas(String key, int exp, T value, long casId) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);
    CachedData co = tc.encode(value);
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            result.set(true);
            break;
          case ERR_NOT_FOUND:
          case ERR_EXISTS:
            result.set(false);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH or unknown statement */
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
            .cas(StoreType.set, key, casId, co.getFlags(), exp, co.getData(), cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Map<String, Boolean>> multiSet(Map<String, T> items, int exp) {
    return multiStore(StoreType.set, items, exp);
  }

  public ArcusFuture<Map<String, Boolean>> multiAdd(Map<String, T> items, int exp) {
    return multiStore(StoreType.add, items, exp);
  }

  public ArcusFuture<Map<String, Boolean>> multiReplace(Map<String, T> items, int exp) {
    return multiStore(StoreType.replace, items, exp);
  }

  /**
   * @param type     store type
   * @param items map of key to value to store
   * @param exp      expiration time
   * @return ArcusFuture with Map of key to Boolean result. If an operation fails exceptionally,
   * the corresponding value in the map will be null.
   */
  private ArcusFuture<Map<String, Boolean>> multiStore(StoreType type,
                                                       Map<String, T> items,
                                                       int exp) {
    Map<String, CompletableFuture<?>> keyToFuture = new HashMap<>(items.size());

    items.forEach((key, value) -> {
      CompletableFuture<Boolean> future = store(type, key, exp, value).toCompletableFuture();
      keyToFuture.put(key, future);
    });

    /* Combine multiple CompletableFutures into a single ArcusFuture. */
    return new ArcusMultiFuture<>(keyToFuture.values(), () -> {
      Map<String, Boolean> results = new HashMap<>();
      keyToFuture.forEach((key, future) -> {
        if (future.isCompletedExceptionally()) {
          results.put(key, null);
        } else {
          results.put(key, (Boolean) future.join());
        }
      });
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

  public ArcusFuture<CASValue<T>> gets(String key) {
    AbstractArcusResult<GetsResultImpl<T>> result
            = new AbstractArcusResult<>(new AtomicReference<>());
    @SuppressWarnings("unchecked")
    ArcusFutureImpl<CASValue<T>> future = new ArcusFutureImpl<>(
            result, r -> r == null ? null : ((GetsResultImpl<T>) r).getDecodedValue());
    ArcusClient client = arcusClientSupplier.get();

    GetsOperation.Callback cb = new GetsOperation.Callback() {
      @Override
      public void gotData(String key, int flags, long cas, byte[] data) {
        CachedData cachedData = new CachedData(flags, data, tc.getMaxSize());
        result.set(new GetsResultImpl<>(cas, cachedData, tc));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
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
    GetsOperation op = client.getOpFact().gets(key, cb);
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


  public ArcusFuture<Long> incr(String key, int delta) {
    return mutate(Mutator.incr, key, delta, -1L, 0);
  }

  public ArcusFuture<Long> incr(String key, int delta, long initial, int exp) {
    if (initial < 0) {
      throw new IllegalArgumentException("Initial value must be 0 or greater.");
    }
    return mutate(Mutator.incr, key, delta, initial, exp);
  }

  public ArcusFuture<Long> decr(String key, int delta) {
    return mutate(Mutator.decr, key, delta, -1L, 0);
  }

  public ArcusFuture<Long> decr(String key, int delta, long initial, int exp) {
    if (initial < 0) {
      throw new IllegalArgumentException("Initial value must be 0 or greater.");
    }
    return mutate(Mutator.decr, key, delta, initial, exp);
  }

  private ArcusFuture<Long> mutate(Mutator mutator, String key, int delta, long initial, int exp) {
    if (delta <= 0) {
      throw new IllegalArgumentException("Delta must be greater than 0.");
    }

    AbstractArcusResult<Long> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Long> future = new ArcusFutureImpl<>(result);
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            result.set(Long.parseLong(status.getMessage()));
            break;
          case ERR_NOT_FOUND:
            result.set(-1L);
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
    Operation op = client.getOpFact().mutate(mutator, key, delta, initial, exp, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Map<String, CASValue<T>>> multiGets(List<String> keys) {
    ArcusClient client = arcusClientSupplier.get();
    Collection<Map.Entry<MemcachedNode, List<String>>> arrangedKeys
            = client.groupingKeys(keys, MemcachedClient.GET_BULK_CHUNK_SIZE, APIType.GETS);

    Collection<CompletableFuture<?>> futures = new ArrayList<>();
    Map<CompletableFuture<Map<String, CASValue<T>>>, List<String>> futureToKeys = new HashMap<>();

    for (Map.Entry<MemcachedNode, List<String>> entry : arrangedKeys) {
      MemcachedNode node = entry.getKey();
      List<String> keyList = entry.getValue();
      CompletableFuture<Map<String, CASValue<T>>> future
              = gets(client, node, keyList).toCompletableFuture();
      futureToKeys.put(future, keyList);
      futures.add(future);
    }

    return new ArcusMultiFuture<>(futures, () -> {
      Map<String, CASValue<T>> results = new HashMap<>();
      futureToKeys.forEach((future, keyList) -> {
        if (future.isCompletedExceptionally()) {
          keyList.forEach(key -> results.put(key, null));
        } else {
          Map<String, CASValue<T>> result = future.join();
          if (result != null) {
            results.putAll(result);
          }
        }
      });
      return results;
    });
  }

  /**
   * Use only in multiGets method.
   *
   * @param keyList key list to get from single node
   * @return ArcusFuture with results.
   */
  private ArcusFuture<Map<String, CASValue<T>>> gets(ArcusClient client, MemcachedNode node,
                                                     List<String> keyList) {
    AbstractArcusResult<Map<String, GetsResultImpl<T>>> result
            = new AbstractArcusResult<>(new AtomicReference<>(new HashMap<>()));

    @SuppressWarnings("unchecked")
    ArcusFutureImpl<Map<String, CASValue<T>>> future = new ArcusFutureImpl<>(result, r -> {
      Map<String, CASValue<T>> decodedMap = new HashMap<>();
      ((Map<String, GetsResultImpl<T>>) r).forEach((key, getsResult) ->
              decodedMap.put(key, getsResult.getDecodedValue()));
      return decodedMap;
    });

    GetsOperation.Callback cb = new GetsOperation.Callback() {
      @Override
      public void gotData(String key, int flags, long cas, byte[] data) {
        Map<String, GetsResultImpl<T>> map = result.get();
        CachedData cachedData = new CachedData(flags, data, tc.getMaxSize());
        map.put(key, new GetsResultImpl<>(cas, cachedData, tc));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
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
    Operation op = client.getOpFact().gets(keyList, cb, node.enabledMGetOp());
    future.setOp(op);
    client.addOp(node, op);

    return future;
  }


  public ArcusFuture<Boolean> delete(String key) {
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
          case ERR_NOT_FOUND:
            result.set(false);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            // unknown statement
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().delete(key, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Map<String, Boolean>> multiDelete(List<String> keys) {
    Map<String, CompletableFuture<?>> keyToFuture = new HashMap<>(keys.size());

    for (String key : keys) {
      CompletableFuture<Boolean> future = delete(key).toCompletableFuture();
      keyToFuture.put(key, future);
    }

    return new ArcusMultiFuture<>(keyToFuture.values(), () -> {
      Map<String, Boolean> results = new HashMap<>();

      keyToFuture.forEach((key, future) -> {
        if (future.isCompletedExceptionally()) {
          results.put(key, null);
        } else {
          results.put(key, (Boolean) future.join());
        }
      });
      return results;
    });
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
    return collectionInsert(key, element.getBKey().toString(), insert);
  }

  public ArcusFuture<Boolean> bopInsert(String key, BTreeElement<T> element) {
    return bopInsert(key, element, null);
  }

  @Override
  public ArcusFuture<Boolean> bopUpsert(String key, BTreeElement<T> element,
                                        CollectionAttributes attributes) {
    BTreeUpsert<T> upsert = new BTreeUpsert<>(element.getValue(), element.getEFlag(),
        null, attributes);
    return collectionInsert(key, element.getBKey().toString(), upsert);
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
        switch (status.getStatusCode()) {
          case SUCCESS:
            result.set(true);
            break;
          case ERR_ELEMENT_EXISTS:
            result.set(false);
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /*
             * TYPE_MISMATCH / BKEY_MISMATCH / OVERFLOWED / OUT_OF_RANGE / NOT_SUPPORTED
             * or unknown statement
             */
            result.addError(key, status);
            break;
        }
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

  public ArcusFuture<Boolean> bopUpdate(String key, BTreeUpdateElement<T> element) {
    BTreeUpdate<T> update = new BTreeUpdate<>(element.getValue(), element.getEFlagUpdate(), false);
    return collectionUpdate(key, element.getBKey().toString(), update);
  }

  private ArcusFuture<Boolean> collectionUpdate(String key,
                                                String internalKey,
                                                CollectionUpdate<T> collectionUpdate) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);
    CachedData co = null;
    if (collectionUpdate.getNewValue() != null) {
      co = tcForCollection.encode(collectionUpdate.getNewValue());
      collectionUpdate.setFlags(co.getFlags());
    }
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            result.set(true);
            break;
          case ERR_NOT_FOUND_ELEMENT:
            result.set(false);
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /*
            * TYPE_MISMATCH / BKEY_MISMATCH / EFLAG_MISMATCH / NOTHING_TO_UPDATE /
            * OVERFLOWED / OUT_OF_RANGE / NOT_SUPPORTED or unknown statement
            */
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
            .collectionUpdate(key, internalKey, collectionUpdate,
                    (co == null) ? null : co.getData(), cb);
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
        trimmedElement = new BTreeElement<>(
                BKey.of(bKeyObject),
                tcForCollection.decode(new CachedData(flags, data, tcForCollection.getMaxSize())),
                eFlag);
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
    if (element.getBKey().getType() == BKey.BKeyType.LONG) {
      insertAndGet = new BTreeInsertAndGet<>((Long) element.getBKey().getData(),
          element.getEFlag(), element.getValue(), isUpsert, attributes);
    } else {
      insertAndGet = new BTreeInsertAndGet<>((byte[]) element.getBKey().getData(),
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

      public void gotData(String bKey, int flags, byte[] data, byte[] eFlag) {
        result.set(new BTreeElement<>(
                BKey.of(bKey),
                tcForCollection.decode(new CachedData(flags, data, tcForCollection.getMaxSize())),
                eFlag));
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

      public void gotData(String bKey, int flags, byte[] data, byte[] eFlag) {
        BTreeElements<T> elements = result.get();
        elements.addElement(new BTreeElement<>(
                BKey.of(bKey),
                tcForCollection.decode(new CachedData(flags, data, tcForCollection.getMaxSize())),
                eFlag));
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
        elements.addElement(new BTreeElement<>(
                BKey.of(bKey),
                tcForCollection.decode(new CachedData(flags, data, tcForCollection.getMaxSize())),
                eFlag));
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

  public ArcusFuture<Integer> bopGetPosition(String key, BKey bKey, BTreeOrder order) {
    AbstractArcusResult<Integer> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Integer> future = new ArcusFutureImpl<>(result);
    BTreeFindPosition findPosition = new BTreeFindPosition(bKey.toString(), order);
    ArcusClient client = arcusClientSupplier.get();

    BTreeFindPositionOperation.Callback cb = new BTreeFindPositionOperation.Callback() {
      @Override
      public void gotData(int position) {
        result.set(position);
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            break;
          case ERR_NOT_FOUND:
          case ERR_NOT_FOUND_ELEMENT:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / BKEY_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().bopFindPosition(key, findPosition, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<BTreeElement<T>> bopGetByPosition(String key, int pos, BTreeOrder order) {
    AbstractArcusResult<BTreeElement<T>> result
            = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<BTreeElement<T>> future = new ArcusFutureImpl<>(result);
    BTreeGetByPosition getByPosition = new BTreeGetByPosition(order, pos);
    ArcusClient client = arcusClientSupplier.get();

    BTreeGetByPositionOperation.Callback cb = new BTreeGetByPositionOperation.Callback() {
      @Override
      public void gotData(int pos, int flags, BKeyObject bKey, byte[] eFlag, byte[] data) {
        result.set(buildBTreeElement(flags, bKey, eFlag, data));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            break;
          case ERR_NOT_FOUND:
          case ERR_NOT_FOUND_ELEMENT:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().bopGetByPosition(key, getByPosition, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<List<BTreeElement<T>>> bopGetByPosition(String key,
                                                             int from, int to,
                                                             BTreeOrder order) {
    if (from > to) {
      throw new IllegalArgumentException("from should be less than or equal to to.");
    }

    AbstractArcusResult<List<BTreeElement<T>>> result
            = new AbstractArcusResult<>(new AtomicReference<>(new ArrayList<>()));
    ArcusFutureImpl<List<BTreeElement<T>>> future = new ArcusFutureImpl<>(result);
    BTreeGetByPosition getByPosition = new BTreeGetByPosition(order, from, to);
    ArcusClient client = arcusClientSupplier.get();

    BTreeGetByPositionOperation.Callback cb = new BTreeGetByPositionOperation.Callback() {
      @Override
      public void gotData(int pos, int flags, BKeyObject bKey, byte[] eFlag, byte[] data) {
        result.get().add(buildBTreeElement(flags, bKey, eFlag, data));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
          case ERR_NOT_FOUND_ELEMENT:
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().bopGetByPosition(key, getByPosition, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<List<BTreePositionElement<T>>> bopPositionWithGet(String key,
                                                                      BKey bKey,
                                                                      int count,
                                                                      BTreeOrder order) {
    AbstractArcusResult<List<BTreePositionElement<T>>> result =
            new AbstractArcusResult<>(new AtomicReference<>(new ArrayList<>()));
    ArcusFutureImpl<List<BTreePositionElement<T>>> future = new ArcusFutureImpl<>(result);
    BTreeFindPositionWithGet findPositionWithGet =
            new BTreeFindPositionWithGet(bKey.toBKeyObject(), order, count);
    ArcusClient client = arcusClientSupplier.get();

    BTreeFindPositionWithGetOperation.Callback cb = new BTreeFindPositionWithGetOperation
            .Callback() {

      @Override
      public void gotData(int pos, int flags, BKeyObject bKey, byte[] eFlag, byte[] data) {
        T decodedData = tcForCollection.decode(
                new CachedData(flags, data, tcForCollection.getMaxSize()));
        result.get().add(new BTreePositionElement<>(BKey.of(bKey), decodedData, eFlag, pos));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
          case ERR_NOT_FOUND_ELEMENT:
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / BKEY_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().bopFindPositionWithGet(key, findPositionWithGet, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  private BTreeElement<T> buildBTreeElement(int flags, BKeyObject bKey,
                                            byte[] eFlag, byte[] data) {
    T decodedData = tcForCollection.decode(
            new CachedData(flags, data, tcForCollection.getMaxSize()));
    return new BTreeElement<>(BKey.of(bKey), decodedData, eFlag);
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
        BTreeElement<T> btreeElement = new BTreeElement<>(
                BKey.of(bKey),
                tcForCollection.decode(new CachedData(flags, data, tcForCollection.getMaxSize())),
                eFlag);
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

  public ArcusFuture<Long> bopIncr(String key, BKey bKey, int delta) {
    CollectionMutate mutate = new BTreeMutate(Mutator.incr, delta);
    return collectionMutate(key, bKey.toString(), mutate);
  }

  public ArcusFuture<Long> bopIncr(String key, BKey bKey, int delta, long initial, byte[] eFlag) {
    CollectionMutate mutate = new BTreeMutate(Mutator.incr, delta, initial, eFlag);
    return collectionMutate(key, bKey.toString(), mutate);
  }

  public ArcusFuture<Long> bopDecr(String key, BKey bKey, int delta) {
    CollectionMutate mutate = new BTreeMutate(Mutator.decr, delta);
    return collectionMutate(key, bKey.toString(), mutate);
  }

  public ArcusFuture<Long> bopDecr(String key, BKey bKey, int delta, long initial, byte[] eFlag) {
    CollectionMutate mutate = new BTreeMutate(Mutator.decr, delta, initial, eFlag);
    return collectionMutate(key, bKey.toString(), mutate);
  }

  private ArcusFuture<Long> collectionMutate(String key, String internalKey,
                                             CollectionMutate mutate) {
    AbstractArcusResult<Long> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Long> future = new ArcusFutureImpl<>(result);
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            result.set(Long.parseLong(status.getMessage()));
            break;
          case ERR_NOT_FOUND:
          case ERR_NOT_FOUND_ELEMENT:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /*
             * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE /
             * OVERFLOWED / NOT_SUPPORTED or unknown statement
             */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().collectionMutate(key, internalKey, mutate, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Boolean> bopDelete(String key, BKey bKey, BopDeleteArgs args) {
    BTreeDelete delete = new BTreeDelete(bKey.toString(),
            args.getEFlagFilter(), args.isDropIfEmpty(), false);
    return collectionDelete(key, delete);
  }

  public ArcusFuture<Boolean> bopDelete(String key, BKey from, BKey to, BopDeleteArgs args) {
    verifyBKeyRange(from, to);
    BTreeDelete delete = new BTreeDelete(from.toString(), to.toString(),
            args.getCount(), args.getEFlagFilter(), args.isDropIfEmpty(), false);
    return collectionDelete(key, delete);
  }

  private ArcusFuture<Boolean> collectionDelete(String key, CollectionDelete delete) {
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
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case ERR_NOT_FOUND_ELEMENT:
            result.set(false);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / BKEY_MISMATCH / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
            break;
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().collectionDelete(key, delete, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Long> bopCount(String key, BKey from, BKey to, ElementFlagFilter eFlagFilter) {
    verifyBKeyRange(from, to);

    AbstractArcusResult<Long> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Long> future = new ArcusFutureImpl<>(result);
    CollectionCount collectionCount = new BTreeCount(from.toString(), to.toString(), eFlagFilter);
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            long count = Long.parseLong(status.getMessage());
            result.set(count);
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / BKEY_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().collectionCount(key, collectionCount, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Boolean> lopCreate(String key, ElementValueType type,
                                        CollectionAttributes attributes) {
    if (attributes == null) {
      throw new IllegalArgumentException("CollectionAttributes cannot be null");
    }

    ListCreate create = new ListCreate(TranscoderUtils.examineFlags(type),
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getOverflowAction(), attributes.getReadable(), false);
    return collectionCreate(key, create);
  }

  public ArcusFuture<Boolean> lopInsert(String key, int index, T value) {
    return lopInsert(key, index, value, null);
  }

  public ArcusFuture<Boolean> lopInsert(String key, int index, T value,
                                        CollectionAttributes attributes) {
    ListInsert<T> insert = new ListInsert<>(value, null, attributes);
    return collectionInsert(key, String.valueOf(index), insert);
  }

  public ArcusFuture<T> lopGet(String key, int index, GetArgs args) {
    AbstractArcusResult<T> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<T> future = new ArcusFutureImpl<>(result);
    ListGet get = new ListGet(index, args.isWithDelete(), args.isDropIfEmpty());
    ArcusClient client = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            break;
          case ERR_NOT_FOUND:
          case ERR_NOT_FOUND_ELEMENT:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }

      @Override
      public void gotData(String subKey, int flags, byte[] data, byte[] eFlag) {
        CachedData cachedData = new CachedData(flags, data, tcForCollection.getMaxSize());
        result.set(tcForCollection.decode(cachedData));
      }
    };
    Operation op = client.getOpFact().collectionGet(key, get, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<List<T>> lopGet(String key, int from, int to, GetArgs args) {
    AbstractArcusResult<List<T>> result =
        new AbstractArcusResult<>(new AtomicReference<>(new ArrayList<>()));
    ArcusFutureImpl<List<T>> future = new ArcusFutureImpl<>(result);
    ListGet get = new ListGet(from, to, args.isWithDelete(), args.isDropIfEmpty());
    ArcusClient client = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
          case ERR_NOT_FOUND_ELEMENT:
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
          }
      }

      @Override
      public void complete() {
        future.complete();
      }

      @Override
      public void gotData(String subKey, int flags, byte[] data, byte[] eFlag) {
        CachedData cachedData = new CachedData(flags, data, tcForCollection.getMaxSize());
        result.get().add(tcForCollection.decode(cachedData));
      }
    };
    Operation op = client.getOpFact().collectionGet(key, get, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Boolean> lopDelete(String key, int index, boolean dropIfEmpty) {
    ListDelete delete = new ListDelete(index, dropIfEmpty, false);
    return collectionDelete(key, delete);
  }

  public ArcusFuture<Boolean> lopDelete(String key, int from, int to, boolean dropIfEmpty) {
    ListDelete delete = new ListDelete(from, to, dropIfEmpty, false);
    return collectionDelete(key, delete);
  }

  public ArcusFuture<Boolean> sopCreate(String key, ElementValueType type,
                                        CollectionAttributes attributes) {
    if (attributes == null) {
      throw new IllegalArgumentException("CollectionAttributes cannot be null");
    }

    SetCreate create = new SetCreate(
            TranscoderUtils.examineFlags(type), attributes.getExpireTime(),
            attributes.getMaxCount(), attributes.getReadable(), false);
    return collectionCreate(key, create);
  }

  public ArcusFuture<Boolean> sopInsert(String key, T value) {
    return sopInsert(key, value, null);
  }

  public ArcusFuture<Boolean> sopInsert(String key, T value, CollectionAttributes attributes) {
    SetInsert<T> insert = new SetInsert<>(value, null, attributes);
    return collectionInsert(key, "", insert);
  }

  public ArcusFuture<Boolean> sopExist(String key, T value) {
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);
    SetExist<T> exist = new SetExist<>(value, tcForCollection);
    ArcusClient client = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case EXIST:
            result.set(true);
            break;
          case NOT_EXIST:
            result.set(false);
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /*
             * TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement
             */
            result.addError(key, status);
            break;
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().collectionExist(key, "", exist, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Set<T>> sopGet(String key, int count, GetArgs args) {
    AbstractArcusResult<Set<T>> result
            = new AbstractArcusResult<>(new AtomicReference<>(new HashSet<>()));
    ArcusFutureImpl<Set<T>> future = new ArcusFutureImpl<>(result);
    SetGet get = new SetGet(count, args.isWithDelete(), args.isDropIfEmpty());
    ArcusClient client = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      @Override
      public void gotData(String subkey, int flags, byte[] data, byte[] eFlag) {
        CachedData cachedData = new CachedData(flags, data, tcForCollection.getMaxSize());
        result.get().add(tcForCollection.decode(cachedData));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
          case ERR_NOT_FOUND_ELEMENT:
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().collectionGet(key, get, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Boolean> sopDelete(String key, T value, boolean dropIfEmpty) {
    SetDelete<T> delete = new SetDelete<>(value, dropIfEmpty, false, tcForCollection);
    return collectionDelete(key, delete);
  }

  public ArcusFuture<Boolean> mopCreate(String key, ElementValueType type,
                                        CollectionAttributes attributes) {
    MapCreate create = new MapCreate(TranscoderUtils.examineFlags(type),
            attributes.getExpireTime(), attributes.getMaxCount(),
            attributes.getReadable(), false);
    return collectionCreate(key, create);
  }

  public ArcusFuture<Boolean> mopInsert(String key, String mKey, T value) {
    return mopInsert(key, mKey, value, null);
  }

  public ArcusFuture<Boolean> mopInsert(String key, String mKey, T value,
                                        CollectionAttributes attributes) {
    MapInsert<T> insert = new MapInsert<>(value, null, attributes);
    return collectionInsert(key, mKey, insert);
  }

  public ArcusFuture<Boolean> mopUpsert(String key, String mKey, T value) {
    return mopUpsert(key, mKey, value, null);
  }

  public ArcusFuture<Boolean> mopUpsert(String key, String mKey, T value,
                                        CollectionAttributes attributes) {
    MapUpsert<T> upsert = new MapUpsert<>(value, attributes);
    return collectionInsert(key, mKey, upsert);
  }

  public ArcusFuture<Boolean> mopUpdate(String key, String mKey, T value) {
    MapUpdate<T> update = new MapUpdate<>(value, false);
    return collectionUpdate(key, mKey, update);
  }

  public ArcusFuture<Map<String, T>> mopGet(String key, GetArgs args) {
    return mopGet(key, new ArrayList<>(), args);
  }

  public ArcusFuture<T> mopGet(String key, String mKey, GetArgs args) {
    AbstractArcusResult<T> result = new AbstractArcusResult<>(new AtomicReference<>());
    ArcusFutureImpl<T> future = new ArcusFutureImpl<>(result);
    List<String> mKeys = Collections.singletonList(mKey);
    MapGet get = new MapGet(mKeys, args.isWithDelete(), args.isDropIfEmpty());
    ArcusClient client = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      @Override
      public void gotData(String mKey, int flags, byte[] data, byte[] eFlag) {
        CachedData cachedData = new CachedData(flags, data, tcForCollection.getMaxSize());
        result.set(tcForCollection.decode(cachedData));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
            break;
          case ERR_NOT_FOUND_ELEMENT:
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().collectionGet(key, get, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Map<String, T>> mopGet(String key, List<String> mKeys, GetArgs args) {
    AbstractArcusResult<Map<String, T>> result =
            new AbstractArcusResult<>(new AtomicReference<>(new HashMap<>()));
    ArcusFutureImpl<Map<String, T>> future = new ArcusFutureImpl<>(result);
    MapGet get = new MapGet(mKeys, args.isWithDelete(), args.isDropIfEmpty());
    ArcusClient client = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      @Override
      public void gotData(String mKey, int flags, byte[] data, byte[] eFlag) {
        CachedData cachedData = new CachedData(flags, data, tcForCollection.getMaxSize());
        result.get().put(mKey, tcForCollection.decode(cachedData));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case SUCCESS:
          case ERR_NOT_FOUND_ELEMENT:
            break;
          case ERR_NOT_FOUND:
            result.set(null);
            break;
          case CANCELLED:
            future.internalCancel();
            break;
          default:
            /* TYPE_MISMATCH / UNREADABLE / NOT_SUPPORTED or unknown statement */
            result.addError(key, status);
        }
      }

      @Override
      public void complete() {
        future.complete();
      }
    };
    Operation op = client.getOpFact().collectionGet(key, get, cb);
    future.setOp(op);
    client.addOp(key, op);

    return future;
  }

  public ArcusFuture<Boolean> mopDelete(String key, boolean dropIfEmpty) {
    return mopDelete(key, new ArrayList<>(), dropIfEmpty);
  }

  public ArcusFuture<Boolean> mopDelete(String key, String mKey, boolean dropIfEmpty) {
    return mopDelete(key, Collections.singletonList(mKey), dropIfEmpty);
  }

  public ArcusFuture<Boolean> mopDelete(String key, List<String> mKeys, boolean dropIfEmpty) {
    MapDelete delete = new MapDelete(mKeys, dropIfEmpty, false);
    return collectionDelete(key, delete);
  }

  public ArcusFuture<Boolean> flush() {
    return flush(-1);
  }

  public ArcusFuture<Boolean> flush(int delay) {
    if (delay < -1) {
      throw new IllegalArgumentException("Delay should be greater than or equal to -1");
    }

    ArcusClient client = arcusClientSupplier.get();
    Collection<MemcachedNode> nodes = client.getFlushNodes();
    Collection<CompletableFuture<?>> futures = new ArrayList<>(nodes.size());

    for (MemcachedNode node : nodes) {
      AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
      ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);

      OperationCallback cb = new OperationCallback() {
        @Override
        public void receivedStatus(OperationStatus status) {
          switch (status.getStatusCode()) {
            case SUCCESS:
              result.set(true);
              break;
            case CANCELLED:
              future.internalCancel();
              break;
            default:
              result.addError(node.getSocketAddress().toString(), status);
              break;
          }
        }

        @Override
        public void complete() {
          future.complete();
        }
      };

      Operation op = client.getOpFact().flush(delay, cb);
      future.setOp(op);
      client.addOp(node, op);
      futures.add(future);
    }

    return new ArcusMultiFuture<>(futures, () -> true);
  }

  public ArcusFuture<Boolean> flush(String prefix) {
    return flush(prefix, -1);
  }

  public ArcusFuture<Boolean> flush(String prefix, int delay) {
    if (prefix == null) {
      throw new IllegalArgumentException("Prefix should not be null");
    }

    if (delay < -1) {
      throw new IllegalArgumentException("Delay should be greater than or equal to -1");
    }

    ArcusClient client = arcusClientSupplier.get();
    Collection<MemcachedNode> nodes = client.getFlushNodes();
    Collection<CompletableFuture<?>> futures = new ArrayList<>(nodes.size());

    for (MemcachedNode node : nodes) {
      AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(new AtomicReference<>());
      ArcusFutureImpl<Boolean> future = new ArcusFutureImpl<>(result);

      OperationCallback cb = new OperationCallback() {
        @Override
        public void receivedStatus(OperationStatus status) {
          switch (status.getStatusCode()) {
            case SUCCESS:
              result.set(true);
              break;
            case ERR_NOT_FOUND:
              result.set(false);
              break;
            case CANCELLED:
              future.internalCancel();
              break;
            default:
              result.addError(node.getSocketAddress().toString(), status);
              break;
          }
        }

        @Override
        public void complete() {
          future.complete();
        }
      };

      Operation op = client.getOpFact()
              .flush(prefix.isEmpty() ? "<null>" : prefix, delay, false, cb);
      future.setOp(op);
      client.addOp(node, op);
      futures.add(future);
    }


    return new ArcusMultiFuture<>(futures, () -> {
      for (CompletableFuture<?> future : futures) {
        if (!future.isCompletedExceptionally() && Boolean.TRUE.equals(future.join())) {
          return true;
        }
      }
      return false;
    });
  }

  public ArcusFuture<Map<SocketAddress, Map<String, String>>> stats() {
    return stats(StatsArg.GENERAL);
  }

  public ArcusFuture<Map<SocketAddress, Map<String, String>>> stats(StatsArg arg) {
    ArcusClient client = arcusClientSupplier.get();
    Collection<MemcachedNode> nodes = client.getAllNodes();

    Map<SocketAddress, CompletableFuture<?>> addressToFuture = new HashMap<>(nodes.size());

    for (MemcachedNode node : nodes) {
      SocketAddress address = node.getSocketAddress();
      AbstractArcusResult<Map<String, String>> result
              = new AbstractArcusResult<>(new AtomicReference<>(new HashMap<>()));
      ArcusFutureImpl<Map<String, String>> future = new ArcusFutureImpl<>(result);

      StatsOperation.Callback cb = new StatsOperation.Callback() {
        @Override
        public void gotStat(String name, String val) {
          result.get().put(name, val);
        }

        @Override
        public void receivedStatus(OperationStatus status) {
          if (status.getStatusCode() == StatusCode.CANCELLED) {
            future.internalCancel();
          }
        }

        @Override
        public void complete() {
          future.complete();
        }
      };
      Operation op = client.getOpFact().stats(arg.getArg(), cb);
      future.setOp(op);
      client.addOp(node, op);

      addressToFuture.put(address, future);
    }

    return new ArcusMultiFuture<>(addressToFuture.values(), () -> {
      Map<SocketAddress, Map<String, String>> resultMap = new HashMap<>(addressToFuture.size());
      addressToFuture.forEach((address, future) -> {
        if (future.isCompletedExceptionally()) {
          resultMap.put(address, null);
        } else {
          @SuppressWarnings("unchecked")
          Map<String, String> stats = (Map<String, String>) future.join();
          resultMap.put(address, stats);
        }
      });
      return resultMap;
    });
  }

  public ArcusFuture<Map<SocketAddress, String>> versions() {
    ArcusClient client = arcusClientSupplier.get();
    Collection<MemcachedNode> nodes = client.getAllNodes();

    Map<SocketAddress, CompletableFuture<?>> addressToFuture = new HashMap<>(nodes.size());

    for (MemcachedNode node : nodes) {
      SocketAddress address = node.getSocketAddress();
      AbstractArcusResult<String> result = new AbstractArcusResult<>(new AtomicReference<>());
      ArcusFutureImpl<String> future = new ArcusFutureImpl<>(result);

      OperationCallback cb = new OperationCallback() {
        @Override
        public void receivedStatus(OperationStatus status) {
          if (status.getStatusCode() == StatusCode.CANCELLED) {
            future.internalCancel();
            return;
          }
          result.set(status.getMessage());
        }

        @Override
        public void complete() {
          future.complete();
        }
      };
      Operation op = client.getOpFact().version(cb);
      future.setOp(op);
      client.addOp(node, op);

      addressToFuture.put(address, future);
    }

    return new ArcusMultiFuture<>(addressToFuture.values(), () -> {
      Map<SocketAddress, String> resultMap = new HashMap<>(addressToFuture.size());
      addressToFuture.forEach((address, future) -> {
        if (future.isCompletedExceptionally()) {
          resultMap.put(address, null);
        } else {
          resultMap.put(address, (String) future.join());
        }
      });
      return resultMap;
    });
  }
}
