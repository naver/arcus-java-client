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
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;

public class AsyncArcusCommands<T> implements AsyncArcusCommandsIF<T> {

  private final Transcoder<T> tc;
  private final Transcoder<T> tcForCollection;

  private final Supplier<ArcusClient> arcusClientSupplier;

  @SuppressWarnings("unchecked")
  public AsyncArcusCommands(Supplier<ArcusClient> arcusClientSupplier) {
    this.tc = (Transcoder<T>) arcusClientSupplier.get().getTranscoder();
    this.tcForCollection = (Transcoder<T>) arcusClientSupplier.get().getCollectionTranscoder();
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
}
