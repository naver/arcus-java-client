package net.spy.memcached.v2;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

public class AsyncArcusCommands<T> {

  private final Transcoder<T> tc;

  private final Supplier<ArcusClient> arcusClientSupplier;

  public AsyncArcusCommands(Supplier<ArcusClient> arcusClientSupplier) {
    this.tc = new GenericTranscoder<>();
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
    AtomicReference<Boolean> atomicReference = new AtomicReference<>();
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<Boolean> arcusFuture = new ArcusFutureImpl<>(result);
    CachedData co = tc.encode(value);
    ArcusClient arcusClient = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        StatusCode code = status.getStatusCode();
        switch (code) {
          case SUCCESS:
            atomicReference.set(true);
            break;
          case ERR_NOT_STORED:
            atomicReference.set(false);
            break;
          case CANCELLED:
            arcusFuture.internalCancel();
            break;
          default:
            // TYPE_MISMATCH
            result.addError(status);
            break;
        }
      }

      @Override
      public void complete() {
        arcusFuture.complete();
      }
    };
    Operation op = arcusClient.getOpFact()
        .store(type, key, co.getFlags(), exp, co.getData(), cb);
    arcusClient.addOp(key, op);
    arcusFuture.addOp(op);

    return arcusFuture;
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

  private ArcusFuture<Map<String, Boolean>> multiStore(StoreType type,
                                                       List<String> keys, int exp, T value) {
    AtomicReference<Map<String, Boolean>> atomicReference = new AtomicReference<>(new HashMap<>());
    ArcusResult<Map<String, Boolean>> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<Map<String, Boolean>> arcusFuture = new ArcusFutureImpl<>(result);
    CachedData co = tc.encode(value);
    ArcusClient arcusClient = arcusClientSupplier.get();

    int opCount = keys.size();
    AtomicInteger completedCount = new AtomicInteger();

    for (String key : keys) {
      OperationCallback cb = new OperationCallback() {
        @Override
        public void receivedStatus(OperationStatus status) {
          StatusCode code = status.getStatusCode();
          switch (code) {
            case SUCCESS:
              result.get().put(key, true);
              break;
            case ERR_NOT_STORED:
              result.get().put(key, false);
              break;
            case CANCELLED:
              arcusFuture.internalCancel();
              break;
            default:
              // TYPE_MISMATCH
              result.addError(key, status);
              break;
          }
        }

        @Override
        public void complete() {
          int count = completedCount.incrementAndGet();
          if (count == opCount && !arcusFuture.isDone()) {
            arcusFuture.complete();
          }
        }
      };
      Operation op = arcusClient.getOpFact().store(type, key, co.getFlags(),
          exp, co.getData(), cb);
      arcusClient.addOp(key, op);
      arcusFuture.addOp(op);
    }

    return arcusFuture;
  }

  public ArcusFuture<T> get(String key) {
    AtomicReference<T> atomicReference = new AtomicReference<>();
    ArcusFutureImpl<T> arcusFuture = new ArcusFutureImpl<>(
        new AbstractArcusResult<>(atomicReference));
    ArcusClient arcusClient = arcusClientSupplier.get();

    GetOperation.Callback cb = new GetOperation.Callback() {
      @Override
      public void gotData(String key, int flags, byte[] data) {
        atomicReference.set(tc.decode(new CachedData(flags, data, tc.getMaxSize())));
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        /*
         * For propagating internal cancel to the future.
         */
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          arcusFuture.internalCancel();
        }
      }

      @Override
      public void complete() {
        arcusFuture.complete();
      }
    };
    Operation op = arcusClient.getOpFact().get(key, cb);
    arcusClient.addOp(key, op);
    arcusFuture.addOp(op);

    return arcusFuture;
  }

  public ArcusFuture<Map<String, T>> multiGet(List<String> keys) {
    AtomicReference<Map<String, T>> atomicReference = new AtomicReference<>(new HashMap<>());
    ArcusFutureImpl<Map<String, T>> arcusFuture = new ArcusFutureImpl<>(
        new AbstractArcusResult<>(atomicReference));
    ArcusClient arcusClient = arcusClientSupplier.get();

    Collection<Map.Entry<MemcachedNode, List<String>>> arrangedKeys
        = arcusClient.groupingKeys(keys, MemcachedClient.GET_BULK_CHUNK_SIZE, APIType.GET);

    int opCount = arrangedKeys.size();
    AtomicInteger completedCount = new AtomicInteger();

    GetOperation.Callback cb = new GetOperation.Callback() {
      @Override
      public void gotData(String key, int flags, byte[] data) {
        T value = tc.decode(new CachedData(flags, data, tc.getMaxSize()));
        Map<String, T> map = atomicReference.get();
        map.put(key, value);
      }

      @Override
      public void receivedStatus(OperationStatus status) {
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          arcusFuture.internalCancel();
        }
      }

      @Override
      public void complete() {
        int count = completedCount.incrementAndGet();
        if (count == opCount && !arcusFuture.isDone()) {
          arcusFuture.complete();
        }
      }
    };

    for (Map.Entry<MemcachedNode, List<String>> entry : arrangedKeys) {
      MemcachedNode node = entry.getKey();
      List<String> keyList = entry.getValue();

      Operation op;
      if (node == null) {
        op = arcusClient.getOpFact().get(keyList, cb, false);
      } else {
        op = arcusClient.getOpFact().get(keyList, cb, node.enabledMGetOp());
      }
      arcusClient.addOp(node, op);
      arcusFuture.addOp(op);
    }

    return arcusFuture;
  }

  public ArcusFuture<Boolean> flush(int delay) {
    AtomicReference<Boolean> atomicReference = new AtomicReference<>();
    ArcusFutureImpl<Boolean> arcusFuture = new ArcusFutureImpl<>(
        new AbstractArcusResult<>(atomicReference));
    ArcusClient arcusClient = arcusClientSupplier.get();

    Collection<MemcachedNode> nodes = arcusClient.getFlushNodes();

    int opCount = nodes.size();
    AtomicInteger completedCount = new AtomicInteger();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          arcusFuture.internalCancel();
          return;
        }
        atomicReference.set(status.isSuccess());
      }

      @Override
      public void complete() {
        int count = completedCount.incrementAndGet();
        if (count == opCount && !arcusFuture.isDone()) {
          arcusFuture.complete();
        }
      }
    };

    for (MemcachedNode node : nodes) {
      Operation op = arcusClient.getOpFact().flush(delay, cb);
      arcusClient.addOp(node, op);
      arcusFuture.addOp(op);
    }

    return arcusFuture;
  }
}
