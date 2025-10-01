package net.spy.memcached.v2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.BTreeCreate;
import net.spy.memcached.collection.BTreeGet;
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.BTreeGetBulkWithLongTypeBkey;
import net.spy.memcached.collection.BTreeGetBulkWithByteTypeBkey;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.BTreeSMGetWithLongTypeBkey;
import net.spy.memcached.collection.BTreeSMGetWithByteTypeBkey;
import net.spy.memcached.collection.SMGetMode;
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
import net.spy.memcached.v2.vo.SMGetResult;

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

  public ArcusFuture<Boolean> bopCreate(String key, ElementValueType type,
                                        CollectionAttributes attributes) {
    CollectionCreate create = new BTreeCreate(TranscoderUtils.examineFlags(type),
        attributes.getExpireTime(), attributes.getMaxCount(),
        attributes.getOverflowAction(), attributes.getReadable(), false);

    return collectionCreate(key, create);
  }

  private ArcusFuture<Boolean> collectionCreate(String key, CollectionCreate collectionCreate) {
    AtomicReference<Boolean> atomicReference = new AtomicReference<>();
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<Boolean> arcusFuture = new ArcusFutureImpl<>(result);
    ArcusClient arcusClient = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        switch (status.getStatusCode()) {
          case ERR_NOT_SUPPORTED:
            result.addError(status);
            break;
          case CANCELLED:
            arcusFuture.internalCancel();
            break;
          default:
            atomicReference.set(status.isSuccess());
        }
      }

      @Override
      public void complete() {
        arcusFuture.complete();
      }
    };
    CollectionCreateOperation op = arcusClient.getOpFact()
        .collectionCreate(key, collectionCreate, cb);
    arcusClient.addOp(key, op);
    arcusFuture.addOp(op);

    return arcusFuture;
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

  private ArcusFuture<Boolean> collectionInsert(String key,
                                                String internalKey,
                                                CollectionInsert<T> collectionInsert) {
    AtomicReference<Boolean> atomicReference = new AtomicReference<>();
    AbstractArcusResult<Boolean> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<Boolean> arcusFuture = new ArcusFutureImpl<>(result);
    CachedData co = tc.encode(collectionInsert.getValue());
    ArcusClient arcusClient = arcusClientSupplier.get();

    OperationCallback cb = new OperationCallback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_ELEMENT_EXISTS:
            case ERR_NOT_FOUND:
              break;
            case CANCELLED:
              arcusFuture.internalCancel();
              return;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OVERFLOWED / OUT_OF_RANGE / NOT_SUPPORTED
               */
              result.addError(status);
              return;
          }
        }
        atomicReference.set(status.isSuccess());
      }

      @Override
      public void complete() {
        arcusFuture.complete();
      }
    };
    CollectionInsertOperation op = arcusClient.getOpFact()
        .collectionInsert(key, internalKey, collectionInsert, co.getData(), cb);
    arcusClient.addOp(key, op);
    arcusFuture.addOp(op);

    return arcusFuture;
  }

  public ArcusFuture<InsertAndGetResult<T>> bopInsertAndGetTrimmed(
      String key, BTreeElement<T> element, CollectionAttributes attributes) {
    AtomicReference<InsertAndGetResult<T>> atomicReference = new AtomicReference<>();
    AbstractArcusResult<InsertAndGetResult<T>> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<InsertAndGetResult<T>> arcusFuture = new ArcusFutureImpl<>(result);
    BTreeInsertAndGet<T> insertAndGet;
    if (element.getBkey().getType() == BKey.BKeyType.LONG) {
      insertAndGet = new BTreeInsertAndGet<>((Long) element.getBkey().getData(),
          element.getEFlag(), element.getValue(), true, attributes);
    } else {
      insertAndGet = new BTreeInsertAndGet<>((byte[]) element.getBkey().getData(),
          element.getEFlag(), element.getValue(), true, attributes);
    }
    CachedData co = tc.encode(insertAndGet.getValue());
    insertAndGet.setFlags(co.getFlags());
    ArcusClient arcusClient = arcusClientSupplier.get();

    BTreeInsertAndGetOperation.Callback cb = new BTreeInsertAndGetOperation.Callback() {
      private boolean isInserted = false;
      private BTreeElement<T> trimmedElement = null;

      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_ELEMENT_EXISTS:
            case ERR_NOT_FOUND:
              break;
            case CANCELLED:
              arcusFuture.internalCancel();
              return;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OVERFLOWED / OUT_OF_RANGE / NOT_SUPPORTED
               */
              result.addError(status);
              return;
          }
        }
        isInserted = status.isSuccess();
      }

      public void complete() {
        atomicReference.set(new InsertAndGetResult<>(isInserted, trimmedElement));
        arcusFuture.complete();
      }

      @Override
      public void gotData(int flags, BKeyObject bkeyObject, byte[] eflag, byte[] data) {
        trimmedElement = new BTreeElement<>(BKey.of(bkeyObject),
            tc.decode(new CachedData(flags, data, tc.getMaxSize())),
            eflag);
      }
    };
    Operation op = arcusClient.getOpFact()
        .bopInsertAndGet(key, insertAndGet, co.getData(), cb);
    arcusClient.addOp(key, op);
    arcusFuture.addOp(op);

    return arcusFuture;
  }

  public ArcusFuture<BTreeElement<T>> bopGet(String key, BKey bkey, BopGetArgs args) {
    AtomicReference<BTreeElement<T>> atomicReference = new AtomicReference<>();
    AbstractArcusResult<BTreeElement<T>> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<BTreeElement<T>> arcusFuture = new ArcusFutureImpl<>(result);
    BTreeGet get = createBTreeGet(bkey, args);
    ArcusClient arcusClient = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_NOT_FOUND:
            case ERR_NOT_FOUND_ELEMENT:
              break;
            case CANCELLED:
              arcusFuture.internalCancel();
              break;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / UNREADABLE / NOT_SUPPORTED
               */
              result.addError(status);
          }
        }
      }

      public void complete() {
        arcusFuture.complete();
      }

      public void gotData(String bKey, int flags, byte[] data, byte[] eflag) {
        atomicReference.set(new BTreeElement<>(BKey.of(bKey),
            tc.decode(new CachedData(flags, data, tc.getMaxSize())), eflag));
      }
    };
    Operation op = arcusClient.getOpFact().collectionGet(key, get, cb);
    arcusClient.addOp(key, op);
    arcusFuture.addOp(op);

    return arcusFuture;
  }

  private static BTreeGet createBTreeGet(BKey bkey, BopGetArgs args) {
    BTreeGet get;
    if (bkey.getType() == BKey.BKeyType.LONG) {
      get = new BTreeGet((Long) bkey.getData(), args.getElementFlagFilter(),
          args.isWithDelete(), args.isDropIfEmpty());
    } else {
      get = new BTreeGet((byte[]) bkey.getData(), args.getElementFlagFilter(),
          args.isWithDelete(), args.isDropIfEmpty());
    }
    return get;
  }

  public ArcusFuture<BTreeElements<T>> bopGet(String key, BKey from, BKey to, BopGetArgs args) {
    verifyBKeyRange(from, to);

    AtomicReference<BTreeElements<T>> atomicReference = new AtomicReference<>();
    AbstractArcusResult<BTreeElements<T>> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<BTreeElements<T>> arcusFuture = new ArcusFutureImpl<>(result);
    BTreeGet get = createBTreeGet(from, to, args);
    ArcusClient arcusClient = arcusClientSupplier.get();

    CollectionGetOperation.Callback cb = new CollectionGetOperation.Callback() {
      public void receivedStatus(OperationStatus status) {
        if (status.getStatusCode() == StatusCode.TRIMMED) {
          atomicReference.get().trimmed();
        } else if (!status.isSuccess()) {
          switch (status.getStatusCode()) {
            case ERR_NOT_FOUND:
            case ERR_NOT_FOUND_ELEMENT:
              atomicReference.set(new BTreeElements<>(new TreeMap<>()));
              break;
            case CANCELLED:
              arcusFuture.internalCancel();
              break;
            default:
              /*
               * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / UNREADABLE / NOT_SUPPORTED
               */
              result.addError(status);
          }
        }
      }

      public void complete() {
        arcusFuture.complete();
      }

      public void gotData(String bKey, int flags, byte[] data, byte[] eflag) {
        if (atomicReference.get() == null) {
          atomicReference.set(new BTreeElements<>(new TreeMap<>()));
        }
        atomicReference.get().addElement(BKey.of(bKey),
            new BTreeElement<>(BKey.of(bKey),
                tc.decode(new CachedData(flags, data, tc.getMaxSize())), eflag));
      }
    };
    Operation op = arcusClient.getOpFact().collectionGet(key, get, cb);
    arcusClient.addOp(key, op);
    arcusFuture.addOp(op);

    return arcusFuture;
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

  /**
   * @implNote Spread request to multiple nodes if keys are mapped to different nodes.
   * Complete when all operations are completed or one of the operations has error.
   */
  public ArcusFuture<Map<String, BTreeElements<T>>> bopMultiGet(List<String> keys,
                                                                BKey from, BKey to,
                                                                BopGetArgs args) {
    verifyBKeyRange(from, to);
    if (args.getCount() <= 0 || args.getCount() > 50) {
      throw new IllegalArgumentException("Count should be between 1 and 50");
    }

    AtomicReference<Map<String, BTreeElements<T>>> atomicReference =
        new AtomicReference<>(new HashMap<>());
    AbstractArcusResult<Map<String, BTreeElements<T>>> result =
        new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<Map<String, BTreeElements<T>>> arcusFuture =
        new ArcusFutureImpl<>(result);
    ArcusClient arcusClient = arcusClientSupplier.get();

    Collection<Map.Entry<MemcachedNode, List<String>>> arrangedKeys =
        arcusClient.groupingKeys(keys, MemcachedClient.GET_BULK_CHUNK_SIZE, APIType.BOP_GET);
    List<BTreeGetBulk<T>> getBulks = new ArrayList<>(arrangedKeys.size());

    for (Map.Entry<MemcachedNode, List<String>> entry : arrangedKeys) {
      getBulks.add(createBTreeGetBulk(from, to, args, entry));
    }

    int opCount = getBulks.size();
    AtomicInteger completedCount = new AtomicInteger();

    BTreeGetBulkOperation.Callback cb = new BTreeGetBulkOperation.Callback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (status.isSuccess()) {
          return;
        }
        if (status.getStatusCode() == StatusCode.CANCELLED) {
          arcusFuture.internalCancel();
          return;
        }
        /*
         * NOT_SUPPORTED
         */
        result.addError(status);
        arcusFuture.complete();
      }

      @Override
      public void complete() {
        int count = completedCount.incrementAndGet();
        if (count == opCount && !arcusFuture.isDone()) {
          arcusFuture.complete();
        }
      }

      @Override
      public void gotKey(String key, int elementCount, OperationStatus status) {
        if (elementCount == 0 && !status.isSuccess()) {
          StatusCode code = status.getStatusCode();
          if (code == StatusCode.ERR_NOT_FOUND) {
            return;
          } else if (code == StatusCode.ERR_NOT_FOUND_ELEMENT) {
            atomicReference.get().put(key, new BTreeElements<>(new TreeMap<>()));
            return;
          }
          /*
           * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / UNREADABLE
           */
          result.addError(status);
          arcusFuture.complete();
        } else if (elementCount > 0 && status.isSuccess()) {
          BTreeElements<T> elements = new BTreeElements<>(new TreeMap<>());
          atomicReference.get().put(key, elements);
          if (status.getStatusCode() == StatusCode.TRIMMED) {
            elements.trimmed();
          }
        }
      }

      @Override
      public void gotElement(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
        BTreeElements<T> elements = atomicReference.get().get(key);
        BKey bKey = BKey.of(bkey);
        elements.addElement(bKey, new BTreeElement<>(bKey,
            tc.decode(new CachedData(flags, data, tc.getMaxSize())), eflag));
      }
    };

    for (BTreeGetBulk<T> getBulk : getBulks) {
      Operation op = arcusClient.getOpFact().bopGetBulk(getBulk, cb);
      arcusClient.addOp(getBulk.getMemcachedNode(), op);
      arcusFuture.addOp(op);
    }

    return arcusFuture;
  }

  private static void verifyBKeyRange(BKey from, BKey to) {
    if (from.getType() != to.getType()) {
      throw new IllegalArgumentException("Two BKey types(from, to) must be the same.");
    }
  }

  private BTreeGetBulk<T> createBTreeGetBulk(BKey from, BKey to, BopGetArgs args,
                                             Map.Entry<MemcachedNode, List<String>> entry) {
    if (from.getType() == BKey.BKeyType.LONG) {
      return new BTreeGetBulkWithLongTypeBkey<>(entry.getKey(), entry.getValue(),
          (Long) from.getData(), (Long) to.getData(), args.getElementFlagFilter(),
          args.getOffset(), args.getCount());
    } else {
      return new BTreeGetBulkWithByteTypeBkey<>(entry.getKey(), entry.getValue(),
          (byte[]) from.getData(), (byte[]) to.getData(), args.getElementFlagFilter(),
          args.getOffset(), args.getCount());
    }
  }

  public ArcusFuture<SMGetResult<T>> bopSortMergeGet(List<String> keys, BKey from, BKey to,
                                                     boolean unique, BopGetArgs args) {
    verifyBKeyRange(from, to);

    List<SMGetResult.SMGetElement<T>> smGetElements = new ArrayList<>();
    List<SMGetResult.MissedKey> missedKeys = new ArrayList<>();
    List<SMGetResult.TrimmedKey> trimmedKeys = new ArrayList<>();
    SMGetResult<T> smGetResult = new SMGetResult<>(smGetElements, missedKeys, trimmedKeys);

    AtomicReference<SMGetResult<T>> atomicReference = new AtomicReference<>(smGetResult);
    AbstractArcusResult<SMGetResult<T>> result = new AbstractArcusResult<>(atomicReference);
    ArcusFutureImpl<SMGetResult<T>> arcusFuture = new ArcusFutureImpl<>(result);
    ArcusClient arcusClient = arcusClientSupplier.get();

    Collection<Map.Entry<MemcachedNode, List<String>>> arrangedKeys =
        arcusClient.groupingKeys(keys, 500, APIType.BOP_SMGET);
    List<BTreeSMGet<T>> smGetList = new ArrayList<>(arrangedKeys.size());
    SMGetMode smgetMode = unique ? SMGetMode.UNIQUE : SMGetMode.DUPLICATE;

    for (Map.Entry<MemcachedNode, List<String>> entry : arrangedKeys) {
      smGetList.add(createBTreeSMGet(from, to, args, smgetMode, entry));
    }

    int opCount = smGetList.size();
    AtomicInteger completedCount = new AtomicInteger();

    BTreeSortMergeGetOperation.Callback cb = new BTreeSortMergeGetOperation.Callback() {
      @Override
      public void receivedStatus(OperationStatus status) {
        if (!status.isSuccess()) {
          if (status.getStatusCode() == StatusCode.CANCELLED) {
            arcusFuture.internalCancel();
          } else {
            /*
             * TYPE_MISMATCH / BKEY_MISMATCH / OUT_OF_RANGE / NOT_SUPPORTED
             */
            result.addError(status);
            arcusFuture.complete();
          }
        }
      }

      @Override
      public void complete() {
        int count = completedCount.incrementAndGet();
        if (count == opCount && !arcusFuture.isDone()) {
          Collections.sort(smGetElements);
          Collections.sort(missedKeys);
          Collections.sort(trimmedKeys);
          arcusFuture.complete();
        }
      }

      @Override
      public void gotData(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
        BKey bKey = BKey.of(bkey);
        T value = tc.decode(new CachedData(flags, data, tc.getMaxSize()));
        BTreeElement<T> btreeElement = new BTreeElement<>(bKey, value, eflag);
        smGetElements.add(new SMGetResult.SMGetElement<>(key, btreeElement));
      }

      @Override
      public void gotMissedKey(String key, OperationStatus cause) {
        missedKeys.add(new SMGetResult.MissedKey(key, cause.getStatusCode()));
      }

      @Override
      public void gotTrimmedKey(String key, Object bkey) {
        BKey bKey = BKey.of(bkey);
        trimmedKeys.add(new SMGetResult.TrimmedKey(key, bKey));
      }
    };

    for (BTreeSMGet<T> smGet : smGetList) {
      Operation op = arcusClient.getOpFact().bopsmget(smGet, cb);
      arcusClient.addOp(smGet.getMemcachedNode(), op);
      arcusFuture.addOp(op);
    }

    return arcusFuture;
  }

  private BTreeSMGet<T> createBTreeSMGet(BKey from, BKey to, BopGetArgs args,
                                         SMGetMode smgetMode,
                                         Map.Entry<MemcachedNode, List<String>> entry) {

    if (from.getType() == BKey.BKeyType.LONG) {
      return new BTreeSMGetWithLongTypeBkey<>(entry.getKey(), entry.getValue(),
          (Long) from.getData(), (Long) to.getData(), args.getElementFlagFilter(),
          args.getCount(), smgetMode);
    } else {
      return new BTreeSMGetWithByteTypeBkey<>(entry.getKey(), entry.getValue(),
          (byte[]) from.getData(), (byte[]) to.getData(), args.getElementFlagFilter(),
          args.getCount(), smgetMode);
    }
  }
}
