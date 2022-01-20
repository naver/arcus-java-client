// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;


import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;

/**
 * Operation to store data in a memcached server.
 */
final class StoreOperationImpl extends BaseStoreOperationImpl
        implements StoreOperation {

  private final StoreType storeType;

  public StoreOperationImpl(StoreType t, String k, int f, int e,
                            byte[] d, OperationCallback cb) {
    super(t.name(), k, f, e, d, cb);
    storeType = t;
    if (t == StoreType.add) {
      setAPIType(APIType.ADD);
    } else if (t == StoreType.set) {
      setAPIType(APIType.SET);
    } else if (t == StoreType.replace) {
      setAPIType(APIType.REPLACE);
    }
  }

  public StoreType getStoreType() {
    return storeType;
  }

  @Override
  public Operation clone() {
    return new StoreOperationImpl(storeType, key, flags, exp, data, callback);
  }
}
