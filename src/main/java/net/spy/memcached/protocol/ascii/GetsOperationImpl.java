package net.spy.memcached.protocol.ascii;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.Operation;

/**
 * Implementation of the gets operation.
 */
class GetsOperationImpl extends BaseGetOpImpl implements GetsOperation {

  private static final String CMD = "gets";

  public GetsOperationImpl(String key, GetsOperation.Callback cb) {
    super(CMD, cb, Collections.singleton(key));
    setAPIType(APIType.GETS);
  }

  public GetsOperationImpl(Collection<String> keys, GetsOperation.Callback cb) {
    super(CMD, cb, new HashSet<String>(keys));
    setAPIType(APIType.GETS);
  }

  @Override
  public Operation clone() {
    return new GetsOperationImpl(getKeys(), (GetsOperation.Callback)callback);
  }
}
