package net.spy.memcached.protocol.ascii;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetsOperation;

/**
 * Implementation of the gets operation.
 */
class GetsOperationImpl extends BaseGetOpImpl implements GetsOperation {

  private static final String CMD = "gets";
  private static final String CMD_MGETS = "mgets";

  public GetsOperationImpl(String key, GetsOperation.Callback cb) {
    super(CMD, cb, Collections.singleton(key));
    setAPIType(APIType.GETS);
  }

  public GetsOperationImpl(Collection<String> keys, GetsOperation.Callback cb, boolean isMGet) {
    super(isMGet ? CMD_MGETS : CMD, cb, new HashSet<>(keys));
    setAPIType(APIType.GETS);
  }

}
