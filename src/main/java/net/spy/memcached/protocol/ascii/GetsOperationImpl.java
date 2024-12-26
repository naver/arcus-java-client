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

  public GetsOperationImpl(String key, GetsOperation.Callback cb, String command) {
    super(command, cb, Collections.singleton(key));
    setAPIType(APIType.GETS);
  }

  public GetsOperationImpl(Collection<String> keys, GetsOperation.Callback cb, String command) {
    super(command, cb, new HashSet<>(keys));
    setAPIType(APIType.GETS);
  }

  public static GetsOperationImpl generateGetsOp(String key, GetsOperation.Callback c,
                                                 boolean isMget) {
    if (isMget) {
      return new GetsOperationImpl(key, c, CMD_MGETS);
    }
    return new GetsOperationImpl(key, c, CMD);
  }

  public static GetsOperationImpl generateGetsOp(Collection<String> keys, GetsOperation.Callback c,
                                                 boolean isMget) {
    if (isMget) {
      return new GetsOperationImpl(keys, c, CMD_MGETS);
    }
    return new GetsOperationImpl(keys, c, CMD);
  }

}
