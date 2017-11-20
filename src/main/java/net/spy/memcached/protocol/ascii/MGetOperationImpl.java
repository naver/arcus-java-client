package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;

import java.util.Collection;
import java.util.HashSet;

/**
 * Operation for retrieving data.
 */
public class MGetOperationImpl extends BaseGetOpImpl implements GetOperation {

  private static final String CMD = "mget";

  public MGetOperationImpl(Collection<String> k, Callback c) {
    super(CMD, c, new HashSet<String>(k));
    setAPIType(APIType.MGET);
  }

  /* ENABLE_MIGRATION if */
  public MGetOperationImpl(Collection<String> k, Callback c, Operation p) {
    super(CMD, c, new HashSet<String>(k), p);
    setAPIType(APIType.GET);
  }
  /* ENABLE_MIGRATION end */
}
