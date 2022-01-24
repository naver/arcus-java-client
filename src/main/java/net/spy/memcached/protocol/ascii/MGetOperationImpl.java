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

  @Override
  public boolean isBulkOperation() {
    return true;
  }

  @Override
  public boolean isPipeOperation() {
    return false;
  }

  @Override
  public Operation clone() {
    return new MGetOperationImpl(getKeys(), (Callback)callback);
  }
}
