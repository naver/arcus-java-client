package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.Operation;

import java.util.Collection;
import java.util.HashSet;

/**
 * Operation for retrieving data.
 */
public class MGetsOperationImpl extends BaseGetOpImpl implements GetsOperation {

  private static final String CMD = "mgets";

  public MGetsOperationImpl(Collection<String> k, Callback c) {
    super(CMD, c, new HashSet<String>(k));
    setAPIType(APIType.MGETS);
  }

  @Override
  public Operation clone() {
    return new MGetsOperationImpl(getKeys(), (Callback)callback);
  }
}
