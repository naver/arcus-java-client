package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.ConcatenationOperation;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.OperationCallback;

/**
 * Operation for ascii concatenations.
 */
public class ConcatenationOperationImpl extends BaseStoreOperationImpl
        implements ConcatenationOperation {

  private final ConcatenationType concatType;

  public ConcatenationOperationImpl(ConcatenationType t, String k,
                                    byte[] d, OperationCallback cb) {
    super(t.name(), k, 0, 0, d, cb);
    concatType = t;
    if (t == ConcatenationType.append)
      setAPIType(APIType.APPEND);
    else if (t == ConcatenationType.prepend)
      setAPIType(APIType.PREPEND);
  }

  public long getCasValue() {
    // ASCII cat ops don't have CAS.
    return 0;
  }

  public ConcatenationType getStoreType() {
    return concatType;
  }

}
