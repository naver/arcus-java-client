// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetOperation;

/**
 * Operation for retrieving data.
 */
class GetOperationImpl extends BaseGetOpImpl implements GetOperation {

  private static final String CMD = "get";
  private static final String CMD_MGET = "mget";

  public GetOperationImpl(String key, GetOperation.Callback c, String command) {
    super(command, c, Collections.singleton(key));
    setAPIType(APIType.GET);
  }

  public GetOperationImpl(Collection<String> k, GetOperation.Callback c, String command) {
    super(command, c, new HashSet<>(k));
    setAPIType(APIType.GET);
  }

  public static GetOperationImpl generateGetOp(String key, GetOperation.Callback c,
                                               boolean isMget) {
    if (isMget) {
      return new GetOperationImpl(key, c, CMD_MGET);
    }
    return new GetOperationImpl(key, c, CMD);
  }

  public static GetOperationImpl generateGetOp(Collection<String> keys, GetOperation.Callback c,
                                               boolean isMget) {
    if (isMget) {
      return new GetOperationImpl(keys, c, CMD_MGET);
    }
    return new GetOperationImpl(keys, c, CMD);
  }

}
