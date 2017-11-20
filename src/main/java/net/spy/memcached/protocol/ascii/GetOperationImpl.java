// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;

/**
 * Operation for retrieving data.
 */
class GetOperationImpl extends BaseGetOpImpl implements GetOperation {

  private static final String CMD = "get";

  public GetOperationImpl(String key, GetOperation.Callback c) {
    super(CMD, c, Collections.singleton(key));
    setAPIType(APIType.GET);
  }

  public GetOperationImpl(Collection<String> k, GetOperation.Callback c) {
    super(CMD, c, new HashSet<String>(k));
    setAPIType(APIType.GET);
  }

  /* ENABLE_MIGRATION if */
  public GetOperationImpl(Collection<String> k, GetOperation.Callback c, Operation p) {
    super(CMD, c, new HashSet<String>(k), p);
    setAPIType(APIType.GET);
  }
  /* ENABLE_MIGRATION end */

}
