/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    if (t == ConcatenationType.append) {
      setAPIType(APIType.APPEND);
    } else if (t == ConcatenationType.prepend) {
      setAPIType(APIType.PREPEND);
    }
  }

  public long getCasValue() {
    // ASCII cat ops don't have CAS.
    return 0;
  }

  public ConcatenationType getStoreType() {
    return concatType;
  }

  @Override
  public boolean isIdempotentOperation() {
    return false;
  }

}
