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
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;

/**
 * Operation to store data in a memcached server.
 */
final class StoreOperationImpl extends BaseStoreOperationImpl
        implements StoreOperation {

  private final StoreType storeType;

  public StoreOperationImpl(StoreType t, String k, int f, int e,
                            byte[] d, OperationCallback cb) {
    super(t.name(), k, f, e, d, cb);
    storeType = t;
    if (t == StoreType.add) {
      setAPIType(APIType.ADD);
    } else if (t == StoreType.set) {
      setAPIType(APIType.SET);
    } else if (t == StoreType.replace) {
      setAPIType(APIType.REPLACE);
    }
  }

  public StoreType getStoreType() {
    return storeType;
  }

}
