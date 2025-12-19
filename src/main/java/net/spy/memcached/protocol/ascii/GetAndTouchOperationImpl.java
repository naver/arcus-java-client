/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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

import java.util.Collections;

import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetOperation;

/**
 * Implementation of the get and touch operation.
 */
class GetAndTouchOperationImpl extends BaseGetOpImpl implements GetOperation {
  private static final String CMD = "gat";

  public GetAndTouchOperationImpl(String k, int e, GetOperation.Callback cb) {
    super(CMD, e, cb, Collections.singleton(k));
    setAPIType(APIType.GAT);
  }
}
