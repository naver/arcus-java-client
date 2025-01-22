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

import java.util.Collections;

import net.spy.memcached.collection.SetPipedExist;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionPipedExistOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

public final class CollectionPipedExistOperationImpl extends PipeOperationImpl implements
        CollectionPipedExistOperation {

  public CollectionPipedExistOperationImpl(String key,
                                           SetPipedExist<?> collectionExist, OperationCallback cb) {
    super(Collections.singletonList(key), collectionExist, cb);
    setAPIType(APIType.SOP_EXIST);
    setOperationType(OperationType.READ);
  }

  @Override
  protected OperationStatus checkStatus(String line) {
    return matchStatus(line, EXIST, NOT_EXIST,
            NOT_FOUND, TYPE_MISMATCH, UNREADABLE);
  }

  @Override
  public SetPipedExist<?> getExist() {
    return (SetPipedExist<?>) getCollectionPipe();
  }

}
