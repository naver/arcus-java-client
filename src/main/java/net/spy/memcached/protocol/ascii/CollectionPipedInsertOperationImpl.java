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

import net.spy.memcached.collection.CollectionPipedInsert;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionPipedInsertOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to store collection data in a memcached server.
 */
public final class CollectionPipedInsertOperationImpl extends PipeOperationImpl
        implements CollectionPipedInsertOperation {

  public CollectionPipedInsertOperationImpl(String key,
                                            CollectionPipedInsert<?> insert, OperationCallback cb) {
    super(Collections.singletonList(key), insert, cb);
    if (insert instanceof CollectionPipedInsert.ListPipedInsert) {
      setAPIType(APIType.LOP_INSERT);
    } else if (insert instanceof CollectionPipedInsert.SetPipedInsert) {
      setAPIType(APIType.SOP_INSERT);
    } else if (insert instanceof CollectionPipedInsert.MapPipedInsert) {
      setAPIType(APIType.MOP_INSERT);
    } else if (insert instanceof CollectionPipedInsert.BTreePipedInsert) {
      setAPIType(APIType.BOP_INSERT);
    } else if (insert instanceof CollectionPipedInsert.ByteArraysBTreePipedInsert) {
      setAPIType(APIType.BOP_INSERT);
    }
    setOperationType(OperationType.WRITE);
  }

  @Override
  protected OperationStatus checkStatus(String line) {
    return matchStatus(line, STORED, CREATED_STORED,
            NOT_FOUND, ELEMENT_EXISTS, OVERFLOWED, OUT_OF_RANGE,
            TYPE_MISMATCH, BKEY_MISMATCH);
  }

  @Override
  public CollectionPipedInsert<?> getInsert() {
    return (CollectionPipedInsert<?>) getCollectionPipe();
  }
}
