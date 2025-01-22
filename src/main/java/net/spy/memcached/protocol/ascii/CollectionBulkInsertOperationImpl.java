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

import net.spy.memcached.collection.CollectionBulkInsert;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionBulkInsertOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to store collection data in a memcached server.
 */
public final class CollectionBulkInsertOperationImpl extends PipeOperationImpl
        implements CollectionBulkInsertOperation {

  public CollectionBulkInsertOperationImpl(CollectionBulkInsert<?> insert, OperationCallback cb) {
    super(insert.getKeyList(), insert, cb);
    if (insert instanceof CollectionBulkInsert.ListBulkInsert) {
      setAPIType(APIType.LOP_INSERT);
    } else if (insert instanceof CollectionBulkInsert.SetBulkInsert) {
      setAPIType(APIType.SOP_INSERT);
    } else if (insert instanceof CollectionBulkInsert.MapBulkInsert) {
      setAPIType(APIType.MOP_INSERT);
    } else if (insert instanceof CollectionBulkInsert.BTreeBulkInsert) {
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
  public CollectionBulkInsert<?> getInsert() {
    return (CollectionBulkInsert<?>) getCollectionPipe();
  }

  @Override
  public boolean isBulkOperation() {
    return true;
  }

}
