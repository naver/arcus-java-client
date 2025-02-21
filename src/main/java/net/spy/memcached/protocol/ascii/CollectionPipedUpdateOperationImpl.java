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

import net.spy.memcached.collection.CollectionPipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.BTreePipedUpdate;
import net.spy.memcached.collection.CollectionPipedUpdate.MapPipedUpdate;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionPipedUpdateOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;

/**
 * Operation to update collection data in a memcached server.
 */
public final class CollectionPipedUpdateOperationImpl extends PipeOperationImpl implements
        CollectionPipedUpdateOperation {

  public CollectionPipedUpdateOperationImpl(String key,
                                            CollectionPipedUpdate<?> update, OperationCallback cb) {
    super(Collections.singletonList(key), update, cb);
    if (update instanceof BTreePipedUpdate) {
      setAPIType(APIType.BOP_UPDATE);
    } else if (update instanceof MapPipedUpdate) {
      setAPIType(APIType.MOP_UPDATE);
    }
  }

  @Override
  protected OperationStatus checkStatus(String line) {
    return matchStatus(line, UPDATED, NOT_FOUND,
            NOT_FOUND_ELEMENT, NOTHING_TO_UPDATE, TYPE_MISMATCH,
            BKEY_MISMATCH, EFLAG_MISMATCH);
  }

  @Override
  public CollectionPipedUpdate<?> getUpdate() {
    return (CollectionPipedUpdate<?>) getCollectionPipe();
  }
}
