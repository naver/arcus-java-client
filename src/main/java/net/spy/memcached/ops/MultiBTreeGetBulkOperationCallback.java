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
package net.spy.memcached.ops;

public class MultiBTreeGetBulkOperationCallback extends MultiOperationCallback
    implements BTreeGetBulkOperation.Callback {

  public MultiBTreeGetBulkOperationCallback(OperationCallback original, int todo) {
    super(original, todo);
  }

  @Override
  public void gotElement(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
    ((BTreeGetBulkOperation.Callback) originalCallback).gotElement(key, flags, bkey, eflag, data);
  }

  @Override
  public void gotKey(String key, int elementCount, OperationStatus status) {
    ((BTreeGetBulkOperation.Callback) originalCallback).gotKey(key, elementCount, status);
  }
}

