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

public class MultiBTreeSortMergeGetOperationCallback extends MultiOperationCallback
    implements BTreeSortMergeGetOperation.Callback {

  public MultiBTreeSortMergeGetOperationCallback(OperationCallback original, int todo) {
    super(original, todo);
  }

  @Override
  public void gotData(String key, int flags, Object subkey, byte[] eflag, byte[] data) {
    ((BTreeSortMergeGetOperation.Callback) originalCallback).gotData(key, flags,
        subkey, eflag, data);
  }

  @Override
  public void gotMissedKey(String key, OperationStatus cause) {
    ((BTreeSortMergeGetOperation.Callback) originalCallback).gotMissedKey(key, cause);
  }

  @Override
  public void gotTrimmedKey(String key, Object subkey) {
    ((BTreeSortMergeGetOperation.Callback) originalCallback).gotTrimmedKey(key, subkey);
  }
}
