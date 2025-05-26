/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
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

import java.util.Objects;

import net.spy.memcached.collection.CollectionResponse;

/**
 * OperationStatus subclass for indicating collection status.
 */
public class CollectionOperationStatus extends OperationStatus {

  private final CollectionResponse collectionResponse;

  public CollectionOperationStatus(boolean success, String msg, CollectionResponse res) {
    super(success, msg, StatusCode.fromAsciiLine(res.name()));
    this.collectionResponse = res;
  }

  public CollectionOperationStatus(OperationStatus status) {
    super(status.isSuccess(), status.getMessage(), status.getStatusCode());
    if (status.getStatusCode() == StatusCode.ERR_INTERNAL) {
      this.collectionResponse = CollectionResponse.EXCEPTION;
    } else {
      this.collectionResponse = CollectionResponse.resolve(status.getMessage());
    }
  }

  /**
   * Get the collection response indicated here.
   */
  public CollectionResponse getResponse() {
    return collectionResponse;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CollectionOperationStatus)) {
      return false;
    }

    CollectionOperationStatus that = (CollectionOperationStatus) o;
    return super.equals(that) &&
            collectionResponse == that.collectionResponse;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), collectionResponse);
  }

}
