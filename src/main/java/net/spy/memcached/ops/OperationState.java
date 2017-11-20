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

/**
 * State of this operation.
 */
public enum OperationState {
  /**
   * State indicating this operation is writing data to the server.
   */
  WRITING,
  /**
   * State indicating this operation is reading data from the server.
   */
  READING,
  /**
   * State indicating this operation is complete.
   */
  COMPLETE,
  /* ENABLE_REPLICATION if */
  /**
   * State indicating this operation will be moved by switchover or failover
   */
  MOVING
  /* ENABLE_REPLICATION end */
  /* ENABLE_MIGRATION if */
  /**
   * State indication this key (operation) was migrated
   */
  , MIGRATING
  /* ENABLE_MIGRATION end */
}