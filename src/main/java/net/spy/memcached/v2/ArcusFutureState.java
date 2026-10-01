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
package net.spy.memcached.v2;

enum ArcusFutureState {
  /**
   * Waiting for a response from the cache server.
   * The timeout task is valid, and cancellation also cancels the operation.
   */
  WAITING_RESPONSE,

  /**
   * The complete response arrived first.
   * Errors are being checked or the result is being decoded.
   * A timeout is ignored, and cancellation discards only the result.
   */
  RESPONSE_RECEIVED,

  /**
   * The outcome is decided and can no longer change.
   * The future may not be completed yet.
   */
  DECIDED,
}
