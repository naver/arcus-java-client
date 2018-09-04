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

/* for Operation API String */
public enum APIType {
  // KV API Type
  SET(OperationType.WRITE), ADD(OperationType.WRITE), REPLACE(OperationType.WRITE),
  APPEND(OperationType.WRITE), PREPEND(OperationType.WRITE),
  CAS(OperationType.WRITE),
  INCR(OperationType.WRITE), DECR(OperationType.WRITE),
  DELETE(OperationType.WRITE),
  GET(OperationType.READ), GETS(OperationType.READ), MGET(OperationType.READ),

  // List API Type
  LOP_CREATE(OperationType.WRITE),
  LOP_INSERT(OperationType.WRITE),
  LOP_DELETE(OperationType.WRITE),
  LOP_GET(OperationType.RW),

  // Set API Type
  SOP_CREATE(OperationType.WRITE),
  SOP_INSERT(OperationType.WRITE),
  SOP_DELETE(OperationType.WRITE),
  SOP_EXIST(OperationType.READ),
  SOP_GET(OperationType.RW),

  // Map API Type
  MOP_CREATE(OperationType.WRITE),
  MOP_INSERT(OperationType.WRITE),
  MOP_UPDATE(OperationType.WRITE),
  MOP_DELETE(OperationType.WRITE),
  MOP_GET(OperationType.RW),

  // B+Tree API Type
  BOP_CREATE(OperationType.WRITE),
  BOP_INSERT(OperationType.WRITE),
  BOP_UPSERT(OperationType.WRITE),
  BOP_UPDATE(OperationType.WRITE),
  BOP_INCR(OperationType.WRITE), BOP_DECR(OperationType.WRITE),
  BOP_COUNT(OperationType.READ),
  BOP_GET(OperationType.RW),
  BOP_SMGET(OperationType.READ),
  BOP_POSITION(OperationType.READ),
  BOP_GBP(OperationType.READ),
  BOP_PWG(OperationType.READ),
  BOP_DELETE(OperationType.WRITE),

  // Attribute API Type
  RNG_GET(OperationType.READ),
  SETATTR(OperationType.WRITE), GETATTR(OperationType.READ),

  // Other API
  FLUSH(OperationType.WRITE),
  STATS(OperationType.ETC),
  VERSION(OperationType.ETC),

  // undefined API
  UNDEFINED(OperationType.UNDEFINED);

  private final OperationType apiOpType;

  private APIType(OperationType t) {
    this.apiOpType = t;
  }

  public OperationType getAPIOpType() {
    return this.apiOpType;
  }
}
