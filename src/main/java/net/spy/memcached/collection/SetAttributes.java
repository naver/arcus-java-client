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
package net.spy.memcached.collection;

/**
 * Contract for producing the {@code setattr} attribute clause.
 * Implemented by the v1 {@link Attributes} and
 * the v2 {@link net.spy.memcached.v2.attribute.UpdateAttributes}
 * so a single setattr operation can serialize either.
 *
 * <p>The clause text is produced by {@link #stringify()}.
 * {@link #getLength()} returns its length for buffer sizing.
 */
public interface SetAttributes {

  /**
   * Serializes the attributes into the {@code setattr} command's attribute clause –
   * space-separated {@code name=value} tokens
   * (e.g. {@code "expiretime=100 maxcount=1000"}).
   *
   * @return the attribute clause text
   */
  String stringify();

  /**
   * @return the length of {@link #stringify()}, used to size the request buffer
   */
  int getLength();
}
