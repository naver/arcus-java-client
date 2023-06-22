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
package net.spy.memcached;

import java.net.SocketAddress;

import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;

/**
 * Default connection factory for binary wire protocol connections.
 */
public class BinaryConnectionFactory extends DefaultConnectionFactory {

  /**
   * Create a DefaultConnectionFactory with the default parameters.
   */
  public BinaryConnectionFactory() {
    super();
  }

  /**
   * Create a BinaryConnectionFactory with the given maximum operation
   * queue length, and the given read buffer size.
   */
  public BinaryConnectionFactory(int len, int bufSize) {
    super(len, bufSize);
  }

  /**
   * Construct a BinaryConnectionFactory with the given parameters.
   *
   * @param len     the queue length.
   * @param bufSize the buffer size
   * @param hash    the algorithm to use for hashing
   */
  public BinaryConnectionFactory(int len, int bufSize, HashAlgorithm hash) {
    super(len, bufSize, hash);
  }

  @Override
  public MemcachedNode createMemcachedNode(String name,
                                           SocketAddress sa,
                                           int bufSize) {
    boolean doAuth = false;
    return new BinaryMemcachedNodeImpl(name,
            sa, bufSize,
            createReadOperationQueue(),
            createWriteOperationQueue(),
            createOperationQueue(),
            getOpQueueMaxBlockTime(),
            doAuth);
  }

  @Override
  public OperationFactory getOperationFactory() {
    return new BinaryOperationFactory();
  }

}
