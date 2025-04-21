/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2025 JaM2in Co., Ltd.
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Arcus-specific distributed lock interface extending {@link java.util.concurrent.locks.Lock}.
 * Provides locking operations with support for reentrancy and lease time.
 */
public interface ArcusLock extends Lock {

  /**
   * Acquires the lock and holds it for the specified {@code leaseTime}.
   * If the lock is not available, the current thread waits until it is acquired
   * and cannot be used for other tasks during this time.
   * Automatically releases the lock after {@code leaseTime}, unless unlocked earlier.
   *
   * @param leaseTime the maximum time to hold the lock after it is acquired.
   *                  If {@code leaseTime} is -1, the lock is held until {@code unlock()} is called.
   * @param unit the time unit of {@code leaseTime}
   */
  void lock(long leaseTime, TimeUnit unit);

  /**
   * Acquires the lock and holds it for the specified {@code leaseTime}, unless interrupted.
   * If the lock is not available, the current thread waits until it is acquired
   * and cannot be used for other tasks during this time.
   * Automatically releases the lock after {@code leaseTime}, unless unlocked earlier.
   *
   * @param leaseTime the maximum time to hold the lock after it is acquired.
   *                  If {@code leaseTime} is -1, the lock is held until {@code unlock()} is called.
   * @param unit the time unit of {@code leaseTime}
   * @throws InterruptedException if the current thread is interrupted
   */
  void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException;

  /**
   * Tries to acquire the lock within the specified {@code waitTime},
   * holding it for {@code leaseTime} if successful.
   *
   * @param waitTime the maximum time to wait for the lock
   * @param leaseTime the maximum time to hold the lock after it is acquired.
   *                  If {@code leaseTime} is negative or zero, the lock is held until {@code unlock()} is called.
   * @param unit the time unit of {@code waitTime} and {@code leaseTime}
   * @return {@code true} if the lock was acquired, otherwise {@code false}
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException;
}
