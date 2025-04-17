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
package net.spy.memcached.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Arcus-specific distributed lock interface extending {@link java.util.concurrent.locks.Lock}.
 * Provides locking operations with support for reentrancy and lease time.
 */
public interface ArcusLock extends Lock {

  /**
   * Acquires the lock and holds it for the specified {@code leaseTime}.
   *
   * <p>If the lock is not available, the current thread waits until it acquires the lock
   * and cannot be used for other tasks during this time.
   * Automatically releases the lock after {@code leaseTime}, unless unlocked earlier.
   *
   * <p><b>Note:</b> Only {@link TimeUnit#SECONDS} or coarser units
   * (e.g., MINUTES, HOURS) are supported for the {@code unit} parameter.
   * Finer units cause an {@link IllegalArgumentException}.
   *
   * @param leaseTime the maximum time to hold the lock after it is acquired.
   *                  If {@code leaseTime} is negative or zero,
   *                  the lock is held until {@code unlock()} is called.
   * @param unit the time unit of {@code leaseTime} (must be SECONDS or coarser)
   */
  void lock(long leaseTime, TimeUnit unit);

  /**
   * Acquires the lock and holds it for the specified {@code leaseTime},
   * unless the current thread is interrupted before the lock is acquired.
   *
   * <p>If the lock is not available, the current thread waits until it acquires the lock.
   * If the thread is interrupted while waiting,
   * it throws an {@link InterruptedException} and fails to acquire the lock.
   *
   * <p> After acquiring the lock, the thread holds it for up to {@code leaseTime},
   * even if it is interrupted later.
   * Automatically releases the lock after {@code leaseTime}, unless unlocked earlier.
   *
   * <p><b>Note:</b> Only {@link TimeUnit#SECONDS} or coarser units
   * (e.g., MINUTES, HOURS) are supported for the {@code unit} parameter.
   * Finer units cause an {@link IllegalArgumentException}.
   *
   * @param leaseTime the maximum time to hold the lock after it is acquired.
   *                  If {@code leaseTime} is negative or zero,
   *                  the lock is held until {@code unlock()} is called.
   * @param unit the time unit of {@code leaseTime} (must be SECONDS or coarser)
   * @throws InterruptedException if the current thread is interrupted
   *                  while waiting to acquire the lock
   */
  void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException;

  /**
   * Tries to acquire the lock within the specified {@code waitTime}.
   * If successful, the lock will be held until {@code unlock()} is called.
   *
   * <p>If the thread is interrupted while waiting, it throws {@link InterruptedException}
   * and does not acquire the lock. If {@code waitTime} is negative or zero,
   * the method returns immediately without waiting.
   *
   * <p><b>Note:</b> Only {@link TimeUnit#SECONDS} or coarser units
   * (e.g., MINUTES, HOURS) are supported for the {@code unit} parameter.
   * Finer units cause an {@link IllegalArgumentException}.
   *
   * @param waitTime the maximum time to wait for the lock
   * @param unit the time unit of the {@code waitTime} (must be SECONDS or coarser)
   * @return {@code true} if the lock was acquired and {@code false}
   *         if the waiting time elapsed before the lock was acquired
   * @throws InterruptedException if the current thread is interrupted
   *                  while waiting to acquire the lock
   */
  boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException;

  /**
   * Tries to acquire the lock within the specified {@code waitTime}.
   * If successful, the lock is held for {@code leaseTime}.
   *
   * <p><b>Note:</b> Only {@link TimeUnit#SECONDS} or coarser units
   * (e.g., MINUTES, HOURS) are supported for the {@code unit} parameter.
   * Finer units cause an {@link IllegalArgumentException}.
   *
   * @param waitTime the maximum time to wait for the lock
   * @param leaseTime the maximum time to hold the lock after it is acquired.
   *                  If {@code leaseTime} is negative or zero,
   *                  the lock is held until {@code unlock()} is called.
   * @param unit the time unit of {@code waitTime} and {@code leaseTime}
   *             (must be SECONDS or coarser)
   * @return {@code true} if the lock was acquired and {@code false}
   *         if the waiting time elapsed before the lock was acquired
   * @throws InterruptedException if the current thread is interrupted
   *                  while waiting to acquire the lock
   */
  boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException;

  /**
   * Checks if the lock is currently held by any thread.
   *
   * @return {@code true} if the lock is held by any thread and {@code false} otherwise
   */
  boolean isLocked();

  /**
   * Checks if the lock is currently held by the specified thread.
   *
   * @param threadId the ID of the thread to check
   * @return {@code true} if the lock is held by the specified thread and {@code false} otherwise
   */
  boolean isHeldByThread(long threadId);

  /**
   * Checks if the current thread holds the lock.
   *
   * @return {@code true} if the current thread holds the lock and {@code false} otherwise
   */
  boolean isHeldByCurrentThread();

}
