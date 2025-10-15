/*
 * arcus-java-client : Arcus Java client
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
package net.spy.memcached.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;

public abstract class AbstractListenableFuture<T, L>
    extends SpyObject implements ListenableFuture<T, L> {
  /**
   * The {@link ExecutorService} in which the notifications will be handled.
   */
  private final ExecutorService service;

  /**
   * Holds the list of listeners which will be notified upon completion.
   */
  private List<GenericCompletionListener<T>> listeners;
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Creates a new {@link AbstractListenableFuture}.
   *
   * @param executor the executor in which the callbacks will be executed in.
   */
  protected AbstractListenableFuture(ExecutorService executor) {
    if (executor == null) {
      throw new IllegalArgumentException("Executor cannot be null");
    }
    service = executor;
    listeners = new ArrayList<>();
  }

  /**
   * Returns the current executor.
   *
   * @return the current executor service.
   */
  protected ExecutorService executor() {
    return service;
  }

  /**
   * Add the given listener to the total list of listeners to be notified.
   *
   * <p>If the future is already done, the listener will be notified
   * immediately.</p>
   *
   * @param listener the listener to add.
   * @return the current future to allow chaining.
   */
  protected Future<T> addToListeners(final GenericCompletionListener<T> listener) {
    if (listener == null) {
      throw new IllegalArgumentException("The listener can't be null.");
    }

    lock.lock();
    try {
      listeners.add(listener);
    } finally {
      lock.unlock();
    }

    if (isDone()) {
      notifyListeners();
    }

    return this;
  }

  /**
   * Notify a specific listener of completion.
   *
   * @param executor the executor to use.
   * @param future the future to hand over.
   * @param listener the listener to notify.
   */
  protected void notifyListener(final ExecutorService executor, final Future<T> future,
                                final GenericCompletionListener<T> listener) {
    executor.submit(() -> {
      try {
        listener.onComplete(future);
      } catch (Throwable t) {
        getLogger().warn(
                "Exception thrown wile executing " + listener.getClass().getName()
                        + ".operationComplete()", t);
      }
    });
  }

  /**
   * Notify all registered listeners of future completion.
   */
  protected void notifyListeners() {
    notifyListeners(this);
  }

  /**
   * Notify all registered listeners with a special future on completion.
   *
   * This method can be used if a different future should be used for
   * notification than the current one (for example if an enclosing future
   * is used, but the enclosed future should be notified on).
   *
   * @param future the future to pass on to the listeners.
   */
  protected void notifyListeners(final Future<T> future) {
    final List<GenericCompletionListener<T>> copy = new ArrayList<>();
    lock.lock();
    try {
      copy.addAll(listeners);
      listeners = new ArrayList<>();
    } finally {
      lock.unlock();
    }
    for (GenericCompletionListener<T> listener : copy) {
      notifyListener(executor(), future, listener);
    }
  }

  /**
   * Remove a listener from the list of registered listeners.
   *
   * @param listener the listener to remove.
   * @return the current future to allow for chaining.
   */
  protected Future<T> removeFromListeners(GenericCompletionListener<T> listener) {
    if (listener == null) {
      throw new IllegalArgumentException("The listener can't be null.");
    }

    if (!isDone()) {
      lock.lock();
      try {
        listeners.remove(listener);
      } finally {
        lock.unlock();
      }
    }
    return this;
  }
}
