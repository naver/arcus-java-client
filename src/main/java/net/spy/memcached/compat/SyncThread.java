// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.compat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * Thread that invokes a callable multiple times concurrently.
 */
public class SyncThread<T> extends SpyThread {

  private final Callable<T> callable;
  private final CyclicBarrier barrier;
  private final CountDownLatch latch;
  private Throwable throwable = null;
  private T rv = null;

  /**
   * Get a SyncThread that will call the given callable when the given
   * barrier allows it past.
   *
   * @param b the barrier
   * @param c the callable
   */
  public SyncThread(CyclicBarrier b, Callable<T> c) {
    super("SyncThread");
    callable = c;
    barrier = b;
    latch = new CountDownLatch(1);
  }

  @Override
  public synchronized void start() {
    setDaemon(true);
    super.start();
  }

  /**
   * Wait for the barrier, invoke the callable and capture the result or an
   * exception.
   */
  @Override
  public void run() {
    try {
      barrier.await();
      rv = callable.call();
    } catch (Throwable t) {
      throwable = t;
    }
    latch.countDown();
  }

  /**
   * Get the result from the invocation.
   *
   * @return the result
   * @throws Throwable if an error occurred when evaluating the callable
   */
  public T getResult() throws Throwable {
    latch.await();
    if (throwable != null) {
      throw throwable;
    }
    return rv;
  }

  /**
   * Get a collection of SyncThreads that all began as close to the
   * same time as possible and have all completed.
   *
   * @param <T>      the result type of the SyncThread
   * @param num      the number of concurrent threads to execute
   * @param callable the thing to call
   * @return the completed SyncThreads
   * @throws InterruptedException if we're interrupted during join
   */
  public static <T> Collection<SyncThread<T>> getCompletedThreads(
          int num, Callable<T> callable) throws InterruptedException {
    Collection<SyncThread<T>> rv = new ArrayList<>(num);

    CyclicBarrier barrier = new CyclicBarrier(num);
    for (int i = 0; i < num; i++) {
      SyncThread<T> syncThread = new SyncThread<>(barrier, callable);
      syncThread.start();
      rv.add(syncThread);
    }

    for (SyncThread<T> t : rv) {
      t.join();
    }

    return rv;
  }

  /**
   * Get the distinct result count for the given callable at the given
   * concurrency.
   *
   * @param <T>      the type of the callable
   * @param num      the concurrency
   * @param callable the callable to invoke
   * @return the number of distinct (by identity) results found
   * @throws Throwable if an exception occurred in one of the invocations
   */
  public static <T> int getDistinctResultCount(int num, Callable<T> callable)
          throws Throwable {
    IdentityHashMap<T, Object> found = new IdentityHashMap<>();
    Collection<SyncThread<T>> threads = getCompletedThreads(num, callable);
    for (SyncThread<T> s : threads) {
      found.put(s.getResult(), new Object());
    }
    return found.size();
  }
}
