package net.spy.memcached.internal;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Additional flexibility for asyncGetBulk
 *
 * <p>
 * This interface is now returned from all asyncGetBulk
 * methods. Unlike {@link #get(long, TimeUnit)},
 * {@link #getSome(long, TimeUnit)} does not throw
 * CheckedOperationTimeoutException, thus allowing retrieval
 * of partial results after timeout occurs. This behavior is
 * especially useful in case of large multi gets.
 * </p>
 *
 * @param <V>
 * @author boris.partensky@gmail.com
 */
public interface BulkFuture<V> extends Future<V> {

  /**
   * @return true if timeout was reached, false otherwise
   */
  boolean isTimeout();

  /**
   * Wait for the operation to complete and return results.
   *
   * If operation could not complete within specified
   * timeout, partial result is returned. Otherwise, the
   * behavior is identical to {@link #get(long, TimeUnit)}
   *
   * @param timeout the maximum time to wait
   * @param unit    the time unit of the timeout argument
   * @return the computed result
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws ExecutionException   if the computation threw an exception
   */
  V getSome(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException;


  int getOpCount();

}
