package net.spy.memcached.lock;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import net.spy.memcached.ArcusClientIF;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;

public class ArcusSpinLock implements ArcusLock {
  private static final int DEFAULT_LEASE_TIME_SECONDS = 30;
  private static final int MAX_LEASE_TIME_SECONDS = 30 * 24 * 60 * 60;
  private static final Boolean RETRY_LATER = false;
  private static final String CLIENT_ID = UUID.randomUUID().toString();
  private int latestLeaseTime = DEFAULT_LEASE_TIME_SECONDS;

  private static final String ERROR_LOCK_ACQUIRE_INTERRUPTED =
          "Failed to acquire lock: Thread interrupted while waiting for value";
  private static final String ERROR_LOCK_ACQUIRE_EXCEPTION =
          "Failed to acquire lock: Exception thrown while waiting for value";
  private static final String ERROR_VERIFY_LOCK_STATE =
          "Failed to verify lock state after timeout";
  private static final String ERROR_LOCK_UNEXPECTED_EXCEPTION =
          "Failed to process lock operation: Unexpected exception occurred";
  private static final String ERROR_UNLOCK_INTERRUPTED =
          "Failed to release lock: Thread interrupted while waiting for value";
  private static final String ERROR_UNLOCK_EXCEPTION =
          "Failed to release lock: Exception thrown while waiting for value";
  private static final String ERROR_UNLOCK_TIMEOUT =
          "Failed to release lock: Operation timeout";
  private static final String ERROR_UNLOCK_DELETION =
          "Failed to release lock: Delete operation returned false";
  private static final String ERROR_UNLOCK_CAS_UPDATE =
          "Failed to release lock: CAS update failed, result=";

  private final String lockName;
  private final ArcusClientIF arcusClient;

  public ArcusSpinLock(String lockName, ArcusClientIF client) {
    this.lockName = lockName;
    this.arcusClient = client;
  }

  protected String getClientId() {
    return CLIENT_ID;
  }

  private long getThreadId() {
    return Thread.currentThread().getId();
  }

  private String getPrefixLockValue(long threadId) {
    return getClientId() + ":" + threadId + ":";
  }

  private String getLockValue(int count) {
    return getPrefixLockValue(getThreadId()) + count;
  }

  private static final class BackOffPolicy {
    private int current = 1;

    public synchronized long getNextSleepPeriod() {
      int max = 256;
      if (current >= max) {
        return max;
      }
      int sleep = current;
      current = Math.min(current * 2, max);
      return sleep;
    }
  }

  @Override
  public void lock() {
    lock(-1, TimeUnit.SECONDS);
  }

  @Override
  public void lock(long leaseTime, TimeUnit unit) {
    boolean interrupted = false;
    while (true) {
      try {
        lockInterruptibly(leaseTime, unit);
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lockInterruptibly(-1, TimeUnit.SECONDS);
  }

  @Override
  public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
    int expireTime = getExpireTime(leaseTime, unit);

    Boolean isAcquired;
    try {
      isAcquired = tryAcquire(expireTime);
      if (isAcquired) {
        return;
      }
    } catch (OperationTimeoutException e) {
      // Ignore the timeout exception and retry
    }

    BackOffPolicy backOffPolicy = new BackOffPolicy();
    while (true) {
      Thread.sleep(backOffPolicy.getNextSleepPeriod());

      try {
        isAcquired = tryAcquire(expireTime);
        if (isAcquired) {
          return;
        }
      } catch (OperationTimeoutException e) {
        // Ignore the timeout exception and retry
      }
    }
  }

  @Override
  public boolean tryLock() {
    try {
      return tryLock(-1, -1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  @Override
  public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
    return tryLock(waitTime, -1, unit);
  }

  @Override
  public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
    int expireTime = getExpireTime(leaseTime, unit);
    long startTime = System.currentTimeMillis();
    long remainTime;
    long waitTimeInMillis = 0;
    if (waitTime > 0) {
      waitTimeInMillis = unit.toMillis(waitTime);
    }

    Boolean isAcquired;
    try {
      isAcquired = tryAcquire(expireTime);
    } catch (OperationTimeoutException e) {
      return false;
    }
    if (isAcquired) {
      return true;
    }
    if (waitTime <= 0) {
      // Lock is already held and no wait time is allowed
      return false;
    }

    BackOffPolicy backOffPolicy = new BackOffPolicy();
    while (true) {
      remainTime = getRemainTime(startTime, waitTimeInMillis);
      if (remainTime <= 0) {
        return false;
      }

      long sleepTime = Math.min(backOffPolicy.getNextSleepPeriod(), remainTime);
      Thread.sleep(sleepTime);

      remainTime = getRemainTime(startTime, waitTimeInMillis);
      if (remainTime <= 0) {
        return false;
      }

      try {
        isAcquired = tryAcquire(expireTime);
      } catch (OperationTimeoutException e) {
        return false;
      }

      if (isAcquired) {
        return true;
      }
    }
  }

  private long getRemainTime(long startTime, long totalWaitTime) {
    long elapsed = System.currentTimeMillis() - startTime;
    return totalWaitTime - elapsed;
  }

  private int getExpireTime(long leaseTime, TimeUnit unit) {
    if (unit == null || unit.compareTo(TimeUnit.SECONDS) < 0) {
      throw new IllegalArgumentException("Time unit must not be null and " +
              "must be SECONDS or a coarser unit.");
    }

    if (leaseTime <= 0) {
      return DEFAULT_LEASE_TIME_SECONDS;
    }

    long seconds = unit.toSeconds(leaseTime);
    if (seconds > MAX_LEASE_TIME_SECONDS) {
      throw new IllegalArgumentException(
              "The leaseTime value (" + leaseTime + " " + unit + ") " +
                      "exceeds the maximum allowed lease time of " +
                      MAX_LEASE_TIME_SECONDS + " seconds (30 days).");
    }

    return (int) seconds;
  }

  protected Boolean tryAcquire(int expireTime) throws OperationTimeoutException {
    Future<CASValue<Object>> future = arcusClient.asyncGets(lockName);

    try {
      CASValue<Object> casValue = future.get();
      if (casValue == null) {
        return handleNoLockExists(expireTime);
      }

      if (isHeldByCurrentThread((String) casValue.getValue())) {
        return handleReentrantLock(expireTime, casValue);
      }

      return false;
    } catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new ArcusLockException(ERROR_LOCK_ACQUIRE_INTERRUPTED, e);
    } catch (ExecutionException e) {
      future.cancel(true);
      throw new ArcusLockException(ERROR_LOCK_ACQUIRE_EXCEPTION, e);
    }
  }

  protected Boolean handleNoLockExists(int expireTime) {
    String newValue = getLockValue(1);
    OperationFuture<Boolean> addFuture = (OperationFuture<Boolean>)
            arcusClient.add(lockName, expireTime, newValue);

    try {
      Boolean result = addFuture.get();
      if (Boolean.TRUE.equals(result)) {
        latestLeaseTime = expireTime;
        return true;
      }

      return RETRY_LATER;
    } catch (Exception e) {
      return handleLockException(e, addFuture, expireTime);
    }
  }

  protected Boolean handleReentrantLock(int expireTime, CASValue<Object> casValue) {
    String value = (String) casValue.getValue();
    int currentCount = parseCurrentCount(value);
    String newValue = getLockValue(currentCount + 1);
    Future<CASResponse> casFuture = arcusClient.asyncCAS(
            lockName, casValue.getCas(), expireTime, newValue);

    try {
      CASResponse result = casFuture.get();
      if (result == CASResponse.OK) {
        latestLeaseTime = expireTime;
        return true;
      }

      return RETRY_LATER;
    } catch (Exception e) {
      return handleReentrantLockException(e, casFuture, currentCount, expireTime);
    }
  }

  private Boolean handleLockException(Exception e, Future<?> future, int expireTime) {
    future.cancel(true);

    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      if (verifyLockAcquisition(expireTime)) {
        return true;
      }
      throw new ArcusLockException(ERROR_LOCK_ACQUIRE_INTERRUPTED, e);
    }

    if (e instanceof ExecutionException) {
      if (verifyLockAcquisition(expireTime)) {
        return true;
      }
      throw new ArcusLockException(ERROR_LOCK_ACQUIRE_EXCEPTION, e);
    }

    if (e instanceof OperationTimeoutException) {
      if (verifyLockAcquisition(expireTime)) {
        return true;
      }
      return RETRY_LATER;
    }

    throw new ArcusLockException(ERROR_LOCK_UNEXPECTED_EXCEPTION, e);
  }

  private boolean verifyLockAcquisition(int expireTime) {
    try {
      Object lock = arcusClient.get(lockName);
      if (lock != null && isHeldByCurrentThread()) {
        latestLeaseTime = expireTime;
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new ArcusLockException(ERROR_VERIFY_LOCK_STATE, e);
    }
  }

  private Boolean handleReentrantLockException(Exception e, Future<?> future,
                                               int currentCount, int expireTime) {
    future.cancel(true);

    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      if (verifyReentrantLockAcquisition(currentCount, expireTime)) {
        return true;
      }
      throw new ArcusLockException(ERROR_LOCK_ACQUIRE_INTERRUPTED, e);
    }

    if (e instanceof ExecutionException) {
      if (verifyReentrantLockAcquisition(currentCount, expireTime)) {
        return true;
      }
      throw new ArcusLockException(ERROR_LOCK_ACQUIRE_EXCEPTION, e);
    }

    if (e instanceof OperationTimeoutException) {
      if (verifyReentrantLockAcquisition(currentCount, expireTime)) {
        return true;
      }
      return RETRY_LATER;
    }

    throw new ArcusLockException(ERROR_LOCK_UNEXPECTED_EXCEPTION, e);
  }

  private boolean verifyReentrantLockAcquisition(int currentCount, int expireTime) {
    try {
      Object lock = arcusClient.get(lockName);
      if (lock != null && isHeldByCurrentThread()) {
        int lockCount = parseCurrentCount((String) lock);
        if (lockCount == (currentCount + 1)) {
          // Consider reentry successful if the count has increased
          latestLeaseTime = expireTime;
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      throw new ArcusLockException(ERROR_VERIFY_LOCK_STATE, e);
    }
  }

  private int parseCurrentCount(String lockValue) {
    return Integer.parseInt(lockValue.substring(lockValue.lastIndexOf(":") + 1));
  }

  @Override
  public void unlock() {
    try {
      CASValue<Object> casValue = arcusClient.gets(lockName);
      validateLockOwnership(casValue);

      String value = (String) casValue.getValue();
      int currentCount = parseCurrentCount(value);

      if (currentCount == 1) {
        deleteLock();
        return;
      }

      decrementLockCount(casValue, currentCount);
    } catch (OperationTimeoutException e) {
      throw new ArcusLockException(ERROR_UNLOCK_TIMEOUT, e);
    }
  }

  private void validateLockOwnership(CASValue<Object> casValue) {
    if (casValue == null || !isHeldByCurrentThread((String) casValue.getValue())) {
      throw new IllegalMonitorStateException(
              "Lock is not held by the current thread. clientId=" + getClientId() +
                      ", threadId=" + getThreadId());
    }
  }

  private void deleteLock() {
    OperationFuture<Boolean> delFuture = (OperationFuture<Boolean>) arcusClient.delete(lockName);

    try {
      Boolean deleted = delFuture.get();
      if (!Boolean.TRUE.equals(deleted)) {
        throw new ArcusLockException(ERROR_UNLOCK_DELETION);
      }
    } catch (Exception e) {
      handleUnlockException(e, delFuture);
    }
  }

  private void decrementLockCount(CASValue<Object> casValue, int currentCount) {
    String newValue = getLockValue(currentCount - 1);

    try {
      CASResponse casResult = arcusClient.cas(
              lockName, casValue.getCas(), latestLeaseTime, newValue);
      if (casResult != CASResponse.OK) {
        throw new ArcusLockException(ERROR_UNLOCK_CAS_UPDATE + casResult);
      }
    } catch (Exception e) {
      if (verifyReentrantLockReleased(currentCount)) {
        return;
      }
      throw new ArcusLockException(ERROR_UNLOCK_EXCEPTION, e);
    }
  }

  private void handleUnlockException(Exception e, Future<?> future) {
    future.cancel(true);

    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      if (verifyLockReleased()) {
        return;
      }
      throw new ArcusLockException(ERROR_UNLOCK_INTERRUPTED, e);
    }

    if (verifyLockReleased()) {
      return;
    }

    throw new ArcusLockException(ERROR_UNLOCK_EXCEPTION, e);
  }

  private boolean verifyLockReleased() {
    try {
      Object lock = arcusClient.get(lockName);
      // Consider it success if the lock has already been deleted
      return lock == null;
    } catch (Exception e) {
      throw new ArcusLockException(ERROR_VERIFY_LOCK_STATE, e);
    }
  }

  private boolean verifyReentrantLockReleased(int currentCount) {
    try {
      Object lock = arcusClient.get(lockName);
      if (lock != null && isHeldByCurrentThread((String) lock)) {
        int lockCount = parseCurrentCount((String) lock);
        // Consider it success if the count has decreased
        return lockCount == currentCount - 1;
      }
      return false;
    } catch (Exception e) {
      throw new ArcusLockException(ERROR_VERIFY_LOCK_STATE, e);
    }
  }

  @Override
  public boolean isLocked() {
    Object lock = arcusClient.get(lockName);
    return lock != null;
  }

  @Override
  public boolean isHeldByCurrentThread() {
    return isHeldByThread(getThreadId());
  }

  private boolean isHeldByCurrentThread(String lock) {
    return lock != null && lock.startsWith(getPrefixLockValue(getThreadId()));
  }

  @Override
  public boolean isHeldByThread(long threadId) {
    Object lock = arcusClient.get(lockName);
    return lock != null && ((String) lock).startsWith(getPrefixLockValue(threadId));
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }
}
