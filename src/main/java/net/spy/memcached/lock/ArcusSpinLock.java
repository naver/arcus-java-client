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
  private static final String clientId = UUID.randomUUID().toString();
  private int latestLeaseTime = DEFAULT_LEASE_TIME_SECONDS;

  private final String lockName;
  private final ArcusClientIF arcusClient;

  public ArcusSpinLock(String lockName, ArcusClientIF client) {
    this.lockName = lockName;
    this.arcusClient = client;
  }

  protected String getClientId() {
    return clientId;
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
    int expireTime = getExpTime(leaseTime, unit);

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
    int expireTime = getExpTime(leaseTime, unit);
    long startTime = System.currentTimeMillis();
    long waitTimeInMillis = 0;
    if (waitTime > 0) {
      waitTimeInMillis = unit.toMillis(waitTime);
    }
    long remainTime;

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

  private int getExpTime(long leaseTime, TimeUnit unit) {
    if (unit == null || unit.compareTo(TimeUnit.SECONDS) < 0) {
      throw new IllegalArgumentException("Time unit must not be null and " +
              "only SECONDS or coarser units are supported.");
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

  protected Boolean tryAcquire(int expireTime) throws InterruptedException {

    Future<CASValue<Object>> futureValue = arcusClient.asyncGets(lockName);

    try {
      CASValue<Object> casValue = futureValue.get();
      if (casValue == null) {
        return handleNoLockExists(expireTime);
      }

      if (isHeldByCurrentThread((String) casValue.getValue())) {
        return handleReentrantLock(expireTime, casValue);
      }

      return false;
    } catch (ExecutionException e) {
      throw new ArcusLockException("Failed to acquire lock", e);
    }
  }

  protected Boolean handleNoLockExists(int expireTime)
          throws ExecutionException, InterruptedException {

    String newValue = getLockValue(1);
    OperationFuture<Boolean> addFuture = (OperationFuture<Boolean>)
            arcusClient.add(lockName, expireTime, newValue);

    Boolean result = addFuture.get();

    if (Boolean.TRUE.equals(result)) {
      latestLeaseTime = expireTime;
      return true;
    }
    return RETRY_LATER;
  }

  protected Boolean handleReentrantLock(int expireTime, CASValue<Object> casVal)
          throws ExecutionException, InterruptedException {

    String value = (String) casVal.getValue();
    int currentCount = parseCurrentCount(value);
    String newValue = getLockValue(currentCount + 1);
    Future<CASResponse> casFuture = arcusClient.asyncCAS(
            lockName, casVal.getCas(), expireTime, newValue);

    CASResponse result = casFuture.get();

    if (result == CASResponse.OK) {
      latestLeaseTime = expireTime;
      return true;
    }
    return RETRY_LATER;
  }

  private int parseCurrentCount(String lockValue) {
    return Integer.parseInt(lockValue.substring(lockValue.lastIndexOf(":") + 1));
  }

  @Override
  public void unlock() {
    CASValue<Object> casVal = arcusClient.gets(lockName);
    validateLockOwnership(casVal);

    String value = (String) casVal.getValue();
    int currentCount = parseCurrentCount(value);

    if (currentCount > 1) {
      decrementLockCount(casVal, currentCount);
    } else {
      deleteLock();
    }
  }

  private void validateLockOwnership(CASValue<Object> casVal) {
    if (casVal == null || !isHeldByCurrentThread((String) casVal.getValue())) {
      throw new IllegalMonitorStateException(
              "Lock not held by current thread. clientId=" + getClientId() +
                      ", threadId=" + getThreadId());
    }
  }

  private void decrementLockCount(CASValue<Object> casVal, int currentCount) {
    String newValue = getLockValue(currentCount - 1);
    try {
      CASResponse casResult = arcusClient.cas(
              lockName, casVal.getCas(), latestLeaseTime, newValue);
      if (casResult != CASResponse.OK) {
        throw new ArcusLockException(
                "Unlock failed: Lock state changed during CAS update. Result=" + casResult);
      }
    } catch (Exception e) {
      throw new ArcusLockException("Unlock failed: " +
              "Unable to update lock with decremented count", e);
    }
  }

  private void deleteLock() {
    try {
      OperationFuture<Boolean> delFuture = (OperationFuture<Boolean>)
              arcusClient.delete(lockName);
      Boolean deleted = delFuture.get();
      if (!Boolean.TRUE.equals(deleted)) {
        throw new ArcusLockException("Unlock failed: Lock deletion returned false");
      }
    } catch (Exception e) {
      throw new ArcusLockException("Unlock failed: Unable to delete lock key", e);
    }
  }

  @Override
  public boolean isLocked() {
    String value = (String) arcusClient.get(lockName);
    return value != null;
  }

  @Override
  public boolean isHeldByCurrentThread() {
    return isHeldByThread(getThreadId());
  }

  private boolean isHeldByCurrentThread(String value) {
    return value != null && value.startsWith(getPrefixLockValue(getThreadId()));
  }

  @Override
  public boolean isHeldByThread(long threadId) {
    String value = (String) arcusClient.get(lockName);
    return value != null && value.startsWith(getPrefixLockValue(threadId));
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }
}
