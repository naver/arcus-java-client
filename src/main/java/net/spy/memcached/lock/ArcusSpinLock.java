package net.spy.memcached.lock;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

public class ArcusSpinLock implements ArcusLock {
  public static final int DEFAULT_INTERNAL_LOCK_LEASE_TIME = 30;
  private static final String clientId = UUID.randomUUID().toString();
  private static final int RETRY_LATER = -1;

  private final String lockName;
  private int internalLockLeaseTime = DEFAULT_INTERNAL_LOCK_LEASE_TIME;

  private final ArcusClient arcusClient;

  public ArcusSpinLock(String lockName, ArcusClient client) {
    this.lockName = lockName;
    this.arcusClient = client;
  }

  protected String getClientId() {
    return clientId;
  }

  private long getCurrentThreadId() {
    return Thread.currentThread().getId();
  }

  private String getPrefixLockValue() {
    return getClientId() + ":" + getCurrentThreadId() + ":";
  }

  private String getLockValue(int count) {
    return getPrefixLockValue() + count;
  }

  // Simple BackOffPolicy
  private static final class BackOffPolicy {
    private int current = 1;

    public long getNextSleepPeriod() {
      int sleep = current;
      int max = 256;
      current = Math.min(current * 2, max);
      return sleep;
    }
  }

  // Helper method to attempt lock acquisition on the server
  private Integer tryAcquire(long leaseTime, TimeUnit unit)
          throws InterruptedException, ExecutionException {
    String value = (String) arcusClient.get(lockName);
    if (unit == null) {
      unit = TimeUnit.SECONDS;
    }
    if (leaseTime > 0) {
      internalLockLeaseTime = (int) unit.toSeconds(leaseTime);
    }
    if (value == null) {
      // No lock exists: try to add a new lock
      String newValue = getLockValue(1);
      OperationFuture<Boolean> addFuture =
              arcusClient.add(lockName, internalLockLeaseTime, newValue);
      OperationStatus status = addFuture.getStatus();
      if (status != null && status.getStatusCode() == StatusCode.SUCCESS) {
        // Lock acquired successfully
        return null;
      } else if (status != null && status.getStatusCode() == StatusCode.ERR_NOT_STORED) {
        // Lock already created during race condition
        try {
          CollectionFuture<CollectionAttributes> attrFuture = arcusClient.asyncGetAttr(lockName);
          CollectionAttributes attrs = attrFuture.get();
          if (attrs != null) {
            return attrs.getExpireTime();
          }
        } catch (Exception e) {
          return RETRY_LATER;
        }
      }
    } else if (isHeldByCurrentThread(value)) {
      // Lock is already held by the current thread: attempt reentrancy via CAS
      int currentCount;
      try {
        currentCount = Integer.parseInt(value.substring(value.lastIndexOf(":") + 1));
      } catch (NumberFormatException e) {
        throw new IllegalStateException("Lock value format error: " + value, e);
      }
      String newValue = getLockValue(currentCount + 1);
      CASValue<Object> casVal = arcusClient.gets(lockName);
      if (casVal != null && value.equals(casVal.getValue())) {
        OperationFuture<CASResponse> casFuture =
                arcusClient.asyncCAS(lockName, casVal.getCas(), internalLockLeaseTime, newValue, arcusClient.getTranscoder());
        if (casFuture.get() == CASResponse.OK) {
          // Reentrant lock acquired successfully
          return null;
        }
      }
    } else {
      // Lock exists and held by another thread: attempt to retrieve expire time
      try {
        CollectionFuture<CollectionAttributes> attrFuture = arcusClient.asyncGetAttr(lockName);
        CollectionAttributes attrs = attrFuture.get();
        if (attrs != null) {
          return attrs.getExpireTime();
        }
      } catch (Exception e) {
        return RETRY_LATER;
      }
    }
    // All other failure cases
    return RETRY_LATER;
  }

  @Override
  public void lock() {
    try {
      lockInterruptibly(-1, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void lock(long leaseTime, TimeUnit unit) {
    try {
      lockInterruptibly(leaseTime, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    try {
      lockInterruptibly(-1, null);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void lockInterruptibly(long leaseTime, TimeUnit unit)
          throws InterruptedException, ExecutionException {
    BackOffPolicy backOffPolicy = new BackOffPolicy();
    while (true) {
      Integer ttl = tryAcquire(leaseTime, unit);
      if (ttl == null) {
        return;
      }
      Thread.sleep(backOffPolicy.getNextSleepPeriod());
    }
  }

  @Override
  public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit)
          throws InterruptedException, ExecutionException {
    final long deadline = System.nanoTime() + unit.toNanos(waitTime);
    BackOffPolicy backOffPolicy = new BackOffPolicy();
    while (true) {
      Integer ttl = tryAcquire(leaseTime, unit);
      if (ttl == null) {
        return true;
      }
      if (waitTime == 0 || System.nanoTime() > deadline) {
        break;
      }
      Thread.sleep(backOffPolicy.getNextSleepPeriod());
    }
    return false;
  }

  @Override
  public boolean tryLock() {
    try {
      return tryLock(0, internalLockLeaseTime, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
    try {
      return tryLock(waitTime, internalLockLeaseTime, unit);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unlock() {
    String value = (String) arcusClient.get(lockName);
    if (!isHeldByCurrentThread(value)) {
      throw new IllegalMonitorStateException("Lock not held by this client: " + getClientId() +
              " thread-id: " + getCurrentThreadId());
    }
    int currentCount;
    try {
      currentCount = Integer.parseInt(value.substring(value.lastIndexOf(":") + 1));
    } catch (NumberFormatException e) {
      throw new IllegalStateException("Lock value format error: " + value, e);
    }
    if (currentCount > 1) {
      // Reentrant unlock: decrement count by 1
      String newValue = getLockValue(currentCount - 1);
      CASValue<Object> casVal = arcusClient.gets(lockName);
      if (casVal != null && value.equals(casVal.getValue())) {
        OperationFuture<CASResponse> casFuture =
                arcusClient.asyncCAS(lockName, casVal.getCas(), internalLockLeaseTime, newValue, arcusClient.getTranscoder());
        try {
          casFuture.get();
        } catch (Exception e) {
          throw new RuntimeException("Failed to decrement lock count", e);
        }
      }
    } else {
      // Final unlock: delete the lock
      OperationFuture<Boolean> delFuture = arcusClient.delete(lockName);
      try {
        delFuture.get();
      } catch (Exception e) {
        throw new RuntimeException("Failed to delete lock", e);
      }
    }
  }

  @Override
  public boolean isLocked() {
    String value = (String) arcusClient.get(lockName);
    return value != null;
  }

  @Override
  public boolean isHeldByCurrentThread() {
    String value = (String) arcusClient.get(lockName);
    return value != null && value.startsWith(getPrefixLockValue());
  }

  private boolean isHeldByCurrentThread(String value) {
    return value != null && value.startsWith(getPrefixLockValue());
  }

  @Override
  public boolean isHeldByThread(long threadId) {
    String value = (String) arcusClient.get(lockName);
    return value != null && value.startsWith(getPrefixLockValue());
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }
}
