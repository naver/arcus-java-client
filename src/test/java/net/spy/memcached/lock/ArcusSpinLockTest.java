package net.spy.memcached.lock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.collection.BaseIntegrationTest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ArcusSpinLockTest extends BaseIntegrationTest {
  private static final String LOCK_NAME = "testLock";

  static class TestableArcusSpinLock extends ArcusSpinLock {
    private int capturedExpiration;

    public TestableArcusSpinLock(String lockName, ArcusClient client) {
      super(lockName, client);
    }

    @Override
    protected Boolean tryAcquire(int expireTime) {
      this.capturedExpiration = expireTime;
      return super.tryAcquire(expireTime);
    }

    public int getCapturedExpiration() {
      return capturedExpiration;
    }
  }


  @Test
  void testLockAcquireAndRelease() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);

    lock.lock();
    assertTrue(lock.isHeldByCurrentThread());
    lock.unlock();
    assertFalse(lock.isLocked());
  }

  @Test
  void testTryLockSuccess() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);

    assertTrue(lock.tryLock());
    assertTrue(lock.isLocked());
    assertTrue(lock.isHeldByCurrentThread());
    lock.unlock();
    assertFalse(lock.isLocked());
  }

  @Test
  void testLockInterruptiblyInterrupted() throws Exception {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);

    lock.lock();
    CountDownLatch latch = new CountDownLatch(1);

    Thread t = new Thread(() -> {
      try {
        latch.countDown();
        lock.lockInterruptibly();
        fail("Should be interrupted");
      } catch (InterruptedException e) {
        // expected
      }
    });
    t.start();
    latch.await();
    Thread.sleep(100); // Give the thread time to enter the lock waiting state
    t.interrupt();
    t.join();

    lock.unlock();
  }

  @Test
  void testLockInterruptiblyWithCustomLeaseTime() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    lock.lock(10, TimeUnit.SECONDS);
    AtomicBoolean wasInterrupted = new AtomicBoolean(false);

    CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      try {
        latch.countDown();
        lock.lockInterruptibly(20, TimeUnit.SECONDS);
        fail("Should be interrupted");
      } catch (InterruptedException e) {
        wasInterrupted.set(true);
      }
    });
    t.start();
    latch.await();
    Thread.sleep(500); // Give the thread time to enter the lock waiting state
    t.interrupt();
    t.join();

    assertTrue(wasInterrupted.get());
    lock.unlock();
  }

  @Test
  void testLockReentrancy() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    lock.lock();
    lock.lock();
    lock.unlock();
    assertTrue(lock.isLocked());
    Thread t = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, mc);
      assertFalse(lock1.tryLock());
    });
    t.start();
    t.join();
    lock.unlock();
    assertFalse(lock.isLocked());
    Thread t2 = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, mc);
      lock1.lock();
      assertTrue(lock1.isLocked());
      lock1.unlock();
    });
    t2.start();
    t2.join();
  }

  @Test
  void testTryLockReentrancy() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    assertTrue(lock.tryLock());
    assertTrue(lock.tryLock());
    lock.unlock();
    assertTrue(lock.isLocked());
    Thread t = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, mc);
      assertFalse(lock1.tryLock());
    });
    t.start();
    t.join();
    lock.unlock();
    assertFalse(lock.isLocked());
    Thread t2 = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, mc);
      assertTrue(lock1.tryLock());
      lock1.unlock();
    });
    t2.start();
    t2.join();
  }

  @Test
  void testUnlockWithoutLock() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    assertThrows(IllegalMonitorStateException.class, lock::unlock);
  }

  @Test
  void testUnlockByOtherThreadFailed() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    lock.lock();

    Thread t = new Thread(() -> assertThrows(IllegalMonitorStateException.class, lock::unlock));
    t.start();
    t.join();
    lock.unlock();
  }

  @Test
  void testLockWaitsWhenHeldByOtherThread() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    CountDownLatch latch = new CountDownLatch(1);
    lock.lock();

    Thread t = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, mc);
      lock1.lock();
      latch.countDown();
      lock1.unlock();
    });
    t.start();
    lock.unlock();

    assertTrue(latch.await(2, TimeUnit.SECONDS));
    t.join();
  }

  @Test
  void testTryLockFailed() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    lock.lock();

    AtomicBoolean tryLockResult = new AtomicBoolean(true);
    Thread t = new Thread(() -> {
      try {
        tryLockResult.set(lock.tryLock(1, 1, TimeUnit.SECONDS));
      } catch (Exception e) {
        tryLockResult.set(true);
      }
    });
    t.start();
    t.join();

    assertFalse(tryLockResult.get());
    lock.unlock();
  }

  @Test
  void testLockTransferAfterUnlock() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    lock.lock();
    AtomicBoolean nextLockAcquired = new AtomicBoolean(false);

    Thread t = new Thread(() -> {
      lock.lock();
      nextLockAcquired.set(true);
      lock.unlock();
    });
    t.start();
    Thread.sleep(1000);
    lock.unlock();
    t.join();

    assertTrue(nextLockAcquired.get());
  }

  @Test
  void testLockReleasedAfterThreadTermination() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    // Thread terminates without releasing the lock
    Thread t = new Thread(lock::lock);
    t.start();
    t.join();
    assertTrue(lock.isLocked());

    AtomicBoolean acquired = new AtomicBoolean();
    Thread t2 = new Thread(() -> {
      ArcusSpinLock lock2 = new ArcusSpinLock(LOCK_NAME, mc);
      acquired.set(lock2.tryLock());
      if (acquired.get()) {
        lock2.unlock();
      }
    });
    t2.start();
    t2.join();

    // Lock acquisition should fail as the lock is still held
    assertFalse(acquired.get());
  }

  @Test
  void testNegativeLeaseTime() {
    TestableArcusSpinLock lock = new TestableArcusSpinLock(LOCK_NAME, mc);
    lock.lock(-10, TimeUnit.SECONDS);

    assertEquals(30, lock.getCapturedExpiration());
    assertTrue(lock.isLocked());
    lock.unlock();
  }

  @Test
  void testZeroLeaseTime() {
    TestableArcusSpinLock lock = new TestableArcusSpinLock(LOCK_NAME, mc);
    lock.lock(0, TimeUnit.SECONDS);

    assertEquals(30, lock.getCapturedExpiration());
    assertTrue(lock.isLocked());
    lock.unlock();
  }

  @Test
  void testNullTimeUnitFailed() {
    TestableArcusSpinLock lock = new TestableArcusSpinLock(LOCK_NAME, mc);

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> lock.lock(10, null));
    assertTrue(ex.getMessage().contains("must not be null"));
  }

  @Test
  void testLockWithDifferentTimeUnits() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);

    lock.lock(60, TimeUnit.SECONDS);
    lock.unlock();

    lock.lock(1, TimeUnit.MINUTES);
    lock.unlock();

    lock.lock(1, TimeUnit.HOURS);
    lock.unlock();

    lock.lock(1, TimeUnit.DAYS);
    lock.unlock();
  }

  @Test
  void testUnsupportedTimeUnit() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    assertThrows(IllegalArgumentException.class, () -> lock.lock(100, TimeUnit.MILLISECONDS));
    assertThrows(IllegalArgumentException.class, () -> lock.lock(100, TimeUnit.MICROSECONDS));
    assertThrows(IllegalArgumentException.class, () -> lock.lock(100, TimeUnit.NANOSECONDS));
  }

  @Test
  void testLargeLeaseTimeConversion() {
    TestableArcusSpinLock lock = new TestableArcusSpinLock(LOCK_NAME, mc);
    lock.lock(30, TimeUnit.DAYS);

    assertEquals(30 * 24 * 60 * 60, lock.getCapturedExpiration());
    assertTrue(lock.isLocked());
    lock.unlock();
  }

  @Test
  void testLeaseTimeBelow30Days() {
    TestableArcusSpinLock lock = new TestableArcusSpinLock(LOCK_NAME, mc);

    lock.lock(29, TimeUnit.DAYS);
    assertTrue(lock.isLocked());
    lock.unlock();
  }

  @Test
  void testLeaseTime30Days() {
    TestableArcusSpinLock lock = new TestableArcusSpinLock(LOCK_NAME, mc);

    lock.lock(30, TimeUnit.DAYS);
    assertTrue(lock.isLocked());
    lock.unlock();
  }

  @Test
  void testLeaseTimeAbove30DaysFailed() {
    TestableArcusSpinLock lock = new TestableArcusSpinLock(LOCK_NAME, mc);

    assertThrows(IllegalArgumentException.class,
            () -> lock.lock(31, TimeUnit.DAYS));
  }

  @Test
  void testExtremelyLargeLeaseTime() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    assertThrows(IllegalArgumentException.class,
            () -> lock.lock(Integer.MAX_VALUE, TimeUnit.MINUTES));
  }

  @Test
  void testTryLockFailWithNegativeWaitTime() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    lock.lock();

    long start = System.currentTimeMillis();
    Thread t = new Thread(() -> {
      try {
        assertFalse(lock.tryLock(-1, 10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        fail("Should not be interrupted");
      }
    });
    t.start();
    t.join();
    long end = System.currentTimeMillis();

    // Negative waitTime should immediately return false if lock is not acquired
    long elapsedTime = end - start;
    assertTrue(elapsedTime <= 100);

    lock.unlock();
  }

  @Test
  void testTryLockSuccessWithNegativeWaitTime() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    lock.lock();
    lock.unlock();

    long start = System.currentTimeMillis();
    Thread t = new Thread(() -> {
      try {
        assertTrue(lock.tryLock(-1, 10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        fail("Should not be interrupted");
      }
      lock.unlock();
    });
    t.start();
    t.join();
    long end = System.currentTimeMillis();

    // Negative wait time should return immediately with true if lock is acquired
    long elapsedTime = end - start;
    assertTrue(elapsedTime <= 100);
  }

  @Test
  void testConcurrentLockCompetition() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, mc);
    final int THREADS = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(THREADS);
    AtomicBoolean[] acquired = new AtomicBoolean[THREADS];

    for (int i = 0; i < THREADS; i++) {
      acquired[i] = new AtomicBoolean(false);
      final int index = i;

      new Thread(() -> {
        try {
          startLatch.await(); // Wait for all threads to start simultaneously

          if (lock.tryLock(4, 2, TimeUnit.SECONDS)) {
            acquired[index].set(true);
            Thread.sleep(1000);
            lock.unlock();
          }
        } catch (Exception e) {
          fail("Exception: " + e.getMessage());
        } finally {
          completionLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown(); // Start all threads
    completionLatch.await(); // Wait for all threads to complete

    int acquiredCount = 0;
    for (AtomicBoolean success : acquired) {
      if (success.get()) {
        acquiredCount++;
      }
    }
    assertEquals(4, acquiredCount);
  }
}
