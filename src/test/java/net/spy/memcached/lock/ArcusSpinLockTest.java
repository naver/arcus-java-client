package net.spy.memcached.lock;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ClientBaseCase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ArcusSpinLock 분산 락 동작 검증 테스트 (1차 PR)
 * - ClientBaseCase 상속으로 실제 Arcus 환경에서 동작 확인
 */
class ArcusSpinLockTest extends ClientBaseCase {
  private static final String LOCK_NAME = "testLock";

  @Test
  void testLockAcquireAndRelease() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
    lock.lock();
    assertTrue(lock.isHeldByCurrentThread());
    lock.unlock();
    assertFalse(lock.isLocked());
  }

  @Test
  void testTryLockSuccess() {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
    assertTrue(lock.tryLock());
    assertTrue(lock.isLocked());
    assertTrue(lock.isHeldByCurrentThread());
    lock.unlock();
    assertFalse(lock.isLocked());
  }

  @Test
  void testLockInterruptiblyInterrupted() throws Exception {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
    lock.lock(); // 메인 스레드가 락을 점유

    CountDownLatch ready = new CountDownLatch(1);

    Thread t = new Thread(() -> {
      try {
        ready.countDown(); // lockInterruptibly() 진입 직전 신호
        lock.lockInterruptibly(); // 여기서 대기
        fail("Should be interrupted");
      } catch (InterruptedException e) {
        // expected
      }
    });
    t.start();

    ready.await(); // t가 lockInterruptibly() 진입할 때까지 대기
    t.interrupt(); // 대기 중인 t를 interrupt

    t.join();
    lock.unlock();
  }

  @Test
  void testLockReentrancy() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
    // 같은 스레드에서 2번 lock (재진입)
    lock.lock();
    lock.lock();
    // 1번 unlock 후 여전히 다른 스레드는 lock 불가
    lock.unlock();
    assertTrue(lock.isLocked());
    Thread t = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
      assertFalse(lock1.tryLock());
    });
    t.start();
    t.join();
    // 마지막 unlock 후에는 다른 스레드가 lock 가능
    lock.unlock();
    assertFalse(lock.isLocked());
    Thread t2 = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
      lock1.lock();
      assertTrue(lock1.isLocked());
      lock1.unlock();
    });
    t2.start();
    t2.join();
  }

  @Test
  void testTryLockReentrancy() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
    // 같은 스레드에서 2번 lock (재진입)
    assertTrue(lock.tryLock());
    assertTrue(lock.tryLock());
    // 1번 unlock 후 여전히 다른 스레드는 lock 불가
    lock.unlock();
    assertTrue(lock.isLocked());
    Thread t = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
      assertFalse(lock1.tryLock());
    });
    t.start();
    t.join();
    // 마지막 unlock 후에는 다른 스레드가 lock 가능
    lock.unlock();
    assertFalse(lock.isLocked());
    Thread t2 = new Thread(() -> {
      ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
      assertTrue(lock1.tryLock());
      lock1.unlock();
    });
    t2.start();
    t2.join();
  }

  // 다른 스레드 해제 시도 → 예외 발생
  @Test
  void testUnlockByOtherThreadFailed() throws InterruptedException {
    ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
    lock.lock();
    Thread t = new Thread(() -> assertThrows(IllegalMonitorStateException.class, lock::unlock));
    t.start();
    t.join();
    lock.unlock();
  }

  // 여러 스레드 경쟁 상황 (단일 인스턴스)
  @Test
  void testLockWaitsWhenHeldByOtherThread() throws InterruptedException {
      ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
      CountDownLatch latch = new CountDownLatch(1);
      lock.lock();
      Thread t = new Thread(() -> {
        ArcusSpinLock lock1 = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
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
      ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
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
      ArcusSpinLock lock = new ArcusSpinLock(LOCK_NAME, (ArcusClient) client);
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
}