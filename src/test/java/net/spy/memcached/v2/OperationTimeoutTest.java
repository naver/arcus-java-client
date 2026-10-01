package net.spy.memcached.v2;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.OperationTimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationTimeoutTest {

  private ArcusClient client;
  private AsyncArcusCommands<Object> commands;

  @BeforeEach
  void setUp() throws Exception {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder()
        .setOpTimeout(1L)
        .setFailureMode(FailureMode.Retry);

    client = new ArcusClient(
        cfb.build(),
        Collections.singletonList(new InetSocketAddress("127.0.0.1", 54321))
    );

    commands = client.asyncCommands();
  }

  @AfterEach
  void tearDown() {
    if (client != null) {
      client.shutdown(0, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  void usesDefaultOperationTimeout() {
    // given

    // when
    ArcusFuture<Object> future = commands.get("key");

    // then
    ExecutionException exception = assertThrows(
        ExecutionException.class,
        () -> future.get(300L, TimeUnit.MILLISECONDS));

    assertInstanceOf(OperationTimeoutException.class, exception.getCause());
    assertFalse(future.isCancelled());
    assertTrue(exception.getCause().getMessage().contains("after 1 milliseconds"));
  }

  @Test
  void overridesOperationTimeout() {
    // given
    AsyncArcusCommands<Object> scoped =
        commands.withOperationTimeout(2L, TimeUnit.MILLISECONDS);

    // when
    ArcusFuture<Object> scopedFuture = scoped.get("scoped-key");
    ArcusFuture<Object> originalFuture = commands.get("original-key");

    // then
    ExecutionException scopedException = assertThrows(
        ExecutionException.class,
        () -> scopedFuture.get(300L, TimeUnit.MILLISECONDS));

    assertInstanceOf(OperationTimeoutException.class, scopedException.getCause());
    assertTrue(scopedException.getCause().getMessage().contains("after 2 milliseconds"));

    ExecutionException originalException = assertThrows(
        ExecutionException.class,
        () -> originalFuture.get(300L, TimeUnit.MILLISECONDS));

    assertInstanceOf(OperationTimeoutException.class, originalException.getCause());
    assertTrue(originalException.getCause().getMessage().contains("after 1 milliseconds"));
  }

  @Test
  void timeOutWithoutWaitingFuture() throws Exception {
    // given
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();

    // when
    commands.get("key")
        .whenComplete((value, error) -> {
          failure.set(error);
          latch.countDown();
        });

    // then
    assertTrue(latch.await(700L, TimeUnit.MILLISECONDS));
    assertInstanceOf(OperationTimeoutException.class, failure.get());
  }

  @Test
  void rejectsNullTimeUnit() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> commands.withOperationTimeout(1L, null)
    );

    assertEquals("TimeUnit cannot be null", exception.getMessage());
  }

  @Test
  void rejectsZeroTimeout() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> commands.withOperationTimeout(0L, TimeUnit.MILLISECONDS));

    assertEquals(
        "Operation timeout must be greater than 0 milliseconds", exception.getMessage()
    );
  }

  @Test
  void rejectsNegativeTimeout() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> commands.withOperationTimeout(-1, TimeUnit.MILLISECONDS)
    );

    assertEquals(
        "Operation timeout must be greater than 0 milliseconds", exception.getMessage()
    );
  }

  @Test
  void truncatesTimeoutToMilliseconds() {
    // given
    AsyncArcusCommands<Object> scoped =
        commands.withOperationTimeout(1999L, TimeUnit.MICROSECONDS);

    // when
    ArcusFuture<Object> future = scoped.get("key");

    // then
    ExecutionException exception = assertThrows(
        ExecutionException.class,
        () -> future.get(300L, TimeUnit.MILLISECONDS)
    );

    assertInstanceOf(OperationTimeoutException.class, exception.getCause());
    assertTrue(exception.getCause().getMessage().contains("after 1 milliseconds"));
  }

}
