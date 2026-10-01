package net.spy.memcached.v2;

import java.lang.reflect.Field;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.Operation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;
import org.jmock.lib.concurrent.Synchroniser;

import static net.spy.memcached.ExpectationsUtil.buildExpectations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.jmock.AbstractExpectations.returnValue;

public class ArcusFutureTimeoutTest {

  private static final long OP_TIMEOUT_MILLIS = 300L;
  private static final long TEST_TIMEOUT_SECONDS = 3L;
  private static final String TIMEOUT_CAUSE =
      "by operation timeout after " + OP_TIMEOUT_MILLIS + " milliseconds";

  private Mockery context;
  private Operation operation;
  private MemcachedNode node;
  private AbstractArcusResult<String> result;
  private ArcusFutureImpl<String> future;

  private final CountDownLatch decodingStarted = new CountDownLatch(1);
  private final CountDownLatch releaseDecoding = new CountDownLatch(1);
  private final AtomicReference<String> cancelCause = new AtomicReference<>();

  @BeforeEach
  void setUp() {
    context = new Mockery();
    context.setThreadingPolicy(new Synchroniser());

    operation = context.mock(Operation.class);
    node = context.mock(MemcachedNode.class);

    context.checking(buildExpectations(e -> {
      e.allowing(operation).getAPIType();
      e.will(returnValue(APIType.GET));

      e.allowing(operation).getHandlingNode();
      e.will(returnValue(node));

      e.allowing(operation).hasErrored();
      e.will(returnValue(false));

      e.allowing(operation).isCancelled();
      e.will(returnValue(false));

      e.allowing(operation).getCancelCause();
      e.will(new CustomAction("return the recorded cancel cause") {
        @Override
        public Object invoke(Invocation invocation) {
          return cancelCause.get();
        }
      });

      e.allowing(node).getNodeName();
      e.will(returnValue("test-node"));
    }));

    result = new AbstractArcusResult<>(new AtomicReference<>("value"));
    future = new ArcusFutureImpl<>(result);
    future.setOp(operation);
  }

  @AfterEach
  void tearDown() throws Exception {
    releaseDecoding.countDown();

    ScheduledFuture<?> timeout = scheduledTimeout();
    if (timeout != null) {
      timeout.cancel(true);
    }

    context.assertIsSatisfied();
  }

  @Test
  void timeOutWithoutWaitingFuture() throws Exception {
    // given
    CountDownLatch completed = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();

    context.checking(buildExpectations(e -> {
      e.oneOf(operation).cancel(TIMEOUT_CAUSE);
      e.will(cancelOperation());

      e.oneOf(node).setContinuousTimeout(true);
    }));

    future.whenComplete((value, error) -> {
      failure.set(error);
      completed.countDown();
    });

    // when
    future.scheduleTimeout(OP_TIMEOUT_MILLIS);

    // then
    assertTrue(completed.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertInstanceOf(OperationTimeoutException.class, failure.get());
    assertTrue(failure.get().getMessage().contains(TIMEOUT_CAUSE));
    awaitTimeoutTask();
  }

  @Test
  void ignoresTimeoutWhenOperationAlreadyCompleted() throws Exception {
    // given: the operation has completed, but its callback has not reached this future yet
    context.checking(buildExpectations(e -> {
      e.oneOf(operation).cancel(TIMEOUT_CAUSE);
      e.will(returnValue(false));

      e.oneOf(node).setContinuousTimeout(false);
    }));

    future.scheduleTimeout(OP_TIMEOUT_MILLIS);

    // when: the timeout fails to cancel the completed operation
    awaitTimeoutWindow();

    // then: no node timeout update, and the response still completes this future
    assertFalse(future.isDone());
    future.complete();
    assertEquals("value", future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void cancelsTimeoutWhenResponseArrivesFirst() throws Exception {
    // given
    context.checking(buildExpectations(e -> {
      e.oneOf(node).setContinuousTimeout(false);
    }));

    future.scheduleTimeout(OP_TIMEOUT_MILLIS);
    ScheduledFuture<?> timeout = scheduledTimeout();
    assertNotNull(timeout);

    // when
    future.complete();

    // then
    assertEquals("value", future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(timeout.isCancelled());

    awaitTimeoutWindow();
    assertEquals("value", future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void cancelsOperationWhileWaitingForResponse() throws Exception {
    // given
    context.checking(buildExpectations(e -> {
      e.oneOf(operation).cancel("by application.");
      e.will(cancelOperation());
    }));

    future.scheduleTimeout(OP_TIMEOUT_MILLIS);
    ScheduledFuture<?> timeout = scheduledTimeout();
    assertNotNull(timeout);

    // when
    boolean cancelled = future.cancel(false);

    // then
    assertTrue(cancelled);
    assertTrue(timeout.isCancelled());
    assertThrows(CancellationException.class,
        () -> future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

    awaitTimeoutWindow();
    assertTrue(future.isCancelled());
  }

  @Test
  void excludesDecodingTimeFromOperationTimeout() throws Exception {
    // given
    future = new ArcusFutureImpl<>(result, this::blockingDecode);
    future.setOp(operation);

    context.checking(buildExpectations(e -> {
      e.oneOf(node).setContinuousTimeout(false);
    }));

    future.scheduleTimeout(OP_TIMEOUT_MILLIS);

    // when
    future.complete();

    assertTrue(decodingStarted.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

    // block the decoding to simulate a long-running decode operation
    awaitTimeoutWindow();

    // then
    assertFalse(future.isDone());
    releaseDecoding.countDown();
    assertEquals("value", future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void doesNotCancelAfterResponseArrives() throws Exception {
    // given
    future = new ArcusFutureImpl<>(result, this::blockingDecode);
    future.setOp(operation);

    context.checking(buildExpectations(e -> {
      e.oneOf(node).setContinuousTimeout(false);

      // the completed operation can no longer be cancelled
      e.oneOf(operation).cancel("by application.");
      e.will(returnValue(false));
    }));

    future.scheduleTimeout(OP_TIMEOUT_MILLIS);
    future.complete();

    assertTrue(decodingStarted.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

    // when: cancel the future while the decoding is still in progress
    boolean cancelled = future.cancel(false);

    // then: the decoded result completes the future
    assertFalse(cancelled);
    assertFalse(future.isCancelled());

    releaseDecoding.countDown();
    assertEquals("value", future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void ignoresResponseAfterOperationTimeout() throws Exception {
    // given
    context.checking(buildExpectations(e -> {
      e.oneOf(operation).cancel(TIMEOUT_CAUSE);
      e.will(cancelOperation());

      e.oneOf(node).setContinuousTimeout(true);
    }));

    future.scheduleTimeout(OP_TIMEOUT_MILLIS);

    ExecutionException timeout = assertThrows(
        ExecutionException.class,
        () -> future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    );

    assertInstanceOf(OperationTimeoutException.class, timeout.getCause());
    awaitTimeoutTask();

    // when: complete the future after the operation has timed out
    future.complete();

    // then
    ExecutionException afterResponse = assertThrows(
        ExecutionException.class,
        () -> future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    );

    assertSame(timeout.getCause(), afterResponse.getCause());
  }

  @Test
  void doesNotScheduleTimeoutAfterResponseCompletes() throws Exception {
    // given
    context.checking(buildExpectations(e -> {
      e.oneOf(node).setContinuousTimeout(false);
    }));

    future.complete();

    // when: schedule a timeout after the response has already completed
    future.scheduleTimeout(OP_TIMEOUT_MILLIS);

    // then
    assertNull(scheduledTimeout());
    assertEquals("value", future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  void doesNotCancelAfterOperationTimeout() throws Exception {
    // given
    context.checking(buildExpectations(e -> {
      e.oneOf(operation).cancel(TIMEOUT_CAUSE);
      e.will(cancelOperation());

      e.oneOf(node).setContinuousTimeout(true);
    }));

    future.scheduleTimeout(OP_TIMEOUT_MILLIS);

    ExecutionException timeout = assertThrows(
        ExecutionException.class,
        () -> future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    );
    awaitTimeoutTask();

    // when: an unexpected op.cancel("by application.") fails the mock
    boolean cancelled = future.cancel(false);

    // then
    assertFalse(cancelled);
    assertFalse(future.isCancelled());

    ExecutionException afterCancel = assertThrows(
        ExecutionException.class,
        () -> future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    );

    assertSame(timeout.getCause(), afterCancel.getCause());
  }

  @Test
  void stopsTimeoutOnInternalCancel() throws Exception {
    // given
    future.scheduleTimeout(OP_TIMEOUT_MILLIS);
    ScheduledFuture<?> timeout = scheduledTimeout();
    assertNotNull(timeout);

    // when
    future.internalCancel();

    // then
    assertTrue(timeout.isCancelled());
    assertTrue(future.isCancelled());

    // no timeout side effects: unexpected mock calls fail in tearDown
    awaitTimeoutWindow();
    assertTrue(future.isCancelled());
  }

  /**
   * Acts like {@code BaseOperationImpl.cancel()}, which records the cancel cause,
   * invokes the callback with the cancelled status and then completes it.
   */
  private Action cancelOperation() {
    return new CustomAction("cancel the operation and invoke its callback") {
      @Override
      public Object invoke(Invocation invocation) {
        cancelCause.set("Cancelled (" + invocation.getParameter(0) + " @ test-node)");
        future.internalCancel();
        future.complete();
        return true;
      }
    };
  }

  private void awaitTimeoutWindow() throws Exception {
    ArcusExecutors.TIMEOUT_SCHEDULER
        .schedule(() -> null, OP_TIMEOUT_MILLIS + 50L, TimeUnit.MILLISECONDS)
        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Waits until the running timeout task returns.
   * The node timeout count is updated after this future is completed.
   */
  private void awaitTimeoutTask() throws Exception {
    ArcusExecutors.TIMEOUT_SCHEDULER
        .submit(() -> null)
        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private ScheduledFuture<?> scheduledTimeout() throws Exception {
    Field field = ArcusFutureImpl.class.getDeclaredField("timeoutFuture");
    field.setAccessible(true);

    return (ScheduledFuture<?>) field.get(future);
  }

  private String blockingDecode(String value) {
    decodingStarted.countDown();

    try {
      if (!releaseDecoding.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Decoder was not released");
      }
      return value;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Decoder was interrupted", e);
    }
  }
}
