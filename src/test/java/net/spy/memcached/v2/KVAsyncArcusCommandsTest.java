package net.spy.memcached.v2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.internal.CompositeException;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KVAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  @Test
  void setAndGet() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    // when
    async.set(key, 0, VALUE)
        .thenCompose(result -> {
          assertTrue(result);
          return async.get(key);
        })
        // then
        .thenAccept(result -> assertEquals(VALUE, result))
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void getNull() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.get(keys.get(0))
        // then
        .thenAccept(Assertions::assertNull)
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void cancelSet() {
    // given
    ArcusFuture<Boolean> future = async.set(keys.get(0), 0, VALUE);

    // when
    boolean cancelled = future.cancel(true);

    // then
    if (cancelled) {
      assertTrue(future.isCancelled());
    }
  }

  @Test
  void multiSetAndGet() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.multiSet(keys, 0, VALUE)
        .thenAccept(result -> {
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
        })
        .thenCompose(v -> async.multiGet(keys))
        // then
        .thenAccept(result -> {
          assertEquals(keys.size(), result.size());
          for (Object o : result.values()) {
            assertEquals(VALUE, o);
          }
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiSetFail() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.set(keys.get(0), 0, VALUE)
        // when
        .thenCompose(result -> {
          assertTrue(result);
          return async.multiAdd(keys, 0, VALUE);
        })
        // then
        .thenAccept(result -> {
          assertEquals(keys.size(), result.size());
          assertFalse(result.get(keys.get(0)));
          for (int i = 1; i < 4; i++) {
            assertTrue(result.get(keys.get(i)));
          }
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiSetTypeMismatchException()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    Map<String, CollectionOperationStatus> result =
        arcusClient.asyncLopInsertBulk(keys.subList(0, 2), 0, "value",
            new CollectionAttributes()).get();
    assertTrue(result.isEmpty());

    // when
    async.multiSet(keys, 0, VALUE)
        // then
        .handle((res, ex) -> {
              assertInstanceOf(CompositeException.class, ex);
              CompositeException ex2 = (CompositeException) ex;
              List<Exception> exceptions = ex2.getExceptions();
              assertEquals(2, exceptions.size());
              for (Exception exception : exceptions) {
                assertTrue(exception.getMessage().contains("TYPE_MISMATCH"));
              }
              return res;
            }
        )
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiGetNothing() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.multiGet(keys)
        // then
        .thenAccept(result -> {
          assertTrue(result.isEmpty());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void cancelMultiGet() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.multiSet(keys, 0, VALUE)
        .thenAccept(result -> {
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
    ArcusFuture<Map<String, Object>> future = async.multiGet(keys);

    //when
    boolean cancelled = future.cancel(true);

    // then
    if (cancelled) {
      assertThrows(CancellationException.class, () -> future.get(300, TimeUnit.MILLISECONDS));
      assertTrue(future.isCancelled());
      Map<String, Object> result =
          ((ArcusMultiFuture<Map<String, Object>>) future).getResultsWithFailures();
      assertEquals(keys.size(), result.size());
      for (Object value : result.values()) {
        assertTrue(value == null || VALUE.equals(value));
      }
    }
  }
}
