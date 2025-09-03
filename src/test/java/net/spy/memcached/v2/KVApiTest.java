package net.spy.memcached.v2;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KVApiTest extends ApiTest {

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
        .thenCompose(result -> {
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
          return async.multiGet(keys);
        })
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

}
