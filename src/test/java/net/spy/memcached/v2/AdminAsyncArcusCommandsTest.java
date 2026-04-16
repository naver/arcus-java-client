package net.spy.memcached.v2;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  @Test
  void flush() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);
    async.set(key, 0, VALUE)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.flush()
        // then
        .thenCompose(result -> {
          assertTrue(result);
          return async.get(key);
        })
        .thenAccept(Assertions::assertNull)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void flushWithDelay() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);
    async.set(key, 0, VALUE)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.flush(0)
        // then
        .thenCompose(result -> {
          assertTrue(result);
          return async.get(key);
        })
        .thenAccept(Assertions::assertNull)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void flushWithPrefix() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String keyA = "prefixA:key";
    String keyB = "prefixB:key";
    async.set(keyA, 0, VALUE)
        .thenCompose(result -> async.set(keyB, 0, VALUE))
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.flush("prefixA")
        // then
        .thenCompose(result -> {
          assertTrue(result);
          return async.get(keyA);
        })
        .thenCompose(result -> {
          assertNull(result);
          return async.get(keyB);
        })
        .thenAccept(result -> {
          assertNotNull(result);
          assertEquals(VALUE, result);
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void flushWithPrefixNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.flush("nonExistentPrefix")
        // then
        .thenAccept(Assertions::assertFalse)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void flushWithPrefixAndDelay() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = "prefixA:key";
    async.set(key, 0, VALUE)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.flush("prefixA", 0)
        // then
        .thenCompose(result -> {
          assertTrue(result);
          return async.get(key);
        })
        .thenAccept(Assertions::assertNull)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void flushNonPrefix() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String prefixKey = "prefixA:key";
    String nonPrefixKey = "test-key";

    async.set(prefixKey, 0, VALUE)
        .thenCompose(result -> async.set(nonPrefixKey, 0, VALUE))
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.flush("")
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.get(prefixKey);
            })
            .thenCompose(result -> {
              assertNotNull(result);
              assertEquals(VALUE, result);
              return async.get(nonPrefixKey);
            })
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void stats() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.stats()
        // then
        .thenAccept(result -> {
          assertFalse(result.isEmpty());
          result.forEach((address, stat) -> {
            assertNotNull(address);
            assertTrue(stat.containsKey("total_items"));
          });
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void statsAllArgs() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    for (StatsArg arg : StatsArg.values()) {
      // when
      async.stats(arg)
              // then
              .thenAccept(Assertions::assertNotNull)
              .toCompletableFuture()
              .get(300L, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  void versions() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.versions()
        // then
        .thenAccept(result -> {
          assertFalse(result.isEmpty());
          result.forEach((socketAddress, version) -> {
            assertNotNull(socketAddress);
            assertNotNull(version);
            assertFalse(version.isEmpty());
          });
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

}
