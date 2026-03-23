package net.spy.memcached.v2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.CASValue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
  void multiGetNothing() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.multiGet(keys)
        // then
        .thenAccept(result -> assertTrue(result.isEmpty()))
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void cancelMultiGet() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.multiSet(items, 0)
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

  @Test
  void appendString() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String expected = "Hello, Arcus!";
    String key = keys.get(0);

    async.set(key, 60, "Hello, ")
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.append(key, "Arcus!")
            .thenCompose(result -> {
              assertTrue(result);
              return async.get(key);
            })
            // then
            .thenAccept(result -> assertEquals(expected, result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void prependString() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String expected = "Hello, World!";
    String key = keys.get(1);

    async.set(key, 60, "World!")
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.prepend(key, "Hello, ")
            .thenCompose(result -> {
              assertTrue(result);
              return async.get(key);
            })
            // then
            .thenAccept(result -> assertEquals(expected, result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void appendNonStringException() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.set(key, 60, 123)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    CompletableFuture<Object> future = async.append(key, "Arcus!")
            .thenCompose(result -> {
              assertTrue(result);
              return async.get(key);
            })
            .toCompletableFuture();
    // then
    // AssertionError in the Transcoder causes the I/O thread to terminate abruptly.
    // The future is never completed, leading to a TimeoutException in the main thread.
    assertThrows(TimeoutException.class, () -> future.get(300L, TimeUnit.MILLISECONDS));
  }

  @Test
  void testGets() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.set(key, 60, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.gets(key)
            // then
            .thenAccept(result -> {
              assertEquals(VALUE, result.getValue());
              assertTrue(result.getCas() > 0);
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void testCas() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    CASValue<Object> casValue = async.set(key, 60, "Hello")
            .thenCompose(result -> {
              assertTrue(result);
              return async.gets(key);
            })
            .thenApply(result -> {
              assertEquals("Hello", result.getValue());
              assertTrue(result.getCas() >= 0);
              return result;
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.cas(key, 60, "Arcus", casValue.getCas())
            .thenCompose(result -> {
              assertTrue(result);
              return async.get(key);
            })
            // then
            .thenAccept(result -> assertEquals("Arcus", result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void casNotFound() throws ExecutionException, InterruptedException,
          TimeoutException {

    // given
    String key = keys.get(0);
    long fakeCas = 1234L;

    async.set(key, 60, "Hello")
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.cas(key, 60, "Arcus", fakeCas)
            // then
            .thenCompose(result -> {
              assertFalse(result);
              return async.get(key);
            })
            .thenAccept(result -> {
              assertNotEquals("Arcus", result);
              assertEquals("Hello", result);
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void casExists() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    CASValue<Object> casValue = async.set(key, 60, "Hello")
            .thenCompose(result -> {
              assertTrue(result);
              return async.gets(key);
            })
            .thenApply(result -> {
              assertEquals("Hello", result.getValue());
              assertTrue(result.getCas() >= 0);
              return result;
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    /* already updated by another client */
    async.cas(key, 60, "Update", casValue.getCas())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.cas(key, 60, "Arcus", 0L)
            // then
            .thenCompose(result -> {
              assertFalse(result);
              return async.get(key);
            })
            .thenAccept(result -> {
              assertNotEquals("Arcus", result);
              assertEquals("Update", result);
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiGetsSuccess() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.multiSet(items, 60)
            .thenAccept(result -> {
              for (Boolean b : result.values()) {
                assertTrue(b);
              }
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.multiGets(keys)
            // then
            .thenAccept(result -> {
              assertEquals(keys.size(), result.size());
              for (Map.Entry<String, CASValue<Object>> entry : result.entrySet()) {
                assertEquals(VALUE, entry.getValue().getValue());
                assertTrue(entry.getValue().getCas() >= 0);
              }
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiGetsPartialFailure() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.set(keys.get(0), 60, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.multiGets(keys)
            // then
            .thenAccept(result -> {
              assertEquals(1, result.size());
              assertTrue(result.containsKey(keys.get(0)));
              assertEquals(VALUE, result.get(keys.get(0)).getValue());
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiGetsNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.multiGets(keys)
            // then
            .thenAccept(result -> assertTrue(result.isEmpty()))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiSetSuccess() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.multiSet(items, 60)
        .thenCompose(result -> {
          assertEquals(keys.size(), result.size());
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
          return async.multiGet(keys);
        })
        // then
        .thenAccept(result -> {
          assertEquals(keys.size(), result.size());
          for (String key : keys) {
            assertEquals(VALUE, result.get(key));
          }
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiAddSuccess() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.multiAdd(items, 60)
        .thenCompose(result -> {
          assertEquals(keys.size(), result.size());
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
          return async.multiGet(keys);
        })
        // then
        .thenAccept(result -> {
          assertEquals(keys.size(), result.size());
          for (String key : keys) {
            assertEquals(VALUE, result.get(key));
          }
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiAddPartialSuccess() throws ExecutionException, InterruptedException,
      TimeoutException {

    // given
    /* 0th key is added before multiAdd, so it should fail. */
    async.set(keys.get(0), 60, VALUE + "-old")
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.multiAdd(items, 60)
        .thenCompose(result -> {
          assertEquals(keys.size(), result.size());
          assertFalse(result.get(keys.get(0)));
          for (int i = 1; i < keys.size(); i++) {
            assertTrue(result.get(keys.get(i)));
          }
          return async.multiGet(keys);
        })
        // then
        .thenAccept(result -> {
          assertEquals(keys.size(), result.size());
          assertEquals(VALUE + "-old", result.get(keys.get(0)));
          for (int i = 1; i < keys.size(); i++) {
            assertEquals(VALUE, result.get(keys.get(i)));
          }
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiReplaceSuccess() throws ExecutionException, InterruptedException,
      TimeoutException {

    // given
    Map<String, Object> newItems = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      newItems.put(keys.get(i), VALUE + "-new-" + i);
    }

    async.multiSet(items, 60)
        .thenAccept(result -> {
          assertEquals(keys.size(), result.size());
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.multiReplace(newItems, 60)
        .thenCompose(result -> {
          assertEquals(keys.size(), result.size());
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
          return async.multiGet(keys);
        })
        // then
        .thenAccept(result -> {
          assertEquals(keys.size(), result.size());
          for (int i = 0; i < keys.size(); i++) {
            assertEquals(VALUE + "-new-" + i, result.get(keys.get(i)));
          }
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiReplacePartialSuccess() throws ExecutionException, InterruptedException,
      TimeoutException {

    // given
    Map<String, Object> oldItems = new HashMap<>();
    for (int i = 0; i < 2; i++) {
      oldItems.put(keys.get(i), VALUE + "-old-" + i);
    }

    Map<String, Object> newItems = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      newItems.put(keys.get(i), VALUE + "-new-" + i);
    }

    /* 0, 1st keys are added before multiReplace, so only they should succeed. */
    async.multiSet(oldItems, 60)
        .thenAccept(result -> {
          assertEquals(oldItems.size(), result.size());
          for (Boolean b : result.values()) {
            assertTrue(b);
          }
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.multiReplace(newItems, 60)
        .thenCompose(result -> {
          assertEquals(keys.size(), result.size());
          for (int i = 0; i < 2; i++) {
            assertTrue(result.get(keys.get(i)));
          }

          for (int i = 2; i < keys.size(); i++) {
            assertFalse(result.get(keys.get(i)));
          }
          return async.multiGet(keys);
        })
        // then
        .thenAccept(result -> {
          assertEquals(2, result.size());
          for (int i = 0; i < 2; i++) {
            assertEquals(VALUE + "-new-" + i, result.get(keys.get(i)));
          }
          for (int i = 2; i < keys.size(); i++) {
            assertNull(result.get(keys.get(i)));
          }
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void deleteSuccess() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.delete(key)
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
  void deleteNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    // when
    async.delete(key)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiDeleteSuccess() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    async.multiSet(items, 0)
            .thenAccept(result -> {
              for (Boolean b : result.values()) {
                assertTrue(b);
              }
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.multiDelete(keys)
            // then
            .thenCompose(result -> {
              assertEquals(keys.size(), result.size());
              result.values().forEach(Assertions::assertTrue);
              return async.multiGet(keys);
            })
            .thenAccept(result -> assertEquals(0, result.size()))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiDeletePartialSuccess() throws ExecutionException, InterruptedException,
          TimeoutException {

    // given
    async.set(keys.get(0), 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.multiDelete(keys)
            // then
            .thenAccept(result -> {
              assertEquals(keys.size(), result.size());
              assertTrue(result.get(keys.get(0)));
              for (int i = 1; i < keys.size(); i++) {
                assertFalse(result.get(keys.get(i)));
              }
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void multiDeleteNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.multiDelete(keys)
            // then
            .thenAccept(result -> {
              assertEquals(keys.size(), result.size());
              for (Boolean b : result.values()) {
                assertFalse(b);
              }
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }
}
