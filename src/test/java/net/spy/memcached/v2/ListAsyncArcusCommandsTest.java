package net.spy.memcached.v2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.v2.vo.GetArgs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  private static final List<Object> VALUES = Arrays.asList("v0", "v1", "v2", "v3", "v4");

  @Test
  void lopCreate() throws Exception {
    // given
    String key = keys.get(0);

    // when
    async.lopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            // then
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopCreateAlreadyExists() throws Exception {
    // given
    String key = keys.get(0);

    async.lopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopInsert() throws Exception {
    // given
    String key = keys.get(0);

    async.lopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopInsert(key, 0, VALUES.get(0))
            // then
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopInsertNotFound() throws Exception {
    // given
    // when
    async.lopInsert(keys.get(0), 0, VALUES.get(0))
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopInsertWithAttributes() throws Exception {
    // given
    String key = keys.get(0);

    // when
    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.lopGet(key, 0, GetArgs.DEFAULT);
            })
            .thenAccept(result -> assertEquals(VALUES.get(0), result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopInsertTypeMismatch() throws Exception {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            // then
            .handle((result, ex) -> {
              assertInstanceOf(OperationException.class, ex);
              assertTrue(ex.getMessage().contains("TYPE_MISMATCH"));
              return result;
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetSingle() throws Exception {
    // given
    String key = keys.get(0);

    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopGet(key, 0, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> assertEquals(VALUES.get(0), result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetSingleNotFound() throws Exception {
    // given
    // when
    async.lopGet(keys.get(0), 0, GetArgs.DEFAULT)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetSingleNotFoundElement() throws Exception {
    // given
    String key = keys.get(0);

    async.lopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopGet(key, 5, GetArgs.DEFAULT)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetSingleWithDelete() throws Exception {
    // given
    String key = keys.get(0);
    GetArgs args = new GetArgs.Builder()
            .withDelete()
            .dropIfEmpty()
            .build();

    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopGet(key, 0, args)
            // then
            .thenCompose(result -> {
              assertEquals(VALUES.get(0), result);
              return async.lopGet(key, 0, GetArgs.DEFAULT);
            })
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetRange() throws Exception {
    // given
    String key = keys.get(0);

    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            .thenCompose(result -> async.lopInsert(key, 1, VALUES.get(1)))
            .thenCompose(result -> async.lopInsert(key, 2, VALUES.get(2)))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopGet(key, 0, 2, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> {
              result.forEach(Assertions::assertNotNull);
              assertIterableEquals(VALUES.subList(0, 3), result);
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetRangeNotFound() throws Exception {
    // given
    // when
    async.lopGet(keys.get(0), 0, 2, GetArgs.DEFAULT)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetRangeNotFoundElement() throws Exception {
    // given
    String key = keys.get(0);

    async.lopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopGet(key, 5, 10, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> assertTrue(result.isEmpty()))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopGetRangeWithDeleteAndDropIfEmpty() throws Exception {
    // given
    String key = keys.get(0);
    GetArgs args = new GetArgs.Builder()
            .withDelete()
            .dropIfEmpty()
            .build();

    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            .thenCompose(result -> async.lopInsert(key, 1, VALUES.get(1)))
            .thenCompose(result -> async.lopInsert(key, 2, VALUES.get(2)))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopGet(key, 0, 2, args)
            .thenCompose(result -> {
              assertNotNull(result);
              assertIterableEquals(VALUES.subList(0, 3), result);
              return async.lopGet(key, 0, 2, GetArgs.DEFAULT);
            })
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopDeleteSuccess() throws Exception {
    // given
    String key = keys.get(0);

    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopDelete(key, 0, false)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.lopGet(key, 0, GetArgs.DEFAULT);
            })
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopDeleteRangeSuccess() throws Exception {
    // given
    String key = keys.get(0);

    async.lopInsert(key, 0, VALUES.get(0), new CollectionAttributes())
            .thenCompose(result -> async.lopInsert(key, 1, VALUES.get(1)))
            .thenCompose(result -> async.lopInsert(key, 2, VALUES.get(2)))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopDelete(key, 0, 2, false)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.lopGet(key, 0, 2, GetArgs.DEFAULT);
            })
            .thenAccept(result -> assertTrue(result.isEmpty()))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopDeleteNotFoundElement() throws Exception {
    // given
    String key = keys.get(0);

    async.lopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopDelete(key, 0, false)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopDeleteNotFound() throws Exception {
    // when
    async.lopDelete(keys.get(0), 0, false)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void lopDeleteTypeMismatch() throws Exception {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.lopDelete(key, 0, false)
            // then
            .handle((result, ex) -> {
              assertInstanceOf(OperationException.class, ex);
              assertTrue(ex.getMessage().contains("TYPE_MISMATCH"));
              return result;
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

}
