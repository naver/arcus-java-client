package net.spy.memcached.v2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.v2.vo.GetArgs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  private static final String MKEY1 = "mkey1";
  private static final String MKEY2 = "mkey2";
  private static final String MKEY3 = "mkey3";
  private static final String VALUE1 = "value1";
  private static final String VALUE2 = "value2";
  private static final String VALUE3 = "value3";

  @Test
  void mopCreate() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    // when
    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            // then
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopCreateAlreadyExists() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopInsert() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopInsert(key, MKEY1, VALUE1)
            // then
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopInsertNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.mopInsert(keys.get(0), MKEY1, VALUE1)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopInsertWithAttributes() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    // when
    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, MKEY1, GetArgs.DEFAULT);
            })
            .thenAccept(result -> assertEquals(VALUE1, result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopInsertDuplicate() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopInsert(key, MKEY1, VALUE2)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopInsertTypeMismatch() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
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
  void mopUpsertInsert() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopUpsert(key, MKEY1, VALUE1)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, MKEY1, GetArgs.DEFAULT);
            })
            .thenAccept(result -> assertEquals(VALUE1, result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopUpsertReplace() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopUpsert(key, MKEY1, VALUE2)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, MKEY1, GetArgs.DEFAULT);
            })
            .thenAccept(result -> assertEquals(VALUE2, result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopUpsertNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // when
    async.mopUpsert(keys.get(0), MKEY1, VALUE1)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopUpdate() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopUpdate(key, MKEY1, VALUE2)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, MKEY1, GetArgs.DEFAULT);
            })
            .thenAccept(result -> assertEquals(VALUE2, result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopUpdateNotFoundElement() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopUpdate(key, MKEY1, VALUE1)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopUpdateNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.mopUpdate(keys.get(0), MKEY1, VALUE1)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetAll() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenCompose(result -> async.mopInsert(key, MKEY2, VALUE2))
            .thenCompose(result -> async.mopInsert(key, MKEY3, VALUE3))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> {
              assertNotNull(result);
              assertEquals(3, result.size());
              assertEquals(VALUE1, result.get(MKEY1));
              assertEquals(VALUE2, result.get(MKEY2));
              assertEquals(VALUE3, result.get(MKEY3));
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetAllNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.mopGet(keys.get(0), GetArgs.DEFAULT)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetAllNotFoundElement() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> {
              assertNotNull(result);
              assertTrue(result.isEmpty());
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetSingle() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, MKEY1, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> assertEquals(VALUE1, result))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetSingleNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    // when
    async.mopGet(keys.get(0), MKEY1, GetArgs.DEFAULT)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetSingleNotFoundElement() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, MKEY1, GetArgs.DEFAULT)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetMultipleKeys() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);
    List<String> mKeys = Arrays.asList(MKEY1, MKEY2);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenCompose(result -> async.mopInsert(key, MKEY2, VALUE2))
            .thenCompose(result -> async.mopInsert(key, MKEY3, VALUE3))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, mKeys, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> {
              assertNotNull(result);
              assertEquals(2, result.size());
              assertEquals(VALUE1, result.get(MKEY1));
              assertEquals(VALUE2, result.get(MKEY2));
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetMultipleKeysNotFoundElement() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, Arrays.asList(MKEY1, MKEY2), GetArgs.DEFAULT)
            // then
            .thenAccept(result -> {
              assertNotNull(result);
              assertTrue(result.isEmpty());
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetMultipleKeysPartialFound() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenCompose(result -> async.mopInsert(key, MKEY2, VALUE2))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, Arrays.asList(MKEY1, MKEY2, MKEY3), GetArgs.DEFAULT)
            // then
            .thenAccept(result -> {
              assertNotNull(result);
              assertEquals(2, result.size());
              assertEquals(VALUE1, result.get(MKEY1));
              assertEquals(VALUE2, result.get(MKEY2));
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopGetAllWithDelete() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);
    GetArgs args = new GetArgs.Builder()
            .withDelete()
            .dropIfEmpty()
            .build();

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopGet(key, args)
            .thenCompose(result -> {
              assertNotNull(result);
              assertEquals(1, result.size());
              return async.mopGet(key, GetArgs.DEFAULT);
            })
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDelete() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenCompose(result -> async.mopInsert(key, MKEY2, VALUE2))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, false)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, GetArgs.DEFAULT);
            })
            .thenAccept(result -> {
              assertNotNull(result);
              assertTrue(result.isEmpty());
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteDropIfEmpty() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, true)
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, GetArgs.DEFAULT);
            })
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // when
    async.mopDelete(keys.get(0), false)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteSingle() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenCompose(result -> async.mopInsert(key, MKEY2, VALUE2))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, MKEY1, false)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, GetArgs.DEFAULT);
            })
            .thenAccept(result -> {
              assertNotNull(result);
              assertEquals(1, result.size());
              assertFalse(result.containsKey(MKEY1));
              assertTrue(result.containsKey(MKEY2));
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteSingleNotFoundElement() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, MKEY1, false)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteSingleNotFound() throws ExecutionException, InterruptedException, TimeoutException {
    // when
    async.mopDelete(keys.get(0), MKEY1, false)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteSingleDropIfEmpty() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, MKEY1, true)
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, GetArgs.DEFAULT);
            })
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteMultiple() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopInsert(key, MKEY2, VALUE2);
            })
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopInsert(key, MKEY3, VALUE3);
            })
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, Arrays.asList(MKEY1, MKEY2), false)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, GetArgs.DEFAULT);
            })
            .thenAccept(result -> {
              assertNotNull(result);
              assertEquals(1, result.size());
              assertFalse(result.containsKey(MKEY1));
              assertFalse(result.containsKey(MKEY2));
              assertTrue(result.containsKey(MKEY3));
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteMultipleNotFoundElement() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when - MKEY2, MKEY3 don't exist
    async.mopDelete(key, Arrays.asList(MKEY2, MKEY3), false)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteMultipleDropIfEmpty() throws ExecutionException, InterruptedException,
          TimeoutException {
    // given
    String key = keys.get(0);

    async.mopInsert(key, MKEY1, VALUE1, new CollectionAttributes())
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopInsert(key, MKEY2, VALUE2);
            })
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, Arrays.asList(MKEY1, MKEY2), true)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.mopGet(key, GetArgs.DEFAULT);
            })
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void mopDeleteTypeMismatch() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.mopDelete(key, false)
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
