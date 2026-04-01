package net.spy.memcached.v2;

import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.v2.vo.GetArgs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SetAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  @Test
  void sopCreate() throws Exception {
    // given
    String key = keys.get(0);

    // when
    async.sopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            // then
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopCreateAlreadyExists() throws Exception {
    // given
    String key = keys.get(0);

    async.sopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopInsert() throws Exception {
    // given
    String key = keys.get(0);

    async.sopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopInsert(key, VALUE)
            // then
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopInsertNotFound() throws Exception {
    // when
    async.sopInsert(keys.get(0), VALUE)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopInsertWithAttributes() throws Exception {
    // given
    String key = keys.get(0);

    // when
    async.sopInsert(key, VALUE, new CollectionAttributes())
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.sopExist(key, VALUE);
            })
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopInsertDuplicate() throws Exception {
    // given
    String key = keys.get(0);

    async.sopInsert(key, VALUE, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopInsert(key, VALUE)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopInsertTypeMismatch() throws Exception {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopInsert(key, VALUE, new CollectionAttributes())
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
  void sopExistTrue() throws Exception {
    // given
    String key = keys.get(0);

    async.sopInsert(key, VALUE, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopExist(key, VALUE)
            // then
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopExistFalse() throws Exception {
    // given
    String key = keys.get(0);

    async.sopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when: 존재하지 않는 값 조회
    async.sopExist(key, VALUE)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopExistNotFound() throws Exception {
    // given
    // when
    async.sopExist(keys.get(0), VALUE)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopExistTypeMismatch() throws Exception {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopExist(key, VALUE)
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
  void sopGet() throws Exception {
    // given
    String key = keys.get(0);

    async.sopInsert(key, "v0", new CollectionAttributes())
            .thenCompose(result -> async.sopInsert(key, "v1"))
            .thenCompose(result -> async.sopInsert(key, "v2"))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopGet(key, 10, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> {
              assertNotNull(result);
              assertEquals(3, result.size());
            })
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopGetNotFound() throws Exception {
    // given
    // when
    async.sopGet(keys.get(0), 10, GetArgs.DEFAULT)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopGetNotFoundElement() throws Exception {
    // given
    String key = keys.get(0);

    async.sopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopGet(key, 10, GetArgs.DEFAULT)
            // then
            .thenAccept(result -> assertTrue(result.isEmpty()))
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopGetWithDelete() throws Exception {
    // given
    String key = keys.get(0);
    GetArgs args = new GetArgs.Builder()
            .withDelete()
            .dropIfEmpty()
            .build();

    async.sopInsert(key, VALUE, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopGet(key, 10, args)
            .thenCompose(result -> {
              assertNotNull(result);
              assertEquals(1, result.size());
              return async.sopGet(key, 10, GetArgs.DEFAULT);
            })
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopDelete() throws Exception {
    // given
    String key = keys.get(0);

    async.sopInsert(key, VALUE, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopDelete(key, VALUE, false)
            // then
            .thenCompose(result -> {
              assertTrue(result);
              return async.sopExist(key, VALUE);
            })
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopDeleteNotFound() throws Exception {
    // when
    async.sopDelete(keys.get(0), VALUE, false)
            // then
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopDeleteNotFoundElement() throws Exception {
    // given
    String key = keys.get(0);

    async.sopCreate(key, ElementValueType.STRING, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopDelete(key, VALUE, false)
            // then
            .thenAccept(Assertions::assertFalse)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopDeleteDropIfEmpty() throws Exception {
    // given
    String key = keys.get(0);

    async.sopInsert(key, VALUE, new CollectionAttributes())
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when: 마지막 요소 삭제 + dropIfEmpty=true
    async.sopDelete(key, VALUE, true)
            // then: set 자체가 삭제되어 null
            .thenCompose(result -> {
              assertTrue(result);
              return async.sopGet(key, 10, GetArgs.DEFAULT);
            })
            .thenAccept(Assertions::assertNull)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void sopDeleteTypeMismatch() throws Exception {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
            .thenAccept(Assertions::assertTrue)
            .toCompletableFuture()
            .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.sopDelete(key, VALUE, false)
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
