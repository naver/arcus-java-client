package net.spy.memcached.v2;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.CreateAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.v2.attribute.UpdateAttributes;
import net.spy.memcached.v2.vo.BKey;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AttributesAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  @Test
  void setKVAttributesSuccess() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.set(key, 0, VALUE)
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    UpdateAttributes attributes = UpdateAttributes.builder()
        .expireTime(100L)
        .build();

    async.setAttributes(key, attributes)
        // then
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void setBTreeAttributesSuccess()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    CreateAttributes createAttributes = CreateAttributes.builder()
        .readable(false)
        .build();

    async.bopCreate(key, ElementValueType.STRING, createAttributes)
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    UpdateAttributes attributes = UpdateAttributes.builder()
        .expireTime(100L)
        .maxCount(5_000L)
        .overflowAction(CollectionOverflowAction.largest_trim)
        .maxBKeyRange(BKey.of(10L))
        .readable()
        .build();

    async.setAttributes(key, attributes)
        .thenCompose(result -> {
          assertTrue(result);
          return async.getAttributes(key);
        })
        .thenAccept(result -> {
          assertNotNull(result);
          assertEquals(100L, result.getExpireTime());
          assertEquals(5_000L, result.getMaxCount());
          assertEquals(CollectionOverflowAction.largest_trim, result.getOverflowAction());
          assertEquals(true, result.getReadable());
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void setAttributesFailureKeyNotFound()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    UpdateAttributes attributes = UpdateAttributes.builder()
        .expireTime(100L)
        .build();

    // when
    async.setAttributes(key, attributes)
        // then
        .thenAccept(Assertions::assertFalse)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void setAttributesFailureMapOverflowAction()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.mopCreate(key, ElementValueType.STRING, CreateAttributes.DEFAULT)
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    UpdateAttributes attributes = UpdateAttributes.builder()
        .overflowAction(CollectionOverflowAction.smallest_trim)
        .build();

    // when
    async.setAttributes(key, attributes)
        // then
        .handle((result, ex) -> {
          assertInstanceOf(OperationException.class, ex);
          assertTrue(ex.getMessage().contains("ATTR_ERROR"));
          return result;
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void setAttributesFailureSetOverflowAction()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    async.sopCreate(key, ElementValueType.STRING, CreateAttributes.DEFAULT)
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    UpdateAttributes attributes = UpdateAttributes.builder()
        .overflowAction(CollectionOverflowAction.largest_trim)
        .build();

    // when
    async.setAttributes(key, attributes)
        // then
        .handle((result, ex) -> {
          assertInstanceOf(OperationException.class, ex);
          assertTrue(ex.getMessage().contains("ATTR_ERROR"));
          return result;
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void getAttributesSuccess() throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    CreateAttributes attributes = CreateAttributes.builder()
        .expireTime(100L)
        .maxCount(5000L)
        .overflowAction(CollectionOverflowAction.smallest_trim)
        .build();

    async.bopCreate(key, ElementValueType.STRING, attributes)
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);

    // when
    async.getAttributes(key)
        // then
        .thenAccept(result -> {
          assertNotNull(result);
          assertTrue(result.getExpireTime() <= 100L);
          assertEquals(5_000L, result.getMaxCount());
          assertEquals(CollectionOverflowAction.smallest_trim, result.getOverflowAction());
        })
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

  @Test
  void getAttributesFailureKeyNotFound()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    String key = keys.get(0);

    // when
    async.getAttributes(key)
        // then
        .thenAccept(Assertions::assertNull)
        .toCompletableFuture()
        .get(300L, TimeUnit.MILLISECONDS);
  }

}
