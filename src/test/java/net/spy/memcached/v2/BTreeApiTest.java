package net.spy.memcached.v2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;
import net.spy.memcached.v2.vo.BTreeElements;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.SMGetResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BTreeApiTest extends ApiTest {
  @Test
  void bopInsert() throws Exception {
    // given
    String key = keys.get(0);
    BKey bkey = BKey.of(1L);
    BTreeElement<Object> element = new BTreeElement<>(bkey, VALUE, null);
    CollectionAttributes attrs = new CollectionAttributes();

    // when
    async.bopCreate(key, ElementValueType.STRING, attrs)
        .thenCompose(result -> {
          assertTrue(result);
          return async.bopInsert(key, element, attrs);
        })
        // then
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopInsertNotFound() throws Exception {
    // given
    String key = keys.get(0);
    BKey bkey = BKey.of(1L);
    BTreeElement<Object> element = new BTreeElement<>(bkey, VALUE, null);

    // when
    async.bopInsert(key, element, null)
        // then
        .thenAccept(Assertions::assertFalse)
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopInsertTypeMisMatch() throws Exception {
    // given
    String key = keys.get(0);
    BKey bkey = BKey.of(1L);
    BTreeElement<Object> element = new BTreeElement<>(bkey, VALUE, null);
    CollectionAttributes attrs = new CollectionAttributes();

    // when
    async.set(key, 0, VALUE)
        .thenCompose(result -> {
          assertTrue(result);
          return async.bopInsert(key, element, attrs);
        })
        .handle((result, throwable) -> {
          assertNotNull(throwable);
          assertTrue(throwable.getCause().getMessage().contains("TYPE_MISMATCH"));
          assertNull(result);
          return result;
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopInsertAndGet() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(5);

    // when
    async.bopCreate(key, ElementValueType.STRING, attrs)
        .thenCompose(result -> {
          assertTrue(result);
          // Insert multiple elements to trigger trimming
          return async.bopInsert(key,
                  new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
              .thenCompose(r1 -> async.bopInsert(key,
                  new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
              .thenCompose(r2 -> async.bopInsert(key,
                  new BTreeElement<>(BKey.of(3L), "value3", null), attrs))
              .thenCompose(r3 -> async.bopInsert(key,
                  new BTreeElement<>(BKey.of(4L), "value4", null), attrs))
              .thenCompose(r4 -> async.bopInsert(key,
                  new BTreeElement<>(BKey.of(5L), "value5", null), attrs))
              .thenCompose(r5 -> async.bopInsertAndGetTrimmed(key,
                  new BTreeElement<>(BKey.of(6L), "value6", null), attrs));
        })
        // then
        .thenAccept(result -> {
          assertTrue(result.isInserted());
          BTreeElement<Object> trimmedElement = result.getTrimmedElement();
          if (trimmedElement != null) {
            assertEquals(BKey.of(1L), trimmedElement.getBkey());
            assertEquals("value1", trimmedElement.getValue());
          }
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGet() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();

    // when
    async.bopCreate(key, ElementValueType.STRING, attrs)
        .thenCompose(result -> {
          assertTrue(result);
          return async.bopInsert(key,
                  new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
              .thenCompose(r1 -> async.bopInsert(key,
                  new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
              .thenCompose(r2 -> async.bopInsert(key,
                  new BTreeElement<>(BKey.of(3L), "value3", null), attrs));
        })
        // then
        .thenCompose(result -> async.bopGet(key, BKey.of(2L), getArgs))
        .thenAccept(element -> {
          assertEquals(BKey.of(2L), element.getBkey());
          assertEquals("value2", element.getValue());
        })
        .thenCompose(v -> async.bopGet(key, BKey.of(1L), BKey.of(3L), getArgs))
        .thenAccept(elements -> {
          assertEquals(3, elements.getElements().size());
          assertTrue(elements.getElements().containsKey(BKey.of(1L)));
          assertTrue(elements.getElements().containsKey(BKey.of(2L)));
          assertTrue(elements.getElements().containsKey(BKey.of(3L)));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGetWithDelete() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgsWithDelete = new BopGetArgs.Builder()
        .withDelete()
        .count(10)
        .build();
    BopGetArgs getArgsNormal = new BopGetArgs.Builder()
        .count(10)
        .build();

    // when
    async.bopInsert(key,
            new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(r1 -> async.bopInsert(key,
            new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
        .thenCompose(r2 -> async.bopInsert(key,
            new BTreeElement<>(BKey.of(3L), "value3", null), attrs))
        .thenCompose(r3 -> async.bopGet(key, BKey.of(2L), getArgsWithDelete))
        // then
        .thenAccept(element -> {
          assertNotNull(element);
          assertEquals(BKey.of(2L), element.getBkey());
          assertEquals("value2", element.getValue());
        })
        .thenCompose(v -> async.bopGet(key, BKey.of(2L), getArgsNormal))
        .thenAccept(Assertions::assertNull)
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGetWithDeleteAndDropIfEmpty() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgsWithDeleteAndDrop = new BopGetArgs.Builder()
        .withDelete()
        .dropIfEmpty()
        .count(10)
        .build();

    // when
    async.bopInsert(key,
            new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(result -> async.bopGet(key, BKey.of(1L), getArgsWithDeleteAndDrop))
        // then
        .thenAccept(element -> {
          assertNotNull(element);
          assertEquals(BKey.of(1L), element.getBkey());
          assertEquals("value1", element.getValue());
        })
        .thenCompose(v -> async.bopInsert(key,
            new BTreeElement<>(BKey.of(2L), "value2", null), null))
        .thenAccept(Assertions::assertFalse) // Key Miss
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGetRangeWithDelete() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgsWithDelete = new BopGetArgs.Builder()
        .withDelete()
        .count(10)
        .build();
    BopGetArgs getArgsNormal = new BopGetArgs.Builder()
        .count(10)
        .build();

    // when
    async.bopInsert(key,
            new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(r1 -> async.bopInsert(key,
            new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
        .thenCompose(r2 -> async.bopInsert(key,
            new BTreeElement<>(BKey.of(3L), "value3", null), attrs))
        .thenCompose(r3 -> async.bopInsert(key,
            new BTreeElement<>(BKey.of(4L), "value4", null), attrs))
        .thenCompose(r4 -> async.bopGet(key, BKey.of(2L), BKey.of(3L), getArgsWithDelete))
        // then
        .thenAccept(elements -> {
          assertNotNull(elements);
          assertEquals(2, elements.getElements().size());
          assertTrue(elements.getElements().containsKey(BKey.of(2L)));
          assertTrue(elements.getElements().containsKey(BKey.of(3L)));
        })
        .thenCompose(v -> async.bopGet(key, BKey.of(1L), BKey.of(4L), getArgsNormal))
        .thenAccept(elements -> {
          assertNotNull(elements);
          assertEquals(2, elements.getElements().size());
          assertTrue(elements.getElements().containsKey(BKey.of(1L)));
          assertTrue(elements.getElements().containsKey(BKey.of(4L)));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopMGet() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();

    // when
    async.bopInsert(testKeys.get(0), new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(r -> async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1),
            new BTreeElement<>(BKey.of(3L), "value3", null), attrs))
        .thenCompose(r -> async.bopInsert(testKeys.get(1),
            new BTreeElement<>(BKey.of(4L), "value4", null), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(2),
            new BTreeElement<>(BKey.of(5L), "value5", null), attrs))
        // then
        .thenCompose(result -> async.bopMultiGet(testKeys, BKey.of(1L), BKey.of(10L), getArgs))
        .thenAccept(multiResult -> {
          assertEquals(3, multiResult.size());

          BTreeElements<Object> elements0 = multiResult.get(testKeys.get(0));
          assertEquals(2, elements0.getElements().size());
          assertTrue(elements0.getElements().containsKey(BKey.of(1L)));
          assertTrue(elements0.getElements().containsKey(BKey.of(2L)));

          BTreeElements<Object> elements1 = multiResult.get(testKeys.get(1));
          assertEquals(2, elements1.getElements().size());
          assertTrue(elements1.getElements().containsKey(BKey.of(3L)));
          assertTrue(elements1.getElements().containsKey(BKey.of(4L)));

          BTreeElements<Object> elements2 = multiResult.get(testKeys.get(2));
          assertEquals(1, elements2.getElements().size());
          assertTrue(elements2.getElements().containsKey(BKey.of(5L)));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopMGetElementNotFound() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();

    // when
    async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(r -> async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
        .thenCompose(r -> async.bopInsert(testKeys.get(1),
            new BTreeElement<>(BKey.of(3L), "value3", null), attrs))
        .thenCompose(r -> async.bopMultiGet(testKeys, BKey.of(10L), BKey.of(20L), getArgs))
        // then
        .thenAccept(multiResult -> {
          assertEquals(2, multiResult.size());

          BTreeElements<Object> elements0 = multiResult.get(testKeys.get(0));
          assertNotNull(elements0);
          assertEquals(0, elements0.getElements().size());

          BTreeElements<Object> elements1 = multiResult.get(testKeys.get(1));
          assertNotNull(elements1);
          assertEquals(0, elements1.getElements().size());

          BTreeElements<Object> elements2 = multiResult.get(testKeys.get(2));
          assertNull(elements2);
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetSuccess() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();

    async.bopInsert(testKeys.get(0), new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(r -> async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(3L), "value3", null), attrs))
        .thenCompose(r -> async.bopInsert(testKeys.get(1),
            new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
        .thenCompose(r -> async.bopInsert(testKeys.get(1),
            new BTreeElement<>(BKey.of(4L), "value4", null), attrs))
        .thenCompose(r -> async.bopInsert(testKeys.get(2),
            new BTreeElement<>(BKey.of(5L), "value5", null), attrs))
        // when
        .thenCompose(r -> async.bopSortMergeGet(testKeys,
            BKey.of(1L), BKey.of(10L), false, getArgs))
        // then
        .thenAccept(smGetResult -> {
          assertNotNull(smGetResult);
          assertEquals(5, smGetResult.getElements().size());

          // Verify elements are sorted by bkey
          List<SMGetResult.SMGetElement<Object>> elements = smGetResult.getElements();
          assertEquals(BKey.of(1L), elements.get(0).getElement().getBkey());
          assertEquals("value1", elements.get(0).getElement().getValue());
          assertEquals(testKeys.get(0), elements.get(0).getKey());

          assertEquals(BKey.of(2L), elements.get(1).getElement().getBkey());
          assertEquals("value2", elements.get(1).getElement().getValue());
          assertEquals(testKeys.get(1), elements.get(1).getKey());

          assertEquals(BKey.of(3L), elements.get(2).getElement().getBkey());
          assertEquals("value3", elements.get(2).getElement().getValue());
          assertEquals(testKeys.get(0), elements.get(2).getKey());

          assertEquals(BKey.of(4L), elements.get(3).getElement().getBkey());
          assertEquals("value4", elements.get(3).getElement().getValue());
          assertEquals(testKeys.get(1), elements.get(3).getKey());

          assertEquals(BKey.of(5L), elements.get(4).getElement().getBkey());
          assertEquals("value5", elements.get(4).getElement().getValue());
          assertEquals(testKeys.get(2), elements.get(4).getKey());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetUnique() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();


    async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(1L), "value1_from_key0", null), attrs)
        .thenCompose(r -> async.bopInsert(testKeys.get(1),
            new BTreeElement<>(BKey.of(1L), "value1_from_key1", null), attrs))
        // when
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(1L), BKey.of(1L), true, getArgs))
        // then
        .thenAccept(smGetResult -> {
          assertNotNull(smGetResult);
          assertEquals(1, smGetResult.getElements().size());

          SMGetResult.SMGetElement<Object> element = smGetResult.getElements().get(0);
          assertEquals(BKey.of(1L), element.getElement().getBkey());
          assertTrue(element.getElement().getValue().toString().startsWith("value1_from_"));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetWithMissedKeys() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), "nonexistent_key", keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();

    async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(r -> async.bopInsert(testKeys.get(2),
            new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
        // when
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(1L), BKey.of(10L), false, getArgs))
        // then
        .thenAccept(smGetResult -> {
          assertNotNull(smGetResult);

          // Should have 2 successful elements
          assertEquals(2, smGetResult.getElements().size());

          // Should have 1 missed key
          assertEquals(1, smGetResult.getMissedKeys().size());
          assertEquals("nonexistent_key", smGetResult.getMissedKeys().get(0).getKey());
          assertEquals(StatusCode.ERR_NOT_FOUND,
              smGetResult.getMissedKeys().get(0).getStatusCode());

          assertEquals(2, smGetResult.getElements().size());
          List<SMGetResult.SMGetElement<Object>> elements = smGetResult.getElements();
          assertEquals(BKey.of(1L), elements.get(0).getElement().getBkey());
          assertEquals("value1", elements.get(0).getElement().getValue());

          assertEquals(BKey.of(2L), elements.get(1).getElement().getBkey());
          assertEquals("value2", elements.get(1).getElement().getValue());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetNotFound() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();

    // when
    async.bopSortMergeGet(testKeys,
            BKey.of(10L), BKey.of(20L), false, getArgs)
        // then
        .thenAccept(smGetResult -> {
          assertNotNull(smGetResult);
          assertEquals(0, smGetResult.getElements().size());
          assertEquals(3, smGetResult.getMissedKeys().size());
          assertEquals(0, smGetResult.getTrimmedKeys().size());
          assertIterableEquals(testKeys, smGetResult.getMissedKeys().stream()
              .map(SMGetResult.MissedKey::getKey).collect(Collectors.toList()));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetElementNotFound() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();
    BopGetArgs getArgs = new BopGetArgs.Builder().count(10).build();

    // when
    async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(1L), "value1", null), attrs)
        .thenCompose(r -> async.bopInsert(testKeys.get(0),
            new BTreeElement<>(BKey.of(2L), "value2", null), attrs))
        .thenCompose(r -> async.bopInsert(testKeys.get(1),
            new BTreeElement<>(BKey.of(3L), "value3", null), attrs))
        .thenCompose(r -> async.bopSortMergeGet(testKeys,
            BKey.of(10L), BKey.of(20L), false, getArgs))
        // then
        .thenAccept(smGetResult -> {
          assertNotNull(smGetResult);
          assertEquals(0, smGetResult.getElements().size());
          assertEquals(1, smGetResult.getMissedKeys().size());
          assertEquals(0, smGetResult.getTrimmedKeys().size());

          assertEquals(testKeys.get(2), smGetResult.getMissedKeys().get(0).getKey());
          assertEquals(StatusCode.ERR_NOT_FOUND,
              smGetResult.getMissedKeys().get(0).getStatusCode());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }
}
