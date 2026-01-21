package net.spy.memcached.v2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;
import net.spy.memcached.v2.vo.BTreeElements;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.SMGetElements;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BTreeAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  private static final List<BTreeElement<Object>> ELEMENTS = Arrays.asList(
      new BTreeElement<>(BKey.of(1L), "value1", null),
      new BTreeElement<>(BKey.of(2L), "value2", null),
      new BTreeElement<>(BKey.of(3L), "value3", null),
      new BTreeElement<>(BKey.of(4L), "value4", null),
      new BTreeElement<>(BKey.of(5L), "value5", null)
  );

  @Test
  void bopInsert() throws Exception {
    // given
    String key = keys.get(0);

    // when
    async.bopCreate(key, ElementValueType.STRING, new CollectionAttributes())
        .thenCompose(result -> {
          assertTrue(result);
          return async.bopInsert(key, ELEMENTS.get(0));
        })
        // then
        .thenAccept(Assertions::assertTrue)
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopInsertDifferentTypeAndGetDifferentElement() throws Exception {
    // given
    String key = keys.get(0);
    BTreeElement<Object> element = ELEMENTS.get(0);

    // when
    async.bopCreate(key, ElementValueType.LONG, new CollectionAttributes())
        .thenCompose(result -> {
          assertTrue(result);
          return async.bopInsert(key, element);
        })
        .thenCompose(result -> {
          assertTrue(result);
          return async.bopGet(key, element.getBkey(), BopGetArgs.DEFAULT);
        })
        // then
        .thenAccept(result -> {
          assertNotEquals(element, result);
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopInsertNotFound() throws Exception {
    // given & when
    async.bopInsert(keys.get(0), ELEMENTS.get(0))
        // then
        .thenAccept(Assertions::assertFalse)
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopInsertTypeMisMatch() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();

    // when
    async.set(key, 0, VALUE)
        .thenCompose(result -> {
          assertTrue(result);
          return async.bopInsert(key, ELEMENTS.get(0), attrs);
        })
        // then
        .exceptionally(throwable -> {
          assertNotNull(throwable);
          assertTrue(throwable.getCause().getMessage().contains("TYPE_MISMATCH"));
          return null;
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopInsertAndGetTrimmed() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(3);

    // when
    async.bopInsert(key, ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1), attrs))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2), attrs))
        .thenCompose(result -> async.bopInsertAndGetTrimmed(key, ELEMENTS.get(3), attrs))
        // then
        .thenAccept(result -> {
          assertTrue(result.getKey());
          BTreeElement<Object> trimmedElement = result.getValue();
          assertNotNull(trimmedElement);
          assertEquals(ELEMENTS.get(0), trimmedElement);
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopFailToInsertAndGetTrimmed() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(3);

    // when
    async.bopInsert(key, ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2)))
        .thenCompose(result -> async.bopInsertAndGetTrimmed(key, ELEMENTS.get(0)))
        // then
        .thenAccept(result -> {
          assertFalse(result.getKey());
          assertNull(result.getValue());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopUpsertAndGetTrimmed() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(3);

    // when
    async.bopInsert(key, ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2)))
        .thenCompose(result -> async.bopUpsertAndGetTrimmed(key,
            ELEMENTS.get(3)))
        // then
        .thenAccept(result -> {
          assertTrue(result.getKey());
          assertEquals(ELEMENTS.get(0), result.getValue());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopUpsertAndNotGetTrimmed() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(3);
    String newValue = "new_value";

    // when
    async.bopInsert(key, ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2)))
        .thenCompose(result -> async.bopUpsertAndGetTrimmed(key,
            new BTreeElement<>(BKey.of(1L), newValue, null)))
        // then
        .thenAccept(result -> {
          assertTrue(result.getKey());
          assertNull(result.getValue());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGet() throws Exception {
    // given
    String key = keys.get(0);
    CollectionAttributes attrs = new CollectionAttributes();

    // when
    async.bopInsert(key, ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2)))
        .thenCompose(result -> async.bopGet(key, BKey.of(1L), BKey.of(3L), BopGetArgs.DEFAULT))
        // then
        .thenAccept(elements -> {
          assertEquals(3, elements.getElements().size());

          int i = 0;
          for (BTreeElement<Object> element : elements.getElements()) {
            assertEquals(ELEMENTS.get(i++), element);
          }
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

    // when
    async.bopInsert(key, ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2)))
        .thenCompose(result -> async.bopGet(key, BKey.of(2L), getArgsWithDelete))
        // then
        .thenAccept(element -> assertEquals(ELEMENTS.get(1), element))
        .thenCompose(v -> async.bopGet(key, BKey.of(2L), BopGetArgs.DEFAULT))
        .thenAccept(element -> {
          // ELEMENT NOT FOUND
          assertNotNull(element);
          assertEquals(BKey.of(2L), element.getBkey());
          assertNull(element.getValue());
          assertNull(element.getEFlag());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGetWithDeleteAndDropIfEmpty() throws Exception {
    // given
    String key = keys.get(0);
    BopGetArgs getArgsWithDeleteAndDrop = new BopGetArgs.Builder()
        .withDelete()
        .dropIfEmpty()
        .count(10)
        .build();

    // when
    async.bopInsert(key, ELEMENTS.get(0), new CollectionAttributes())
        .thenCompose(result -> async.bopGet(key, BKey.of(1L), getArgsWithDeleteAndDrop))
        // then
        .thenAccept(element -> assertEquals(ELEMENTS.get(0), element))
        .thenCompose(v -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenAccept(Assertions::assertFalse) // NOT FOUND (key miss)
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGetRangeNotExistKey() {
    // given
    String key = keys.get(0);

    // when
    async.bopGet(key, BKey.of(1L), BKey.of(10L), BopGetArgs.DEFAULT)
        // then
        .thenAccept(Assertions::assertNull) // NOT FOUND (key miss)
        .toCompletableFuture()
        .join();
  }

  @Test
  void bopGetRangeNotExistElements() throws Exception {
    // given
    String key = keys.get(0);

    // when
    async.bopInsert(key, ELEMENTS.get(0), new CollectionAttributes())
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2)))
        .thenCompose(result -> async.bopGet(key,
            BKey.of(10L), BKey.of(20L), BopGetArgs.DEFAULT))
        // then
        .thenAccept(elements -> {
          // NOT FOUND ELEMENT
          assertNotNull(elements);
          assertEquals(0, elements.getElements().size());
        })
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

    // when
    async.bopInsert(key, ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1), attrs))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2), attrs))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(3), attrs))
        .thenCompose(result -> async.bopGet(key, BKey.of(2L), BKey.of(3L), getArgsWithDelete))
        // then
        .thenAccept(elements -> {
          assertNotNull(elements);
          assertEquals(2, elements.getElements().size());
          assertEquals(ELEMENTS.get(1), elements.getElements().get(0));
          assertEquals(ELEMENTS.get(2), elements.getElements().get(1));
        })
        .thenCompose(v -> async.bopGet(key, BKey.of(2L), BKey.of(3L), BopGetArgs.DEFAULT))
        .thenAccept(elements -> {
          assertNotNull(elements);
          assertTrue(elements.getElements().isEmpty());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopGetRangeDescending() throws Exception {
    // given
    String key = keys.get(0);

    // when
    async.bopInsert(key, ELEMENTS.get(0), new CollectionAttributes())
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(2)))
        .thenCompose(result -> async.bopInsert(key, ELEMENTS.get(3)))
        .thenCompose(result -> async.bopGet(key, BKey.of(10L), BKey.of(1L), BopGetArgs.DEFAULT))
        // then
        .thenAccept(elements -> {
          assertNotNull(elements);
          int i = 3;
          for (BTreeElement<Object> element : elements.getElements()) {
            assertEquals(ELEMENTS.get(i--), element);
          }
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }


  @Test
  void bopMultiGet() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attr = new CollectionAttributes();

    // when
    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attr)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(2), attr))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(3)))
        .thenCompose(result -> async.bopInsert(testKeys.get(2), ELEMENTS.get(4), attr))
        // then
        .thenCompose(result -> async
            .bopMultiGet(testKeys, BKey.of(1L), BKey.of(10L), BopGetArgs.DEFAULT))
        .thenAccept(map -> {
          assertEquals(3, map.size());

          BTreeElements<Object> elements0 = map.get(testKeys.get(0));
          assertEquals(2, elements0.getElements().size());
          assertEquals(ELEMENTS.get(0), elements0.getElements().get(0));
          assertEquals(ELEMENTS.get(1), elements0.getElements().get(1));

          BTreeElements<Object> elements1 = map.get(testKeys.get(1));
          assertEquals(2, elements1.getElements().size());
          assertEquals(ELEMENTS.get(2), elements1.getElements().get(0));
          assertEquals(ELEMENTS.get(3), elements1.getElements().get(1));

          BTreeElements<Object> elements2 = map.get(testKeys.get(2));
          assertEquals(1, elements2.getElements().size());
          assertEquals(ELEMENTS.get(4), elements2.getElements().get(0));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopMultiGetDescending() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attr = new CollectionAttributes();

    // when
    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attr)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(2), attr))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(3)))
        .thenCompose(result -> async.bopInsert(testKeys.get(2), ELEMENTS.get(4), attr))
        // then
        .thenCompose(result -> async
            .bopMultiGet(testKeys, BKey.of(10L), BKey.of(1L), BopGetArgs.DEFAULT))
        .thenAccept(map -> {
          assertEquals(3, map.size());

          BTreeElements<Object> elements0 = map.get(testKeys.get(0));
          assertEquals(2, elements0.getElements().size());
          BTreeElements<Object> elements1 = map.get(testKeys.get(1));
          assertEquals(2, elements1.getElements().size());
          BTreeElements<Object> elements2 = map.get(testKeys.get(2));
          assertEquals(1, elements2.getElements().size());

          // Make sure that the order is descending
          int i = 1;
          for (BTreeElement<Object> element : elements0.getElements()) {
            assertEquals(ELEMENTS.get(i--), element);
          }
          i = 3;
          for (BTreeElement<Object> element : elements1.getElements()) {
            assertEquals(ELEMENTS.get(i--), element);
          }

          assertEquals(ELEMENTS.get(4), elements2.getElements().get(0));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopMultiGetNotFoundElement() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();

    // when
    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async
            .bopMultiGet(testKeys, BKey.of(10L), BKey.of(20L), BopGetArgs.DEFAULT))
        // then
        .thenAccept(map -> {
          assertEquals(1, map.size());

          // NOT FOUND ELEMENT
          BTreeElements<Object> elements = map.get(testKeys.get(0));
          assertNotNull(elements);
          assertEquals(0, elements.getElements().size());

          // NOT FOUND
          assertFalse(map.containsKey(testKeys.get(1)));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetAscendingUnique() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();

    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(2)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(testKeys.get(2), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(2), ELEMENTS.get(3)))
        // when
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(1L), BKey.of(10L), true, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(4, smGetElements.getElements().size());

          List<SMGetElements.Element<Object>> elements = smGetElements.getElements();
          assertEquals(testKeys.get(0), elements.get(0).getKey());
          assertEquals(ELEMENTS.get(0), elements.get(0).getbTreeElement());
          assertEquals(testKeys.get(1), elements.get(1).getKey());
          assertEquals(ELEMENTS.get(1), elements.get(1).getbTreeElement());
          assertEquals(testKeys.get(0), elements.get(2).getKey());
          assertEquals(ELEMENTS.get(2), elements.get(2).getbTreeElement());
          assertEquals(testKeys.get(2), elements.get(3).getKey());
          assertEquals(ELEMENTS.get(3), elements.get(3).getbTreeElement());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetDescendingUnique() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();

    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(2)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(testKeys.get(2), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(2), ELEMENTS.get(3)))
        // when
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(10L), BKey.of(1L), true, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(4, smGetElements.getElements().size());

          List<SMGetElements.Element<Object>> elements = smGetElements.getElements();
          assertEquals(testKeys.get(2), elements.get(0).getKey());
          assertEquals(ELEMENTS.get(3), elements.get(0).getbTreeElement());
          assertEquals(testKeys.get(0), elements.get(1).getKey());
          assertEquals(ELEMENTS.get(2), elements.get(1).getbTreeElement());
          assertEquals(testKeys.get(1), elements.get(2).getKey());
          assertEquals(ELEMENTS.get(1), elements.get(2).getbTreeElement());
          assertEquals(testKeys.get(2), elements.get(3).getKey());
          assertEquals(ELEMENTS.get(0), elements.get(3).getbTreeElement());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetAscendingDuplicated() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();

    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(1)))
        // when
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(1L), BKey.of(2L), false, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(4, smGetElements.getElements().size());

          SMGetElements.Element<Object> element1 = smGetElements.getElements().get(0);
          assertEquals(ELEMENTS.get(0), element1.getbTreeElement());
          assertEquals(testKeys.get(0), element1.getKey());

          SMGetElements.Element<Object> element2 = smGetElements.getElements().get(1);
          assertEquals(ELEMENTS.get(0), element2.getbTreeElement());
          assertEquals(testKeys.get(1), element2.getKey());

          SMGetElements.Element<Object> element3 = smGetElements.getElements().get(2);
          assertEquals(ELEMENTS.get(1), element3.getbTreeElement());
          assertEquals(testKeys.get(0), element3.getKey());

          SMGetElements.Element<Object> element4 = smGetElements.getElements().get(3);
          assertEquals(ELEMENTS.get(1), element4.getbTreeElement());
          assertEquals(testKeys.get(1), element4.getKey());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetDescendingDuplicated() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();

    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(1)))
        // when
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(2L), BKey.of(1L), false, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(4, smGetElements.getElements().size());

          SMGetElements.Element<Object> element1 = smGetElements.getElements().get(0);
          assertEquals(ELEMENTS.get(1), element1.getbTreeElement());
          assertEquals(testKeys.get(1), element1.getKey());

          SMGetElements.Element<Object> element2 = smGetElements.getElements().get(1);
          assertEquals(ELEMENTS.get(1), element2.getbTreeElement());
          assertEquals(testKeys.get(0), element2.getKey());

          SMGetElements.Element<Object> element3 = smGetElements.getElements().get(2);
          assertEquals(ELEMENTS.get(0), element3.getbTreeElement());
          assertEquals(testKeys.get(1), element3.getKey());

          SMGetElements.Element<Object> element4 = smGetElements.getElements().get(3);
          assertEquals(ELEMENTS.get(0), element4.getbTreeElement());
          assertEquals(testKeys.get(0), element4.getKey());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetUnique() throws Exception {
    // given
    CollectionAttributes attrs = new CollectionAttributes();

    async.bopInsert(keys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(keys.get(1), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(keys.get(2), ELEMENTS.get(0), attrs))
        .thenCompose(result -> async.bopInsert(keys.get(3), ELEMENTS.get(0), attrs))
        // when
        .thenCompose(result -> async.bopSortMergeGet(keys,
            BKey.of(1L), BKey.of(3L), true, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(1, smGetElements.getElements().size());

          SMGetElements.Element<Object> element = smGetElements.getElements().get(0);
          assertEquals(ELEMENTS.get(0), element.getbTreeElement());
          assertEquals(keys.get(0), element.getKey());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetWithMissedKeys() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();

    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        // when
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(1L), BKey.of(10L), false, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);

          assertEquals(1, smGetElements.getElements().size());
          List<SMGetElements.Element<Object>> elements = smGetElements.getElements();
          assertEquals(ELEMENTS.get(0), elements.get(0).getbTreeElement());
          assertEquals(testKeys.get(0), elements.get(0).getKey());

          assertEquals(1, smGetElements.getMissedKeys().size());
          SMGetElements.MissedKey missedKey = smGetElements.getMissedKeys().get(0);
          assertEquals(testKeys.get(1), missedKey.getKey());
          assertEquals(StatusCode.ERR_NOT_FOUND, missedKey.getStatusCode());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetNotFound() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));

    // when
    async.bopSortMergeGet(testKeys,
            BKey.of(10L), BKey.of(20L), false, BopGetArgs.DEFAULT)
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(0, smGetElements.getElements().size());
          assertEquals(3, smGetElements.getMissedKeys().size());
          assertEquals(0, smGetElements.getTrimmedKeys().size());
          assertIterableEquals(testKeys, smGetElements.getMissedKeys().stream()
              .map(SMGetElements.MissedKey::getKey).collect(Collectors.toList()));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetNotFoundElement() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();

    // when
    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(2), attrs))
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(10L), BKey.of(20L), false, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(0, smGetElements.getElements().size());
          assertEquals(1, smGetElements.getMissedKeys().size());
          assertEquals(0, smGetElements.getTrimmedKeys().size());

          SMGetElements.MissedKey missedKey = smGetElements.getMissedKeys().get(0);
          assertEquals(testKeys.get(2), missedKey.getKey());
          assertEquals(StatusCode.ERR_NOT_FOUND, missedKey.getStatusCode());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetWithTrimmedKeys() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1), keys.get(2));
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(2);

    // when
    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1)))
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(2)))
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(3)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(1), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(2)))
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(10L), BKey.of(1L), false, BopGetArgs.DEFAULT))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(4, smGetElements.getElements().size());
          assertEquals(1, smGetElements.getMissedKeys().size());
          assertEquals(1, smGetElements.getTrimmedKeys().size());

          SMGetElements.TrimmedKey trimmedKey = smGetElements.getTrimmedKeys().get(0);
          assertEquals(keys.get(0), trimmedKey.getKey());
          assertEquals(ELEMENTS.get(2).getBkey(), trimmedKey.getBKey());

          List<SMGetElements.Element<Object>> elements = smGetElements.getElements();
          assertEquals(testKeys.get(0), elements.get(0).getKey());
          assertEquals(ELEMENTS.get(3), elements.get(0).getbTreeElement());
          assertEquals(testKeys.get(1), elements.get(1).getKey());
          assertEquals(ELEMENTS.get(2), elements.get(1).getbTreeElement());
          assertEquals(testKeys.get(0), elements.get(2).getKey());
          assertEquals(ELEMENTS.get(2), elements.get(2).getbTreeElement());
          assertEquals(testKeys.get(1), elements.get(3).getKey());
          assertEquals(ELEMENTS.get(1), elements.get(3).getbTreeElement());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetNotHaveTrimmedKeysOutOfElementsRangeDescending() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(2);

    // when
    async.bopInsert(testKeys.get(0), ELEMENTS.get(0), attrs) // to be trimmed
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1))) // to be trimmed
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(2)))
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(3)))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(2), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(3)))
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(10L), BKey.of(1L), false,
            new BopGetArgs.Builder().build()))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(4, smGetElements.getElements().size());
          assertTrue(smGetElements.getMissedKeys().isEmpty());
          assertTrue(smGetElements.getTrimmedKeys().isEmpty());

          List<SMGetElements.Element<Object>> elements = smGetElements.getElements();
          assertEquals(testKeys.get(1), elements.get(0).getKey());
          assertEquals(ELEMENTS.get(3), elements.get(0).getbTreeElement());
          assertEquals(testKeys.get(0), elements.get(1).getKey());
          assertEquals(ELEMENTS.get(3), elements.get(1).getbTreeElement());
          assertEquals(testKeys.get(1), elements.get(2).getKey());
          assertEquals(ELEMENTS.get(2), elements.get(2).getbTreeElement());
          assertEquals(testKeys.get(0), elements.get(3).getKey());
          assertEquals(ELEMENTS.get(2), elements.get(3).getbTreeElement());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void bopSortMergeGetNotHaveTrimmedKeysOutOfElementsRangeAscending() throws Exception {
    // given
    List<String> testKeys = Arrays.asList(keys.get(0), keys.get(1));
    CollectionAttributes attrs = new CollectionAttributes();
    attrs.setMaxCount(2);
    attrs.setOverflowAction(CollectionOverflowAction.largest_trim);

    // when
    async.bopInsert(testKeys.get(0), ELEMENTS.get(3), attrs)
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(2)))
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(1))) // to be trimmed
        .thenCompose(result -> async.bopInsert(testKeys.get(0), ELEMENTS.get(0))) // to be trimmed
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(1), attrs))
        .thenCompose(result -> async.bopInsert(testKeys.get(1), ELEMENTS.get(0)))
        .thenCompose(result -> async.bopSortMergeGet(testKeys,
            BKey.of(1L), BKey.of(10L), false,
            new BopGetArgs.Builder().build()))
        // then
        .thenAccept(smGetElements -> {
          assertNotNull(smGetElements);
          assertEquals(4, smGetElements.getElements().size());
          assertTrue(smGetElements.getMissedKeys().isEmpty());
          assertTrue(smGetElements.getTrimmedKeys().isEmpty());

          List<SMGetElements.Element<Object>> elements = smGetElements.getElements();
          assertEquals(testKeys.get(0), elements.get(0).getKey());
          assertEquals(ELEMENTS.get(0), elements.get(0).getbTreeElement());
          assertEquals(testKeys.get(1), elements.get(1).getKey());
          assertEquals(ELEMENTS.get(0), elements.get(1).getbTreeElement());
          assertEquals(testKeys.get(0), elements.get(2).getKey());
          assertEquals(ELEMENTS.get(1), elements.get(2).getbTreeElement());
          assertEquals(testKeys.get(1), elements.get(3).getKey());
          assertEquals(ELEMENTS.get(1), elements.get(3).getbTreeElement());
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }
}
