package net.spy.memcached.v2;

import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.internal.CompositeException;
import net.spy.memcached.v2.pipe.Pipeline;
import net.spy.memcached.v2.pipe.PipelineOperationException;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BTreeElement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineAsyncArcusCommandsTest extends AsyncArcusCommandsTest {

  @Test
  void pipelineMixedInsert() throws Exception {
    // given
    CollectionAttributes attr = new CollectionAttributes();

    // when
    Pipeline<Object> pipeline = async.pipeline()
        .lopInsert("list1", 0, "value1", attr)
        .sopInsert("set1", "value1", attr)
        .lopInsert("list1", 0, "value2")
        .sopInsert("set2", "value2")
        .bopInsert("btree1", new BTreeElement<>(BKey.of(1L), "value1", null), attr)
        .mopInsert("map1", "field1", "value1", attr)
        .mopInsert("map1", "field1", "value2", attr);

    async.execute(pipeline)
        // then
        .thenAccept(list -> {
          assertEquals(7, list.size());
          assertEquals(true, list.get(0));
          assertEquals(true, list.get(1));
          assertEquals(true, list.get(2));
          assertEquals(false, list.get(3));
          assertEquals(true, list.get(4));
          assertEquals(true, list.get(5));
          assertEquals(false, list.get(6));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void pipeline500OpsOnSingleKey() throws Exception {
    // given
    CollectionAttributes attr = new CollectionAttributes();
    String key = "btree_max";

    Pipeline<Object> pipeline = async.pipeline();
    for (long i = 0; i < 500; i++) {
      if (i == 0) {
        pipeline.bopInsert(key, new BTreeElement<>(BKey.of(i), "value" + i, null), attr);
      } else {
        pipeline.bopInsert(key, new BTreeElement<>(BKey.of(i), "value" + i, null));
      }
    }

    // when
    async.execute(pipeline)
        // then
        .thenAccept(list -> {
          assertEquals(500, list.size());
          for (int i = 0; i < 500; i++) {
            assertEquals(true, list.get(i));
          }
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void pipeline500OpsOnMultiKeys() throws Exception {
    // given
    CollectionAttributes attr = new CollectionAttributes();

    Pipeline<Object> pipeline = async.pipeline();
    for (int keyIdx = 0; keyIdx < 10; keyIdx++) {
      String key = "btree_multi_" + keyIdx;
      for (long i = 0; i < 50; i++) {
        if (i == 0) {
          pipeline.bopInsert(key, new BTreeElement<>(BKey.of(i), "value" + i, null), attr);
        } else {
          pipeline.bopInsert(key, new BTreeElement<>(BKey.of(i), "value" + i, null));
        }
      }
    }

    // when
    async.execute(pipeline)
        // then
        .thenAccept(list -> {
          assertEquals(500, list.size());
          for (int i = 0; i < 500; i++) {
            assertEquals(true, list.get(i));
          }
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void pipelineExceed500OpsThrowsException() {
    // given
    CollectionAttributes attr = new CollectionAttributes();
    String key = "btree_exceed";

    Pipeline<Object> pipeline = async.pipeline();
    for (long i = 0; i < 500; i++) {
      if (i == 0) {
        pipeline.bopInsert(key, new BTreeElement<>(BKey.of(i), "value" + i, null), attr);
      } else {
        pipeline.bopInsert(key, new BTreeElement<>(BKey.of(i), "value" + i, null));
      }
    }

    // when & then
    BTreeElement<Object> element = new BTreeElement<>(BKey.of(501L), "value501", null);
    assertThrows(IllegalStateException.class, () -> pipeline.bopInsert(key, element));
  }

  @Test
  void pipelineSingleKeyWithMixedOperations() throws Exception {
    // given
    CollectionAttributes attr = new CollectionAttributes();
    String key = "btree_mixed";

    Pipeline<Object> pipeline = async.pipeline()
        .bopInsert(key, new BTreeElement<>(BKey.of(1L), "value1", null), attr)
        .bopInsert(key, new BTreeElement<>(BKey.of(2L), "value2", null))
        .bopUpsert(key, new BTreeElement<>(BKey.of(1L), "updated1", null))
        .bopDelete(key, BKey.of(3L));

    // when
    async.execute(pipeline)
        // then
        .thenAccept(list -> {
          assertEquals(4, list.size());
          assertEquals(true, list.get(0));
          assertEquals(true, list.get(1));
          assertEquals(true, list.get(2));
          assertEquals(false, list.get(3));
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void pipelineFailedOnce() throws Exception {
    // given
    CollectionAttributes attr = new CollectionAttributes();

    // when
    Pipeline<Object> pipeline = async.pipeline()
        .lopInsert("list1", 0, "value1", attr)
        .sopInsert("list1", "value1") // TYPE_MISMATCH
        .lopInsert("list2", 0, "value2", attr)
        .lopInsert("list3", 0, "value3", attr)
        .lopInsert("list4", 0, "value4", attr)
        .lopInsert("list5", 0, "value5", attr)
        .lopInsert("list6", 0, "value6", attr);

    ArcusMultiFuture<List<Boolean>> future =
        (ArcusMultiFuture<List<Boolean>>) async.execute(pipeline);
    future
        // then
        .handle((result, throwable) -> {
          PipelineOperationException ex = (PipelineOperationException) throwable;
          assertTrue(ex.getMessage()
              .contains("Pipeline operation at index 1 failed: { message: TYPE_MISMATCH }"));

          List<Boolean> resultsWithFailures = future.getResultsWithFailures();
          assertEquals(7, resultsWithFailures.size());
          assertEquals(true, resultsWithFailures.get(0));
          assertNull(resultsWithFailures.get(1)); // TYPE_MISMATCH
          assertEquals(true, resultsWithFailures.get(2));
          assertEquals(true, resultsWithFailures.get(3));
          assertEquals(true, resultsWithFailures.get(4));
          assertEquals(true, resultsWithFailures.get(5));
          assertEquals(true, resultsWithFailures.get(6));
          return null;
        })
        .toCompletableFuture()
        .get(300, TimeUnit.MILLISECONDS);
  }

  @Test
  void pipelineFailedTwice() throws Exception {
    // given
    CollectionAttributes attr = new CollectionAttributes();

    // when
    Pipeline<Object> pipeline = async.pipeline()
        .lopInsert("list1", 0, "value1", attr)
        .sopInsert("list1", "value1") // TYPE_MISMATCH
        .lopInsert("list2", 0, "value2", attr)
        .lopInsert("list3", 0, "value3",
            new CollectionAttributes(0, 1L, CollectionOverflowAction.error))
        .lopInsert("list3", 0, "value3", attr) // OVERFLOWED
        .lopInsert("list4", 0, "value4", attr)
        .lopInsert("list5", 0, "value5", attr)
        .lopInsert("list6", 0, "value6", attr);

    ArcusMultiFuture<List<Boolean>> future =
        (ArcusMultiFuture<List<Boolean>>) async.execute(pipeline);
    future
        // then
        .handle((result, throwable) -> {
          CompositeException ex = (CompositeException) throwable;
          assertEquals(2, ex.getExceptions().size());
          assertTrue(ex.getExceptions().get(0).getMessage()
              .contains("Pipeline operation at index 1 failed: { message: TYPE_MISMATCH }"));
          assertTrue(ex.getExceptions().get(1).getMessage()
              .contains("Pipeline operation at index 4 failed: { message: OVERFLOWED }"));

          List<Boolean> resultsWithFailures = future.getResultsWithFailures();
          assertEquals(8, resultsWithFailures.size());
          assertEquals(true, resultsWithFailures.get(0));
          assertNull(resultsWithFailures.get(1)); // TYPE_MISMATCH
          assertEquals(true, resultsWithFailures.get(2));
          assertEquals(true, resultsWithFailures.get(3));
          assertNull(resultsWithFailures.get(4)); // OVERFLOWED
          assertEquals(true, resultsWithFailures.get(5));
          assertEquals(true, resultsWithFailures.get(6));
          assertEquals(true, resultsWithFailures.get(7));
          return null;
        })
        .toCompletableFuture()
        .get(300000, TimeUnit.MILLISECONDS);
  }

  @Test
  void pipelineEmptyThrowsException() {
    // given
    Pipeline<Object> pipeline = async.pipeline();

    // when & then
    assertThrows(IllegalArgumentException.class, () -> async.execute(pipeline));
  }
}
