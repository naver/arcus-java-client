package net.spy.memcached.collection.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.internal.BTreeStoreAndGetFuture;
import net.spy.memcached.internal.BroadcastFuture;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.CollectionGetBulkFuture;
import net.spy.memcached.internal.CollectionGetFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.PipedCollectionFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class CancelFutureTest extends BaseIntegrationTest {

  @Test
  void cancelWithOperationFuture() {
    // given
    OperationFuture<Boolean> future = mc.set("key", 0, "value");

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    OperationStatus status = future.getStatus();
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status instanceof CollectionOperationStatus);
    assertFalse(status.isSuccess());
    assertEquals("cancelled", status.getMessage());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithGetFuture() {
    // given
    GetFuture<Object> future = mc.asyncGet("key");

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    OperationStatus status = future.getStatus();
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status instanceof CollectionOperationStatus);
    assertFalse(status.isSuccess());
    assertEquals("cancelled", status.getMessage());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithBroadcastFuture() throws ExecutionException, InterruptedException {
    // given
    mc.set("prefix:key1", 0, "value1").get();
    BroadcastFuture<Boolean> future = (BroadcastFuture<Boolean>) mc.flush("prefix");

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    OperationStatus status = future.getStatus();
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status instanceof CollectionOperationStatus);
    assertFalse(status.isSuccess());
    assertEquals("cancelled", status.getMessage());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithCollectionFuture() {
    // given
    CollectionFuture<List<Object>> future = mc.asyncLopGet("list", 0, false, false);

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    OperationStatus status = future.getStatus();
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status.isSuccess());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());

    CollectionOperationStatus operationStatus = future.getOperationStatus();
    assertInstanceOf(CollectionOperationStatus.class, operationStatus);
    assertInstanceOf(OperationStatus.class, operationStatus);
    assertFalse(operationStatus.isSuccess());
    assertEquals(StatusCode.CANCELLED, operationStatus.getStatusCode());
    assertEquals(CollectionResponse.CANCELED, operationStatus.getResponse());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithPipedCollectionFuture() {
    // given
    PipedCollectionFuture<Integer, CollectionOperationStatus> future =
            (PipedCollectionFuture<Integer, CollectionOperationStatus>)
                    mc.asyncLopPipedInsertBulk("list", 0, Arrays.asList("value1", "value2"), null);

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    OperationStatus status = future.getStatus();
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status.isSuccess());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());

    CollectionOperationStatus operationStatus = future.getOperationStatus();
    assertInstanceOf(CollectionOperationStatus.class, operationStatus);
    assertInstanceOf(OperationStatus.class, operationStatus);
    assertFalse(operationStatus.isSuccess());
    assertEquals(StatusCode.CANCELLED, operationStatus.getStatusCode());
    assertEquals(CollectionResponse.CANCELED, operationStatus.getResponse());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithBTreeStoreAndGetFuture() {
    // given
    BTreeStoreAndGetFuture<Boolean, Object> future =
            mc.asyncBopInsertAndGetTrimmed("btree", 0, null, "value", null);

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    OperationStatus status = future.getStatus();
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status.isSuccess());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());

    CollectionOperationStatus operationStatus = future.getOperationStatus();
    assertInstanceOf(CollectionOperationStatus.class, operationStatus);
    assertInstanceOf(OperationStatus.class, operationStatus);
    assertFalse(operationStatus.isSuccess());
    assertEquals(StatusCode.CANCELLED, operationStatus.getStatusCode());
    assertEquals(CollectionResponse.CANCELED, operationStatus.getResponse());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithCollectionGetFuture() {
    // given
    CollectionGetFuture<List<Object>> future =
            (CollectionGetFuture<List<Object>>) mc.asyncLopGet("list", 0, false, false);

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    OperationStatus status = future.getStatus();
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status.isSuccess());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());

    CollectionOperationStatus operationStatus = future.getOperationStatus();
    assertInstanceOf(CollectionOperationStatus.class, operationStatus);
    assertInstanceOf(OperationStatus.class, operationStatus);
    assertFalse(operationStatus.isSuccess());
    assertEquals(StatusCode.CANCELLED, operationStatus.getStatusCode());
    assertEquals(CollectionResponse.CANCELED, operationStatus.getResponse());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithCollectionGetBulkFuture() {
    // given
    CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> future =
            mc.asyncBopGetBulk(Arrays.asList("btree1", "btree2"),
                    0, 10, ElementFlagFilter.DO_NOT_FILTER, 0, 50);

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    CollectionOperationStatus status = future.getOperationStatus();
    assertInstanceOf(CollectionOperationStatus.class, status);
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status.isSuccess());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());
    assertEquals(CollectionResponse.CANCELED, status.getResponse());

    assertTrue(future.isCancelled());
  }

  @Test
  void cancelWithSMGetFuture() {
    // given
    SMGetFuture<List<SMGetElement<Object>>> future = mc.asyncBopSortMergeGet(
            Arrays.asList("btree1", "btree2"),
            0, 10, ElementFlagFilter.DO_NOT_FILTER, 100, SMGetMode.UNIQUE);

    // when
    try {
      future.cancel(true);
    } catch (Exception e) {
      fail("Expected cancel to not throw an exception", e);
    }

    // then
    CollectionOperationStatus status = future.getOperationStatus();
    assertInstanceOf(CollectionOperationStatus.class, status);
    assertInstanceOf(OperationStatus.class, status);
    assertFalse(status.isSuccess());
    assertEquals(StatusCode.CANCELLED, status.getStatusCode());
    assertEquals(CollectionResponse.CANCELED, status.getResponse());

    assertTrue(future.isCancelled());
  }

}
