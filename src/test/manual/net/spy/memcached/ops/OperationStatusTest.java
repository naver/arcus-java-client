package net.spy.memcached.ops;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.internal.OperationFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class OperationStatusTest extends BaseIntegrationTest {
  private static final int EXP = 100;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.flush().get();
  }

  @Test
  void testStore_success() throws Exception {
    //given
    String value = "value1";
    String replaceValue = "value2";
    String appendValue = "plus";

    //when
    OperationFuture<Boolean> addOperationFuture = mc.add("abc", EXP, value);
    OperationFuture<Boolean> setOperationFuture = mc.set("abc", EXP, value);
    OperationFuture<Boolean> replaceOperationFuture = mc.replace("abc", EXP, replaceValue);
    OperationFuture<Boolean> prependOperationFuture = mc.prepend(10, "abc", appendValue);
    OperationFuture<Boolean> appendOperationFuture = mc.append(10, "abc", appendValue);

    mc.add("bcc", EXP, value).get();
    mc.add("cbd", EXP, "2222").get();
    mc.add("efg", EXP, "3333").get();

    //then
    assertEquals(StatusCode.SUCCESS, addOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.SUCCESS, setOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.SUCCESS, replaceOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.SUCCESS, prependOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.SUCCESS, appendOperationFuture.getStatus().getStatusCode());
  }

  @Test
  void testStore_fail() throws Exception {
    //given
    String value = "value1";
    String replaceValue = "value2";
    String appendValue = "plus";

    //when
    mc.add("abc", EXP, value).get();

    // add already stored key
    OperationFuture<Boolean> addOperationFuture = mc.add("abc", EXP, replaceValue);
    // replace using non-exist key
    OperationFuture<Boolean> replaceOperationFuture = mc.replace("bc", EXP, replaceValue);
    // prepend using non-exist key
    OperationFuture<Boolean> prependOperationFuture = mc.prepend(102, "cda", appendValue);
    // append using non-exist key
    OperationFuture<Boolean> appendOperationFuture = mc.append(102, "efg", appendValue);

    //then
    assertEquals(StatusCode.ERR_NOT_STORED, addOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.ERR_NOT_STORED, replaceOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.ERR_NOT_STORED, prependOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.ERR_NOT_STORED, appendOperationFuture.getStatus().getStatusCode());
  }

  @Test
  void testIncrAndDecr_success() throws Exception {
    //given
    String key = "key";
    String value = "65";
    int value2 = 61;
    mc.set(key, EXP, value).get();

    //when
    OperationFuture<Long> incrOperationFuture = mc.asyncIncr(key, value2);
    OperationFuture<Long> decrOperationFuture = mc.asyncDecr(key, value2);

    //then
    assertEquals(StatusCode.SUCCESS, incrOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.SUCCESS, decrOperationFuture.getStatus().getStatusCode());
  }

  @Test
  void testIncrAndDecr_fail() throws Exception {
    //given
    int value = 1;
    int value2 = 2;

    mc.set("abc", EXP, value).get();

    //when
    OperationFuture<Long> incrOperationFuture = mc.asyncIncr("bc", value2);
    OperationFuture<Long> decrOperationFuture = mc.asyncDecr("bc", value2);

    //then
    assertEquals(StatusCode.ERR_NOT_FOUND, incrOperationFuture.getStatus().getStatusCode());
    assertEquals(StatusCode.ERR_NOT_FOUND, decrOperationFuture.getStatus().getStatusCode());
  }

  @Test
  void testDelete_success() throws Exception {
    //given
    String value = "example";

    mc.add("abc", EXP, value).get();

    //when
    OperationFuture<Boolean> deleteOperationFuture = mc.delete("abc");

    //then
    assertEquals(StatusCode.SUCCESS, deleteOperationFuture.getStatus().getStatusCode());
  }

  @Test
  void testDelete_fail() throws Exception {
    //given
    String value = "example";

    mc.add("abc", EXP, value).get();

    //when
    OperationFuture<Boolean> deleteOperationFuture = mc.delete("bc");

    //then
    assertEquals(StatusCode.ERR_NOT_FOUND, deleteOperationFuture.getStatus().getStatusCode());
  }

  @Test
  void equalsAndHashCodeTest() {
    // given
    OperationStatus status1 = new OperationStatus(true, "SUCCESS", StatusCode.SUCCESS);
    OperationStatus status2 = new OperationStatus(true, "SUCCESS", StatusCode.SUCCESS);
    OperationStatus status3 = new OperationStatus(false, "FAILURE", StatusCode.ERR_NOT_FOUND);

    // when, then
    assertEquals(status1, status2);
    assertNotEquals(status1, status3);
    assertEquals(status1.hashCode(), status2.hashCode());
    assertNotEquals(status1.hashCode(), status3.hashCode());
  }

  @Test
  void collectionOperationStatusContainsStatusCode() {
    OperationStatus os2 = new OperationStatus(false, "OK", StatusCode.SUCCESS);
    OperationStatus os = new OperationStatus(false, "NOT_FOUND", StatusCode.ERR_NOT_FOUND);
    OperationStatus os3 = new OperationStatus(false, "EXCEPTION", StatusCode.EXCEPTION);

    CollectionOperationStatus cos = new CollectionOperationStatus(os);
    CollectionOperationStatus cos2 = new CollectionOperationStatus(os2);
    CollectionOperationStatus cos3 = new CollectionOperationStatus(os3);
    assertEquals(StatusCode.ERR_NOT_FOUND, cos.getStatusCode());
    assertEquals(StatusCode.SUCCESS, cos2.getStatusCode());
    assertEquals(StatusCode.EXCEPTION, cos3.getStatusCode());
  }

  @Test
  void collectionOperationStatusContainsStatusCodeSuccess() {
    List<CollectionOperationStatus> statusList = new ArrayList<>();
    statusList.add(new CollectionOperationStatus(false, "OK",
            CollectionResponse.OK));
    statusList.add(new CollectionOperationStatus(false, "END",
            CollectionResponse.END));
    statusList.add(new CollectionOperationStatus(false, "STORED",
            CollectionResponse.STORED));
    statusList.add(new CollectionOperationStatus(false, "DELETED",
            CollectionResponse.DELETED));
    statusList.add(new CollectionOperationStatus(false, "DELETED_DROPPED",
            CollectionResponse.DELETED_DROPPED));
    statusList.add(new CollectionOperationStatus(false, "CREATE",
            CollectionResponse.CREATED));
    statusList.add(new CollectionOperationStatus(false, "CREATED_STORED",
            CollectionResponse.CREATED_STORED));
    statusList.add(new CollectionOperationStatus(false, "REPLACED",
            CollectionResponse.REPLACED));
    statusList.add(new CollectionOperationStatus(false, "UPDATED",
            CollectionResponse.UPDATED));

    for (CollectionOperationStatus status : statusList) {
      assertEquals(StatusCode.SUCCESS, status.getStatusCode());
    }
  }
}
