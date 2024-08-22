package net.spy.memcached.ops;

import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.internal.OperationFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OperationStatusTest extends BaseIntegrationTest {
  private static final int EXP = 100;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mc.flush().get();
  }

  @Test
  public void testStore_success() throws Exception {
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
  public void testStore_fail() throws Exception {
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
  public void testIncrAndDecr_success() throws Exception {
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
  public void testIncrAndDecr_fail() throws Exception {
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
  public void testDelete_success() throws Exception {
    //given
    String value = "example";

    mc.add("abc", EXP, value).get();

    //when
    OperationFuture<Boolean> deleteOperationFuture = mc.delete("abc");

    //then
    assertEquals(StatusCode.SUCCESS, deleteOperationFuture.getStatus().getStatusCode());
  }

  @Test
  public void testDelete_fail() throws Exception {
    //given
    String value = "example";

    mc.add("abc", EXP, value).get();

    //when
    OperationFuture<Boolean> deleteOperationFuture = mc.delete("bc");

    //then
    assertEquals(StatusCode.ERR_NOT_FOUND, deleteOperationFuture.getStatus().getStatusCode());
  }
}
