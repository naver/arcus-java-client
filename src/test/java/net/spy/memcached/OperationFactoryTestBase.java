package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import net.spy.memcached.ops.CASOperation;
import net.spy.memcached.ops.ConcatenationOperation;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.KeyedOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jmock.Expectations;
import org.jmock.Mockery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for operation factory tests.
 */
public abstract class OperationFactoryTestBase {

  public final static String TEST_KEY = "someKey";
  protected OperationFactory ofact = null;
  protected OperationCallback genericCallback;
  private byte[] testData;
  private Mockery context;

  @BeforeEach
  protected void setUp() throws Exception {
    ofact = getOperationFactory();
    context = new Mockery();
    genericCallback = new OperationCallback() {
      public void complete() {
        fail("Unexpected invocation");
      }

      public void receivedStatus(OperationStatus status) {
        fail("Unexpected status:  " + status);
      }
    };

    testData = new byte[64];
    new Random().nextBytes(testData);
  }

  @AfterEach
  protected void tearDown() throws Exception {
    context.assertIsSatisfied();
  }

  /**
   * Get the operation factory used by the tests.
   */
  protected abstract OperationFactory getOperationFactory();

  @Test
  public void testDeleteOperationCloning() {
    DeleteOperation op = ofact.delete(TEST_KEY, genericCallback);

    DeleteOperation op2 = cloneOne(DeleteOperation.class, op);
    assertEquals(TEST_KEY, op2.getKeys().iterator().next());
    assertCallback(op2);
  }

  @Test
  public void testCASOperationCloning() {
    CASOperation op = ofact.cas(StoreType.set,
            "someKey", 727582, 8174, 7175, testData, genericCallback);

    CASOperation op2 = cloneOne(CASOperation.class, op);
    assertKey(op2);
    assertEquals(727582, op2.getCasValue());
    assertEquals(8174, op2.getFlags());
    assertEquals(7175, op2.getExpiration());
    assertBytes(op2.getBytes());
    assertCallback(op2);
  }

  @Test
  public void testMutatorOperationIncrCloning() {
    int exp = 823862;
    long def = 28775;
    int by = 7735;
    MutatorOperation op = ofact.mutate(Mutator.incr, TEST_KEY, by, def,
            exp, genericCallback);

    MutatorOperation op2 = cloneOne(MutatorOperation.class, op);
    assertKey(op2);
    assertEquals(exp, op2.getExpiration());
    assertEquals(def, op2.getDefault());
    assertEquals(by, op2.getBy());
    assertSame(Mutator.incr, op2.getType());
    assertCallback(op2);
  }

  @Test
  public void testMutatorOperationDecrCloning() {
    int exp = 823862;
    long def = 28775;
    int by = 7735;
    MutatorOperation op = ofact.mutate(Mutator.decr, TEST_KEY, by, def,
            exp, genericCallback);

    MutatorOperation op2 = cloneOne(MutatorOperation.class, op);
    assertKey(op2);
    assertEquals(exp, op2.getExpiration());
    assertEquals(def, op2.getDefault());
    assertEquals(by, op2.getBy());
    assertSame(Mutator.decr, op2.getType());
    assertCallback(op2);
  }

  @Test
  public void testStoreOperationAddCloning() {
    int exp = 823862;
    int flags = 7735;
    StoreOperation op = ofact.store(StoreType.add, TEST_KEY,
            flags, exp, testData, genericCallback);

    StoreOperation op2 = cloneOne(StoreOperation.class, op);
    assertKey(op2);
    assertEquals(exp, op2.getExpiration());
    assertEquals(flags, op2.getFlags());
    assertSame(StoreType.add, op2.getStoreType());
    assertCallback(op2);
  }

  @Test
  public void testStoreOperationSetCloning() {
    int exp = 823862;
    int flags = 7735;
    StoreOperation op = ofact.store(StoreType.set, TEST_KEY,
            flags, exp, testData, genericCallback);

    StoreOperation op2 = cloneOne(StoreOperation.class, op);
    assertKey(op2);
    assertEquals(exp, op2.getExpiration());
    assertEquals(flags, op2.getFlags());
    assertSame(StoreType.set, op2.getStoreType());
    assertCallback(op2);
  }

  @Test
  public void testConcatenationOperationAppendCloning() {
    long casId = 82757248;
    ConcatenationOperation op = ofact.cat(ConcatenationType.append, casId,
            TEST_KEY, testData, genericCallback);

    ConcatenationOperation op2 = cloneOne(
            ConcatenationOperation.class, op);
    assertKey(op2);
    assertSame(ConcatenationType.append, op2.getStoreType());
    assertCallback(op2);
  }

  @Test
  public void testConcatenationOperationPrependCloning() {
    long casId = 82757248;
    ConcatenationOperation op = ofact.cat(ConcatenationType.prepend, casId,
            TEST_KEY, testData, genericCallback);

    ConcatenationOperation op2 = cloneOne(
            ConcatenationOperation.class, op);
    assertKey(op2);
    assertSame(ConcatenationType.prepend, op2.getStoreType());
    assertCallback(op2);
  }

  @Test
  public void testSingleGetOperationCloning() {
    GetOperation.Callback callback = context.mock(GetOperation.Callback.class);
    GetOperation op = ofact.get(TEST_KEY, callback);

    GetOperation op2 = cloneOne(GetOperation.class, op);
    assertKey(op2);
    assertSame(callback, op.getCallback());
  }

  @Test
  public void testSingleGetsOperationCloning() {
    GetsOperation.Callback callback = context.mock(GetsOperation.Callback.class);
    GetsOperation op = ofact.gets(TEST_KEY, callback);

    GetsOperation op2 = cloneOne(GetsOperation.class, op);
    assertKey(op2);
    assertSame(callback, op.getCallback());
  }

  // These are harder cases as they fan out.
  @Test
  public void testMultipleGetOperationCloning() {
    Collection<String> keys = Arrays.asList("k1", "k2", "k3");
    GetOperation.Callback callback = context.mock(GetOperation.Callback.class);
    GetOperation op = ofact.get(keys, callback);

    Collection<Operation> ops = ofact.clone(op);
    assertEquals(3, ops.size());

    Collection<String> mutableKeys = new ArrayList<>(keys);
    int i = 3;
    for (Operation o : ops) {
      assertEquals(i, mutableKeys.size()); // Starting size
      GetOperation go = (GetOperation) o;
      mutableKeys.removeAll(go.getKeys());
      // Verify we matched and removed 1
      assertEquals(--i, mutableKeys.size());
    }
  }

  @Test
  public void testMultipleGetOperationFanout() {
    Collection<String> keys = Arrays.asList("k1", "k2", "k3");
    GetOperation.Callback callback = context.mock(GetOperation.Callback.class);
    OperationStatus st = new OperationStatus(true, "blah");

    context.checking(
            new Expectations() {{
              oneOf(callback).complete();
              oneOf(callback).receivedStatus(with(same(st)));
              oneOf(callback).gotData(with("k1"), with(1), with(any(byte[].class)));
              oneOf(callback).gotData(with("k2"), with(2), with(any(byte[].class)));
              oneOf(callback).gotData(with("k3"), with(3), with(any(byte[].class)));
            }}
    );

    GetOperation op = ofact.get(keys, callback);

    // Transition each operation callback into the complete state.
    Iterator<String> ki = keys.iterator();
    int i = 0;
    for (Operation o : ofact.clone(op)) {
      GetOperation.Callback cb = (GetOperation.Callback) o.getCallback();
      cb.gotData(ki.next(), ++i, new byte[3]);
      cb.receivedStatus(st);
      cb.complete();
    }
  }

  // These are harder cases as they fan out.
  @Test
  public void testMultipleGetsOperationCloning() {
    Collection<String> keys = Arrays.asList("k1", "k2", "k3");
    GetsOperation.Callback callback = context.mock(GetsOperation.Callback.class);
    GetsOperation op = ofact.gets(keys, callback);

    Collection<Operation> ops = ofact.clone(op);
    assertEquals(3, ops.size());

    Collection<String> mutableKeys = new ArrayList<>(keys);
    int i = 3;
    for (Operation o : ops) {
      assertEquals(i, mutableKeys.size()); // Starting size
      GetsOperation go = (GetsOperation) o;
      mutableKeys.removeAll(go.getKeys());
      // Verify we matched and removed 1
      assertEquals(--i, mutableKeys.size());
    }
  }

  @Test
  public void testMultipleGetsOperationFanout() {
    long[] casId = {82757248, 82757249, 82757250};
    Collection<String> keys = Arrays.asList("k1", "k2", "k3");
    GetsOperation.Callback callback = context.mock(GetsOperation.Callback.class);
    OperationStatus st = new OperationStatus(true, "blah");

    context.checking(
            new Expectations() {{
              oneOf(callback).complete();
              oneOf(callback).receivedStatus(with(same(st)));
              oneOf(callback).gotData(with("k1"), with(1), with(casId[0]), with(any(byte[].class)));
              oneOf(callback).gotData(with("k2"), with(2), with(casId[1]), with(any(byte[].class)));
              oneOf(callback).gotData(with("k3"), with(3), with(casId[2]), with(any(byte[].class)));
            }}
    );

    GetsOperation op = ofact.gets(keys, callback);

    // Transition each operation callback into the complete state.
    Iterator<String> ki = keys.iterator();
    int i = 0;
    for (Operation o : ofact.clone(op)) {
      GetsOperation.Callback cb = (GetsOperation.Callback) o.getCallback();
      cb.gotData(ki.next(), ++i, casId[i - 1], new byte[3]);
      cb.receivedStatus(st);
      cb.complete();
    }
  }

  protected void assertKey(KeyedOperation op) {
    assertEquals(TEST_KEY, op.getKeys().iterator().next());
  }

  protected void assertCallback(Operation op) {
    assertSame(genericCallback, op.getCallback());
  }

  private void assertBytes(byte[] bytes) {
    assertTrue(Arrays.equals(testData, bytes));
  }

  private <T> T assertOne(Class<T> class1, Collection<Operation> ops) {
    assertEquals(1, ops.size());

    @SuppressWarnings("unchecked") T op = (T) ops.iterator().next();
    return op;
  }

  protected <T> T cloneOne(Class<T> c, KeyedOperation t) {
    return assertOne(c, ofact.clone(t));
  }

}
