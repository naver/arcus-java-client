package net.spy.memcached.protocol.ascii;

import net.spy.memcached.OperationFactory;
import net.spy.memcached.OperationFactoryTestBase;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.MutatorOperation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class OperationFactoryTest extends OperationFactoryTestBase {

  @Override
  protected OperationFactory getOperationFactory() {
    return new AsciiOperationFactory();
  }

  @Test
  @Override
  public void testMutatorOperationIncrCloning() {
    int exp = 823862;
    long def = 28775;
    int by = 7735;
    MutatorOperation op = ofact.mutate(Mutator.incr, TEST_KEY, by, def,
            exp, genericCallback);

    MutatorOperation op2 = cloneOne(MutatorOperation.class, op);
    assertKey(op2);
    assertEquals(-1, op2.getExpiration());
    assertEquals(-1, op2.getDefault());
    assertEquals(by, op2.getBy());
    assertSame(Mutator.incr, op2.getType());
    assertCallback(op2);
  }

  @Test
  @Override
  public void testMutatorOperationDecrCloning() {
    int exp = 823862;
    long def = 28775;
    int by = 7735;
    MutatorOperation op = ofact.mutate(Mutator.decr, TEST_KEY, by, def,
            exp, genericCallback);

    MutatorOperation op2 = cloneOne(MutatorOperation.class, op);
    assertKey(op2);
    assertEquals(-1, op2.getExpiration());
    assertEquals(-1, op2.getDefault());
    assertEquals(by, op2.getBy());
    assertSame(Mutator.decr, op2.getType());
    assertCallback(op2);
  }

}
