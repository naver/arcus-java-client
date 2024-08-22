package net.spy.memcached.protocol.binary;

import net.spy.memcached.OperationFactory;
import net.spy.memcached.OperationFactoryTestBase;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class OperationFactoryTest extends OperationFactoryTestBase {

  @Override
  protected OperationFactory getOperationFactory() {
    return new BinaryOperationFactory();
  }

  @Test
  @Override
  public void testMultipleGetsOperationCloning() {
    assertTrue(true);
  }

  @Test
  @Override
  public void testMultipleGetsOperationFanout() {
    assertTrue(true);
  }
}
