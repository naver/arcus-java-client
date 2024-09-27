package net.spy.memcached;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test connection factory variations.
 */
class ConnectionFactoryTest {

  // These tests are a little lame.  They don't verify anything other than
  // that the code executes without failure.
  @Test
  void testBinaryEmptyCons() {
    new BinaryConnectionFactory();
  }

  @Test
  void testBinaryTwoIntCons() {
    new BinaryConnectionFactory(5, 5);
  }

  @Test
  void testBinaryAnIntAnotherIntAndAHashAlgorithmCons() {
    new BinaryConnectionFactory(5, 5,
            HashAlgorithm.FNV1_64_HASH);
  }

  @Test
  void testQueueSizes() {
    ConnectionFactory cf = new DefaultConnectionFactory(100, 1024);
    assertEquals(100, cf.createOperationQueue().remainingCapacity());
    assertEquals(Integer.MAX_VALUE,
            cf.createWriteOperationQueue().remainingCapacity());
    assertEquals(Integer.MAX_VALUE,
            cf.createReadOperationQueue().remainingCapacity());
  }
}
