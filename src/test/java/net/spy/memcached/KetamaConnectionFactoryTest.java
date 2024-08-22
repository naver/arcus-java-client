package net.spy.memcached;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A very basic test that the KetamaConnectionFactory returns both the correct
 * hash algorithm and the correct node locator.
 */
public class KetamaConnectionFactoryTest {

  /*
   * This *is* kinda lame, but it tests the specific differences from the
   * DefaultConnectionFactory.
   */
  @Test
  public void testCorrectTypes() {
    ConnectionFactory factory = new KetamaConnectionFactory();

    NodeLocator locator = factory.createLocator(new ArrayList<>());
    assertTrue(locator instanceof KetamaNodeLocator);

    assertEquals(HashAlgorithm.KETAMA_HASH, factory.getHashAlg());
  }
}
