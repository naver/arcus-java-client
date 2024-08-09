package net.spy.memcached;

import java.util.concurrent.Callable;

import net.spy.memcached.compat.SyncThread;
import net.spy.memcached.transcoders.LongTranscoder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test the CAS mutator.
 */
public class CASMutatorTest extends ClientBaseCase {

  private CASMutation<Long> mutation;
  private CASMutator<Long> mutator;

  @BeforeEach
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mutation = new CASMutation<Long>() {
      public Long getNewValue(Long current) {
        return current + 1;
      }
    };
    mutator = new CASMutator<>(client, new LongTranscoder(), 50);
  }

  @Test
  public void testDefaultConstructor() {
    // Just validate that this doesn't throw an exception.
    new CASMutator<>(client, new LongTranscoder());
  }

  @Test
  public void testConcurrentCAS() throws Throwable {
    int num = SyncThread.getDistinctResultCount(20, new Callable<Long>() {
      public Long call() throws Exception {
        return mutator.cas("test.cas.concurrent", 0L, 0, mutation);
      }
    });
    assertEquals(20, num);
  }

  @Test
  public void testIncorrectTypeInCAS() throws Throwable {
    // Stick something for this CAS in the cache.
    assertTrue(client.set("x", 0, "not a long").get());
    try {
      Long rv = mutator.cas("x", 1L, 0, mutation);
      fail("Expected RuntimeException on invalid type mutation, got " + rv);
    } catch (RuntimeException e) {
      assertEquals("Couldn't get a CAS in 50 attempts", e.getMessage());
    }
  }

  @Test
  public void testCASUpdateWithNullInitial() throws Throwable {
    assertTrue(client.set("x", 0, 1L).get());
    Long rv = mutator.cas("x", (Long) null, 0, mutation);
    assertEquals(rv, (Long) 2L);
  }

  @Test
  public void testCASUpdateWithNullInitialNoExistingVal() throws Throwable {
    assertNull(client.get("x"));
    Long rv = mutator.cas("x", (Long) null, 0, mutation);
    assertNull(rv);
    assertNull(client.get("x"));
  }

  @Test
  public void testCASValueToString() {
    CASValue<String> c = new CASValue<>(717L, "hi");
    assertEquals("{CasValue 717/hi}", c.toString());
  }
}
