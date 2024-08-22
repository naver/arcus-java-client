package net.spy.memcached.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.internal.ImmediateFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jmock.Mockery;

import static net.spy.memcached.ExpectationsUtil.buildExpectations;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.jmock.AbstractExpectations.returnValue;

/**
 * Test the cache loader.
 */
public class CacheLoaderTest {

  private ExecutorService es = null;
  private Mockery context;

  @BeforeEach
  protected void setUp() throws Exception {
    context = new Mockery();
    BlockingQueue<Runnable> wq = new LinkedBlockingQueue<>();
    es = new ThreadPoolExecutor(10, 10, 5 * 60, TimeUnit.SECONDS, wq);
  }

  @AfterEach
  protected void tearDown() throws Exception {
    es.shutdownNow();
    context.assertIsSatisfied();
  }

  @Test
  public void testSimpleLoading() throws Exception {
    MemcachedClientIF m = context.mock(MemcachedClientIF.class);

    LoadCounter sl = new LoadCounter();
    CacheLoader cl = new CacheLoader(m, es, sl, 0);

    context.checking(buildExpectations(e -> {
      e.oneOf(m).set("a", 0, 1);
      e.will(returnValue(new ImmediateFuture(true)));
      e.oneOf(m).set("b", 0, 2);
      e.will(returnValue(new ImmediateFuture(new RuntimeException("blah"))));
      e.oneOf(m).set("c", 0, 3);
      e.will(returnValue(new ImmediateFuture(false)));
    }));

    Map<String, Object> map = new HashMap<>();
    map.put("a", 1);
    map.put("b", 2);
    map.put("c", 3);

    // Load the cache and wait for it to finish.
    cl.loadData(map).get();
    es.shutdown();
    es.awaitTermination(1, TimeUnit.SECONDS);

    assertEquals(1, sl.success);
    assertEquals(1, sl.exceptions);
    assertEquals(1, sl.failure);
  }

  static class LoadCounter implements CacheLoader.StorageListener {

    private volatile int exceptions = 0;
    private volatile int success = 0;
    private volatile int failure = 0;

    public void errorStoring(String k, Exception e) {
      exceptions++;
    }

    public void storeResult(String k, boolean result) {
      if (result) {
        success++;
      } else {
        failure++;
      }
    }

  }

}
