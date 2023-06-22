package net.spy.memcached.reactive;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import net.spy.memcached.collection.BaseIntegrationTest;

public class ReactiveAsyncGetTest extends BaseIntegrationTest {

  private final String key = "ReactiveAsyncGetTest";
  private final String value = "value";

  private final int loopCount = 100;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    for (int i = 0; i < loopCount; i++) {
      mc.delete(key + i).get();
    }
    mc.delete(key).get();
  }

  public void testSingle() {
    try {
      mc.set(key, 0, value).get();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    mc.reactiveAsyncGet(key).whenComplete((v, e) -> {
      System.out.println("key: " + key + ", value: " + v);

       if (e != null) {
        e.printStackTrace();
        fail(e.getMessage());
      }
      assertEquals(value, v);
    }).join();
  }

  public void testMultiple() {
    try {
      for (int i = 0; i < loopCount; i++) {
        mc.set(key + i, 0, value + i).get();
      }

      @SuppressWarnings("rawtypes")
      CompletableFuture[] futures = IntStream.range(0, loopCount).mapToObj((i) -> {
        CompletableFuture<Object> future = mc.reactiveAsyncGet(key + i);
        return future.whenComplete((v, e) -> {
          System.out.println("key: " + (key + i) + ", value: " + v);

          if (e != null) {
            e.printStackTrace();
            fail(e.getMessage());
          }
          assertEquals(value + i, v);
        });
      }).toArray(CompletableFuture[]::new);

      CompletableFuture.allOf(futures).join();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public void testCancel() {
    ReactiveOperationFuture<Object> future = mc.reactiveAsyncGet(key);
    future.whenComplete((v, e) -> {
      System.out.println(e.getMessage());

      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("Cancelled"));
    });

    future.cancel(true);
  }
}
