package net.spy.memcached.v2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ArcusClientPool;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

abstract class ApiTest {
  protected static ArcusClientPool arcusClient;
  protected static AsyncArcusCommands<Object> async;
  protected static final List<String> keys = Arrays.asList("key0", "key1", "key2", "key3");
  protected static final String VALUE = "value";

  @BeforeAll
  static void setUp() {
    arcusClient = ArcusClient.createArcusClientPool("localhost:2181", "test", 4);
    async = arcusClient.asyncCommands();
  }

  @BeforeEach
  void cleanUp() throws ExecutionException, InterruptedException, TimeoutException {
    async.flush(-1).get(300, TimeUnit.MILLISECONDS);
  }

  @AfterAll
  static void tearDown() {
    arcusClient.shutdown();
  }

}
