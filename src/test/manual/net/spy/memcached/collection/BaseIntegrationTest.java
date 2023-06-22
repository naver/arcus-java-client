/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.collection;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ArcusClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.LoggerSetter;

import org.junit.Ignore;

@Ignore
public class BaseIntegrationTest extends TestCase {

  public static final String ZK_HOST = System.getProperty("ZK_HOST",
          "127.0.0.1:2181");

  public static final String ZK_SERVICE_ID = System.getProperty("ZK_SERVICE_ID",
          "test");

  public static final String ARCUS_HOST = System.getProperty("ARCUS_HOST",
          "127.0.0.1:11211");

  public static final boolean USE_ZK = Boolean.valueOf(System.getProperty(
          "USE_ZK", "false"));

  protected static boolean SHUTDOWN_AFTER_EACH_TEST = USE_ZK;

  protected ArcusClient mc = null;

  static {
    LoggerSetter.setLog4JLogger();

    System.out.println("---------------------------------------------");
    System.out.println("[ArcusClient initialization info.]");
    System.out.println("\tUSE_ZK = " + USE_ZK);
    System.out.println("\tSHUTDOWN_AFTER_EACH_TEST = " + USE_ZK);

    if (USE_ZK) {
      System.out.println("\tZK_HOST = " + ZK_HOST);
      System.out.println("\tZK_SERVICE_ID = " + ZK_SERVICE_ID);
    } else {
      System.out.println("\tARCUS_HOST = " + ARCUS_HOST);
    }

    System.out.println("---------------------------------------------");
  }

  @Override
  protected void setUp() throws Exception {
    try {
      System.setProperty("arcus.mbean", "true");
      if (USE_ZK) {
        openFromZK();
      } else {
        openDirect();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      if (SHUTDOWN_AFTER_EACH_TEST) {
        shutdown();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected void openFromZK() {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    mc = ArcusClient.createArcusClient(ZK_HOST, ZK_SERVICE_ID, cfb);
  }

  protected void openDirect() throws Exception {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();

    final CountDownLatch latch = new CountDownLatch(
            ARCUS_HOST.split(",").length);
    final ConnectionObserver obs = new ConnectionObserver() {

      @Override
      public void connectionEstablished(SocketAddress sa,
                                        int reconnectCount) {
        latch.countDown();
      }

      @Override
      public void connectionLost(SocketAddress sa) {
        assert false : "Connection is failed.";
      }

    };
    cfb.setInitialObservers(Collections.singleton(obs));

    mc = new ArcusClient(cfb.build(), AddrUtil.getAddresses(ARCUS_HOST));
    latch.await();
  }

  protected void shutdown() throws Exception {
    mc.shutdown();
  }

  protected void addToList(String key, Object[] items) throws Exception {
    for (Object each : items) {
      assertTrue(mc.asyncLopInsert(key, -1, each,
              new CollectionAttributes())
              .get(1000, TimeUnit.MILLISECONDS));
    }
  }

  protected void addToSet(String key, Object[] items) throws Exception {
    for (Object each : items) {
      assertTrue(mc.asyncSopInsert(key, each, new CollectionAttributes())
              .get(1000, TimeUnit.MILLISECONDS));
    }
  }

  protected void addToBTree(String key, Object[] items) throws Exception {
    for (int i = 0; i < items.length; i++) {
      assertTrue(mc.asyncBopInsert(key, i, null, items[i],
              new CollectionAttributes())
              .get(1000, TimeUnit.MILLISECONDS));
    }
  }

  protected void addToMap(String key, Object[] items) throws Exception {
    for (int i = 0; i < items.length; i++) {
      assertTrue(mc.asyncMopInsert(key, String.valueOf(i), items[i],
              new CollectionAttributes())
              .get(1000, TimeUnit.MILLISECONDS));
    }
  }

  protected void deleteList(String key, int size) throws Exception {
    mc.asyncLopDelete(key, 0, size, true).get(1000, TimeUnit.MILLISECONDS);
  }

  protected void deleteSet(String key, Object[] items) throws Exception {
    for (Object d : items) {
      mc.asyncSopDelete(key, d, true).get(1000, TimeUnit.MILLISECONDS);
    }
  }

  protected void deleteBTree(String key, Object[] values) throws Exception {
    for (int i = 0; i < values.length; i++) {
      mc.asyncBopDelete(key, i, ElementFlagFilter.DO_NOT_FILTER, true)
              .get(1000, TimeUnit.MILLISECONDS);
    }
  }

  protected void deleteMap(String key) throws Exception {
    mc.asyncMopDelete(key, true).get(1000, TimeUnit.MILLISECONDS);
  }
}
