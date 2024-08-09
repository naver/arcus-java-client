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
package net.spy.memcached;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test the AddrUtils stuff.
 */
public class AddrUtilTest {

  @Test
  public void testSingle() throws Exception {
    List<InetSocketAddress> addrs =
            AddrUtil.getAddresses("www.google.com:80");
    assertEquals(1, addrs.size());
    assertEquals("www.google.com", addrs.get(0).getHostName());
    assertEquals(80, addrs.get(0).getPort());
  }

  @Test
  public void testTwo() throws Exception {
    List<InetSocketAddress> addrs =
            AddrUtil.getAddresses("www.google.com:80 www.yahoo.com:81");
    assertEquals(2, addrs.size());
    assertEquals("www.google.com", addrs.get(0).getHostName());
    assertEquals(80, addrs.get(0).getPort());
    assertEquals("www.yahoo.com", addrs.get(1).getHostName());
    assertEquals(81, addrs.get(1).getPort());
  }

  @Test
  public void testThree() throws Exception {
    List<InetSocketAddress> addrs =
            AddrUtil.getAddresses(" ,  www.google.com:80 ,, ,, www.yahoo.com:81 , ,,");
    assertEquals(2, addrs.size());
    assertEquals("www.google.com", addrs.get(0).getHostName());
    assertEquals(80, addrs.get(0).getPort());
    assertEquals("www.yahoo.com", addrs.get(1).getHostName());
    assertEquals(81, addrs.get(1).getPort());
  }

  @Test
  public void testBrokenHost() throws Exception {
    String s = "www.google.com:80 www.yahoo.com:81:more";
    try {
      List<InetSocketAddress> addrs = AddrUtil.getAddresses(s);
      fail("Expected failure, got " + addrs);
    } catch (NumberFormatException e) {
      e.printStackTrace();
      assertEquals("For input string: \"more\"", e.getMessage());
    }
  }

  @Test
  public void testBrokenHost2() throws Exception {
    String s = "www.google.com:80 www.yahoo.com";
    try {
      List<InetSocketAddress> addrs = AddrUtil.getAddresses(s);
      fail("Expected failure, got " + addrs);
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid server ``www.yahoo.com'' in list:  "
              + s, e.getMessage());
    }
  }

  @Test
  public void testNullList() throws Exception {
    String s = null;
    try {
      List<InetSocketAddress> addrs = AddrUtil.getAddresses(s);
      fail("Expected failure, got " + addrs);
    } catch (NullPointerException e) {
      assertEquals("Null host list", e.getMessage());
    }
  }

  @Test
  public void testIPv6Host() throws Exception {
    List<InetSocketAddress> addrs =
            AddrUtil.getAddresses("::1:80");
    assertEquals(1, addrs.size());

    Set<String> validLocalhostNames = new HashSet<>();
    validLocalhostNames.add("localhost");
    validLocalhostNames.add("ip6-localhost");
    validLocalhostNames.add("localhost6.localdomain6");
    validLocalhostNames.add("0:0:0:0:0:0:0:1");
    assert (validLocalhostNames.contains(addrs.get(0).getHostName()));
    assertEquals(80, addrs.get(0).getPort());
  }
}
