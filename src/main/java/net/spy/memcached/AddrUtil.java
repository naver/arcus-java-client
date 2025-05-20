package net.spy.memcached;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Convenience utilities for simplifying common address parsing.
 */
public final class AddrUtil {

  private AddrUtil() {
  }

  /***
   * Convert a List of String containing IP addresses and port numbers
   * of the form "host:port" into a List of InetSocketAddress
   * instances suitable for instantiating a MemcachedClient.
   *
   * Note that colon-delimited IPv6 is also supported.
   * For example:  ::1:11211
   * @param s The {@link java.util.List} of {@link java.lang.String}
   *          containing {@code host:port}
   * @return The {@link java.util.List} of {@link java.net.InetSocketAddress}
   */
  public static List<InetSocketAddress> getAddresses(List<String> s) {
    if (s == null) {
      throw new NullPointerException("Null host list");
    }
    ArrayList<InetSocketAddress> addrs =
            new ArrayList<>();

    if (s.isEmpty()) {
      return addrs;
    }

    for (String hoststuff : s) {
      hoststuff = hoststuff.trim();
      if (hoststuff.equals("")) {
        continue;
      }
      addrs.add(getAddress(hoststuff));
    }
    assert !addrs.isEmpty() : "No addrs found";
    return Collections.unmodifiableList(addrs);
  }

  public static InetSocketAddress getAddress(String s) {
    s = s.trim();

    int finalColon = s.lastIndexOf(':');
    if (finalColon < 1) {
      throw new IllegalArgumentException("Invalid server ``" + s + "''");
    }
    String hostPart = s.substring(0, finalColon);
    String portNum = s.substring(finalColon + 1);

    return new InetSocketAddress(hostPart,
            Integer.parseInt(portNum));
  }

  public static String getSocketAddressString(SocketAddress addr) {
    if (addr instanceof InetSocketAddress) {
      InetSocketAddress temp = (InetSocketAddress) addr;

      if (temp.isUnresolved()) {
        return temp.getHostName() + ":" + temp.getPort();
      }
    }

    return addr.toString();
  }
}
