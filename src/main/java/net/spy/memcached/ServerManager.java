package net.spy.memcached;

public interface ServerManager {
  ArcusClient[] getClients();

  void shutdown();
}
