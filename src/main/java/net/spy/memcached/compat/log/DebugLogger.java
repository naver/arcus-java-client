package net.spy.memcached.compat.log;

public class DebugLogger extends DefaultLogger {
  public DebugLogger(String name) {
    super(name, System.out);
  }

  @Override
  public boolean isDebugEnabled() {
    return (true);
  }

  @Override
  public boolean isTraceEnabled() {
    return (true);
  }
}
