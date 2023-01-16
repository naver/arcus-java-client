package net.spy.memcached;

import net.spy.memcached.compat.log.DefaultLogger;
import net.spy.memcached.compat.log.Log4JLogger;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.SLF4JLogger;
import net.spy.memcached.compat.log.SunLogger;

public final class LoggerSetter {
  private LoggerSetter() {}

  public static <T extends Logger> void setLogger(Class<T> logger) {
    System.setProperty("net.spy.log.LoggerImpl", logger.getName());
  }

  public static void setDefaultLogger() {
    setLogger(DefaultLogger.class);
  }

  public static void setLog4JLogger() {
    setLogger(Log4JLogger.class);
  }

  public static void setSLF4JLogger() {
    setLogger(SLF4JLogger.class);
  }

  public static void setSunLogger() {
    setLogger(SunLogger.class);
  }

}
