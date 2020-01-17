/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2013 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package net.spy.memcached.compat.log;

import org.apache.logging.log4j.message.ObjectMessage;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

/**
 * Logging implementation using
 * <a href="https://logging.apache.org/log4j/2.x/">log4j</a>.
 */
public class Log4JLogger extends AbstractLogger {

  private final ExtendedLoggerWrapper logger;

  /**
   * Get an instance of Log4JLogger.
   */
  public Log4JLogger(String name) {
    super(name);

    // Get the log4j logger instance.
    org.apache.logging.log4j.Logger l4jLogger = org.apache.logging.log4j.LogManager.getLogger(name);
    logger = new ExtendedLoggerWrapper((org.apache.logging.log4j.spi.AbstractLogger) l4jLogger, l4jLogger.getName(), l4jLogger.getMessageFactory());
  }

  /**
   * True if the underlying logger would allow trace messages through.
   */
  @Override
  public boolean isTraceEnabled() {
    return (logger.isTraceEnabled());
  }

  /**
   * True if the underlying logger would allow debug messages through.
   */
  @Override
  public boolean isDebugEnabled() {
    return (logger.isDebugEnabled());
  }

  /**
   * True if the underlying logger would allow info messages through.
   */
  @Override
  public boolean isInfoEnabled() {
    return (logger.isInfoEnabled());
  }

  /**
   * Wrapper around log4j.
   *
   * @param level   net.spy.compat.log.Level level.
   * @param message object message
   * @param e       optional throwable
   */
  @Override
  public void log(Level level, Object message, Throwable e) {
    org.apache.logging.log4j.Level pLevel;

    switch (level == null ? Level.FATAL : level) {
      case TRACE:
        pLevel = org.apache.logging.log4j.Level.TRACE;
        break;
      case DEBUG:
        pLevel = org.apache.logging.log4j.Level.DEBUG;
        break;
      case INFO:
        pLevel = org.apache.logging.log4j.Level.INFO;
        break;
      case WARN:
        pLevel = org.apache.logging.log4j.Level.WARN;
        break;
      case ERROR:
        pLevel = org.apache.logging.log4j.Level.ERROR;
        break;
      case FATAL:
        pLevel = org.apache.logging.log4j.Level.FATAL;
        break;
      default:
        // I don't know what this is, so consider it fatal
        pLevel = org.apache.logging.log4j.Level.FATAL;
        logger.logIfEnabled("net.spy.memcached.compat.log.AbstractLogger",
                pLevel,
                null,
                "Unhandled log level:  " + level
                        + " for the following message");
    }

    logger.logIfEnabled("net.spy.memcached.compat.log.AbstractLogger",
            pLevel,
            null,
            new ObjectMessage(message), e);
  }

}
