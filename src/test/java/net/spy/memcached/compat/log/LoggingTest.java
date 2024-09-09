// Copyright (c) 2005  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.compat.log;

// XXX:  This really needs to get log4j configured first.

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Make sure logging is enabled.
 */
public class LoggingTest {

  private Logger logger = null;

  /**
   * Set up logging.
   */
  @BeforeEach
  protected void setUp() throws Exception {
    logger = LoggerFactory.getLogger(getClass());
  }

  /**
   * Make sure logging is enabled.
   */
  @Test
  public void testDebugLogging() {
//  assertTrue("Debug logging is not enabled", logger.isDebugEnabled());
    logger.debug("debug message");
  }

  /**
   * Make sure info is enabled, and test it.
   */
  @Test
  public void testInfoLogging() {
    assertTrue(logger.isInfoEnabled());
    logger.info("info message");
  }

  /**
   * Test other log stuff.
   */
  @Test
  public void testOtherLogging() {
    logger.warn("warn message");
    logger.warn("test %s", "message");
    logger.error("error message");
    logger.error("test %s", "message");
    logger.fatal("fatal message");
    logger.fatal("test %s", "message");
    logger.log(null, "test null", null);
    assertEquals(getClass().getName(), logger.getName());
  }

  /**
   * Make sure we're using log4j.
   */
  @Test
  public void testLog4j() {
//  Logger l=LoggerFactory.getLogger(getClass());
//  assertEquals("net.spy.compat.log.Log4JLogger", l.getClass().getName());
  }

  /**
   * Test the sun logger.
   */
  @Test
  public void testSunLogger() {
    Logger l = new SunLogger(getClass().getName());
    assertFalse(l.isDebugEnabled());
    l.debug("debug message");
    assertTrue(l.isInfoEnabled());
    l.info("info message");
    l.warn("warn message");
    l.error("error message");
    l.fatal("fatal message");
    l.fatal("fatal message with exception", new Exception());
    l.log(null, "test null", null);
    l.log(null, "null message with exception and no requestor",
            new Exception());
  }

  /**
   * Test the default logger.
   */
  @Test
  public void testMyLogger() {
    Logger l = new DefaultLogger(getClass().getName());
    assertFalse(l.isDebugEnabled());
    l.debug("debug message");
    assertTrue(l.isInfoEnabled());
    l.info("info message");
    l.warn("warn message");
    l.error("error message");
    l.fatal("fatal message");
    l.fatal("fatal message with exception", new Exception());
    l.log(null, "test null", null);
    l.log(null, "null message with exception and no requestor",
            new Exception());

    try {
      l = new DefaultLogger(null);
      fail("Allowed me to create a logger with null name:  " + l);
    } catch (NullPointerException e) {
      assertEquals("Logger name may not be null.", e.getMessage());
    }
  }

  /**
   * Test stringing levels.
   */
  @Test
  public void testLevelStrings() {
    assertEquals("{LogLevel:  DEBUG}", String.valueOf(Level.DEBUG));
    assertEquals("{LogLevel:  INFO}", String.valueOf(Level.INFO));
    assertEquals("{LogLevel:  WARN}", String.valueOf(Level.WARN));
    assertEquals("{LogLevel:  ERROR}", String.valueOf(Level.ERROR));
    assertEquals("{LogLevel:  FATAL}", String.valueOf(Level.FATAL));
    assertEquals("DEBUG", Level.DEBUG.name());
    assertEquals("INFO", Level.INFO.name());
    assertEquals("WARN", Level.WARN.name());
    assertEquals("ERROR", Level.ERROR.name());
    assertEquals("FATAL", Level.FATAL.name());
  }

  /**
   * Test picking up an exception argument.
   */
  @Test
  public void testExceptionArg() throws Exception {
    Object[] args = new Object[]{"a", 42, new Exception("test")};
    Throwable t = ((AbstractLogger) logger).getThrowable(args);
    assertNotNull(t);
    assertEquals("test", t.getMessage());
  }

  /**
   * Test when the last argument is not an exception.
   */
  @Test
  public void testNoExceptionArg() throws Exception {
    Object[] args = new Object[]{"a", 42, new Exception("test"), "x"};
    Throwable t = ((AbstractLogger) logger).getThrowable(args);
    assertNull(t);
  }

}
