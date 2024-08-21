// Copyright (c)  2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.compat;

import junit.framework.TestCase;

import org.jmock.Mockery;

/**
 * Base test case for mock object tests.
 */
public abstract class BaseMockCase extends TestCase {
  protected Mockery context;

  @Override
  protected void setUp() throws Exception {
    context = new Mockery();
  }

  @Override
  protected void tearDown() throws Exception {
    context.assertIsSatisfied();
  }
}
