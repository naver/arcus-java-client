// Copyright (c)  2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.compat;

import org.junit.After;
import org.junit.Before;

import org.jmock.Mockery;

/**
 * Base test case for mock object tests.
 */
public abstract class BaseMockCase {
  protected Mockery context;

  @Before
  public void setUp() throws Exception {
    context = new Mockery();
  }

  @After
  public void tearDown() throws Exception {
    context.assertIsSatisfied();
  }
}
