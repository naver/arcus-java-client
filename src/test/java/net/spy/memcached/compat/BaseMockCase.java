// Copyright (c)  2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.compat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.jmock.Mockery;

/**
 * Base test case for mock object tests.
 */
public abstract class BaseMockCase {
  protected Mockery context;

  @BeforeEach
  protected void setUp() throws Exception {
    context = new Mockery();
  }

  @AfterEach
  protected void tearDown() throws Exception {
    context.assertIsSatisfied();
  }
}
