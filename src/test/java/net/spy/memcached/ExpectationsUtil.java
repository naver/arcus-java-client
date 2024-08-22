package net.spy.memcached;

import java.util.function.Consumer;

import org.jmock.Expectations;

public final class ExpectationsUtil {

  private ExpectationsUtil() {
  }

  public static Expectations buildExpectations(Consumer<Expectations> checkList) {
    Expectations expectations = new Expectations();
    checkList.accept(expectations);
    return expectations;
  }
}
