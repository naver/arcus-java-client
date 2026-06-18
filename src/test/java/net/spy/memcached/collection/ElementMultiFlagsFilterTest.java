package net.spy.memcached.collection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ElementMultiFlagsFilterTest {

  @Test
  void shouldRejectCompValueExceedingMaximumCount() {
    ElementMultiFlagsFilter filter = new ElementMultiFlagsFilter();

    for (int i = 0; i < 100; i++) {
      filter.addCompValue(new byte[]{(byte) i});
    }

    assertThrows(IllegalArgumentException.class, () -> filter.addCompValue(new byte[]{0}));
  }
}
