package net.spy.memcached.collection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateAttributesTest {

  @Test
  void builderDefaults() {
    CreateAttributes attributes = CreateAttributes.builder().build();
    assertEquals(0L, attributes.getExpireTime());
    assertEquals(4_000L, attributes.getMaxCount());
    assertNull(attributes.getOverflowAction());
    assertTrue(attributes.getReadable());
  }

  @Test
  void ofNullReturnsNull() {
    assertNull(CreateAttributes.of(null));
  }

  @Test
  void ofCopyValues() {
    CollectionAttributes prevAttr
        = new CollectionAttributes(60, 1_000L, CollectionOverflowAction.error);
    prevAttr.setReadable(false);

    CreateAttributes attributes = CreateAttributes.of(prevAttr);
    assertEquals(60L, attributes.getExpireTime());
    assertEquals(1_000L, attributes.getMaxCount());
    assertEquals(CollectionOverflowAction.error, attributes.getOverflowAction());
    assertFalse(attributes.getReadable());
  }
}
