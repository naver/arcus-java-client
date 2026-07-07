package net.spy.memcached.v2.attribute;

import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.v2.vo.BKey;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UpdateAttributesTest {

  @Test
  void emptyProducesEmptyUpdateAttributes() {
    assertThrows(IllegalArgumentException.class, () -> UpdateAttributes.builder().build());
  }

  @Test
  void maxBKeyRangeAppears() {
    UpdateAttributes attributes = UpdateAttributes.builder()
        .maxBKeyRange(BKey.of(100L))
        .build();

    assertEquals("maxbkeyrange=100", attributes.stringify());
  }

  @Test
  void onlySetFieldsAppear() {
    UpdateAttributes attributes = UpdateAttributes.builder()
        .expireTime(3_000_000_000L) // out of int range
        .build();

    assertEquals("expiretime=3000000000", attributes.stringify());
  }

  @Test
  void multipleFieldsJoinedBySpace() {
    UpdateAttributes attributes = UpdateAttributes.builder()
        .maxCount(2_000L)
        .overflowAction(CollectionOverflowAction.error)
        .readable(true)
        .build();

    assertEquals("maxcount=2000 overflowaction=error readable=on", attributes.stringify());
  }

  @Test
  void getLengthMatchesStringify() {
    UpdateAttributes attributes = UpdateAttributes.builder()
        .expireTime(60L)
        .build();

    assertEquals(attributes.stringify().length(), attributes.getLength());
  }
}
