package net.spy.memcached.v2.attribute;

import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.CollectionType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ItemAttributesTest {

  @Test
  void parsesCommandAttributes() {
    ItemAttributes attributes = new ItemAttributes();
    attributes.setAttribute("flags=0");
    attributes.setAttribute("expiretime=2208988800"); // > Integer.MAX_VALUE
    attributes.setAttribute("type=list");
    attributes.setAttribute("count=3");
    attributes.setAttribute("maxcount=4000");
    attributes.setAttribute("overflowaction=error");
    attributes.setAttribute("readable=on");

    assertEquals(Integer.valueOf(0), attributes.getFlags());
    assertEquals(Long.valueOf(2208988800L), attributes.getExpireTime());
    assertEquals(CollectionType.list, attributes.getType());
    assertEquals(Long.valueOf(3L), attributes.getCount());
    assertEquals(Long.valueOf(4000L), attributes.getMaxCount());
    assertEquals(CollectionOverflowAction.error, attributes.getOverflowAction());
    assertEquals(Boolean.TRUE, attributes.getReadable());
  }

  @Test
  void unsetFieldsAreNull() {
    ItemAttributes attributes = new ItemAttributes();
    assertNull(attributes.getMaxBKeyRange());
    assertNull(attributes.getMinBKey());
    assertNull(attributes.getTrimmed());
  }

  @Test
  void parsesLongBKeyRange() {
    ItemAttributes attributes = new ItemAttributes();
    attributes.setAttribute("maxbkeyrange=100");
    assertEquals(100L, (long) attributes.getMaxBKeyRange().getData());
  }
}
