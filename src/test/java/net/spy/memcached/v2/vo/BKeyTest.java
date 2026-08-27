package net.spy.memcached.v2.vo;

import java.util.Arrays;

import net.spy.memcached.collection.BKeyObject;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BKeyTest {

  private static final int MAX_LENGTH = 31;
  private static final String MAX_BKEY_HEX =
      "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

  @Test
  void minByteArrayBKeyIsSingleZeroByte() {
    assertEquals(BKey.BKeyType.BYTE_ARRAY, BKey.MIN_BYTE_ARRAY_BKEY.getType());
    assertArrayEquals(new byte[]{(byte) 0x00}, (byte[]) BKey.MIN_BYTE_ARRAY_BKEY.getData());
    assertEquals("0x00", BKey.MIN_BYTE_ARRAY_BKEY.toString());
  }

  @Test
  void maxByteArrayBKeyIsMaxLengthOfFilledBytes() {
    byte[] data = (byte[]) BKey.MAX_BYTE_ARRAY_BKEY.getData();

    assertEquals(BKey.BKeyType.BYTE_ARRAY, BKey.MAX_BYTE_ARRAY_BKEY.getType());
    assertEquals(MAX_LENGTH, data.length);
    for (byte b : data) {
      assertEquals((byte) 0xFF, b);
    }
    assertEquals(MAX_BKEY_HEX, BKey.MAX_BYTE_ARRAY_BKEY.toString());
  }

  @Test
  void minIsLessThanMax() {
    assertTrue(BKey.MIN_BYTE_ARRAY_BKEY.compareTo(BKey.MAX_BYTE_ARRAY_BKEY) < 0);
  }

  @Test
  void rejectsByteArrayShorterThanMinLength() {
    assertThrows(IllegalArgumentException.class, () -> BKey.of(new byte[0]));
    assertThrows(IllegalArgumentException.class, () -> BKey.of("0x"));
  }

  @Test
  void rejectsByteArrayLongerThanMaxLength() {
    assertThrows(IllegalArgumentException.class, () -> BKey.of(new byte[MAX_LENGTH + 1]));
  }

  @Test
  void toBKeyObjectDoesNotExposeSharedConstantData() {
    BKeyObject leaked = BKey.MAX_BYTE_ARRAY_BKEY.toBKeyObject();
    Arrays.fill(leaked.getByteArrayBKeyRaw(), (byte) 0x00);

    for (byte b : (byte[]) BKey.MAX_BYTE_ARRAY_BKEY.getData()) {
      assertEquals((byte) 0xFF, b);
    }
  }

  @Test
  void getDataDoesNotExposeSharedConstantData() {
    Arrays.fill((byte[]) BKey.MIN_BYTE_ARRAY_BKEY.getData(), (byte) 0xFF);

    assertArrayEquals(new byte[]{(byte) 0x00}, (byte[]) BKey.MIN_BYTE_ARRAY_BKEY.getData());
  }

  @Test
  void boundsSurviveBKeyObjectRoundTrip() {
    assertEquals(BKey.MIN_BYTE_ARRAY_BKEY, BKey.of(BKey.MIN_BYTE_ARRAY_BKEY.toBKeyObject()));
    assertEquals(BKey.MAX_BYTE_ARRAY_BKEY, BKey.of(BKey.MAX_BYTE_ARRAY_BKEY.toBKeyObject()));
  }
}
