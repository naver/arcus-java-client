package net.spy.memcached.transcoders.compression;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompressionCodecTest {

  private static final byte[] LARGE_DATA =
      repeat("hello arcus ", 2000).getBytes(StandardCharsets.UTF_8);


  private static String repeat(String s, int times) {
    StringBuilder sb = new StringBuilder(s.length() * times);
    for (int i = 0; i < times; i++) {
      sb.append(s);
    }
    return sb.toString();
  }

  // ---- GZIP ----

  @Test
  void GZIPCompressAndDecompress() {
    GZIPCompressionCodec codec = new GZIPCompressionCodec();
    byte[] compressed = codec.compress(LARGE_DATA);
    byte[] decompressed = codec.decompress(compressed);

    assertNotNull(compressed);
    assertTrue(compressed.length < LARGE_DATA.length);
    assertArrayEquals(LARGE_DATA, decompressed);
  }

  @Test
  void GZIPCompressNullThrows() {
    assertThrows(IllegalArgumentException.class, () -> new GZIPCompressionCodec().compress(null));
  }

  @Test
  void GZIPDecompressNullReturnsNull() {
    assertNull(new GZIPCompressionCodec().decompress(null));
  }

  @Test
  void GZIPDecompressInvalidDataReturnsNull() {
    byte[] invalid = "not gzip data".getBytes(StandardCharsets.UTF_8);
    assertNull(new GZIPCompressionCodec().decompress(invalid));
  }

  @Test
  void GZIPIsCompressionCandidateDefaultThreshold() {
    GZIPCompressionCodec codec = new GZIPCompressionCodec();
    assertFalse(codec.isCompressionCandidate(new byte[100]));
    assertTrue(codec.isCompressionCandidate(new byte[16385]));
    assertFalse(codec.isCompressionCandidate(null));
  }

  @Test
  void GZIPSetCompressionThreshold() {
    GZIPCompressionCodec codec = new GZIPCompressionCodec();
    codec.setCompressionThreshold(100);
    assertFalse(codec.isCompressionCandidate(new byte[50]));
    assertTrue(codec.isCompressionCandidate(new byte[101]));
  }

  // ---- Snappy ----

  @Test
  void SnappyCompressAndDecompress() {
    SnappyCompressionCodec codec = new SnappyCompressionCodec();
    byte[] compressed = codec.compress(LARGE_DATA);
    byte[] decompressed = codec.decompress(compressed);

    assertNotNull(compressed);
    assertTrue(compressed.length < LARGE_DATA.length, "압축 후 크기가 원본보다 작아야 한다");
    assertArrayEquals(LARGE_DATA, decompressed);
  }

  @Test
  void SnappyCompressNullThrows() {
    assertThrows(IllegalArgumentException.class, () -> new SnappyCompressionCodec().compress(null));
  }

  @Test
  void SnappyDecompressNullReturnsNull() {
    assertNull(new SnappyCompressionCodec().decompress(null));
  }

  @Test
  void SnappyDecompressInvalidDataReturnsNull() {
    byte[] invalid = "not snappy data".getBytes(StandardCharsets.UTF_8);
    assertNull(new SnappyCompressionCodec().decompress(invalid));
  }

  @Test
  void SnappyIsCompressionCandidateDefaultThreshold() {
    SnappyCompressionCodec codec = new SnappyCompressionCodec();
    assertFalse(codec.isCompressionCandidate(new byte[100]));
    assertTrue(codec.isCompressionCandidate(new byte[16385]));
    assertFalse(codec.isCompressionCandidate(null));
  }

  @Test
  void SnappySetCompressionThreshold() {
    SnappyCompressionCodec codec = new SnappyCompressionCodec();
    codec.setCompressionThreshold(100);
    assertFalse(codec.isCompressionCandidate(new byte[50]));
    assertTrue(codec.isCompressionCandidate(new byte[101]));
  }

}
