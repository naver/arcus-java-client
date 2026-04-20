package net.spy.memcached.transcoders.compression;


import java.io.IOException;

import net.spy.memcached.compat.SpyObject;

import org.xerial.snappy.Snappy;

public class SnappyCompressionCodec extends SpyObject implements CompressionCodec {

  private volatile int compressionThreshold = 16_384;

  @Override
  public byte[] compress(byte[] data) {
    if (data == null) {
      throw new IllegalArgumentException("Can't compress null");
    }

    try {
      byte[] compressed = Snappy.compress(data);
      getLogger().debug("Compressed %d bytes to %d", data.length, compressed.length);
      return compressed;
    } catch (IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    }
  }

  @Override
  public byte[] decompress(byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      byte[] decompress = Snappy.uncompress(data);
      getLogger().debug("Decompressed %d bytes to %d", data.length, decompress.length);
      return decompress;
    } catch (IOException e) {
      getLogger().warn("Failed to decompress data, returning null", e);
      return null;
    }
  }

  @Override
  public boolean isCompressionCandidate(byte[] data) {
    return data != null && data.length > compressionThreshold;
  }

  @Override
  public void setCompressionThreshold(int threshold) {
    this.compressionThreshold = threshold;
  }

}
