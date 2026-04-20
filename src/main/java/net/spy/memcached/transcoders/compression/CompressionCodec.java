package net.spy.memcached.transcoders.compression;

public interface CompressionCodec {

  byte[] compress(byte[] data);

  byte[] decompress(byte[] data);

  boolean isCompressionCandidate(byte[] data);

  void setCompressionThreshold(int threshold);
}
