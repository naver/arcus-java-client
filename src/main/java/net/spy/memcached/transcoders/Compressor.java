package net.spy.memcached.transcoders;

public interface Compressor {
  /**
   * Default compression threshold value.
   */
  int DEFAULT_COMPRESSION_THRESHOLD = 16384;

  /**
   * Compress the given array of bytes.
   */
  public byte[] compress(byte[] in);

  /**
   * Decompress the given array of bytes.
   *
   * @return null if the bytes cannot be decompressed
   */
  public byte[] decompress(byte[] in);

  public int getCompressionThreshold();

  public void setCompressionThreshold(int to);
}
