package net.spy.memcached.transcoders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.spy.memcached.compat.CloseUtil;
import net.spy.memcached.compat.SpyObject;

/**
 * Utility class for compression and decompression operations.
 */
public class CompressionUtils extends SpyObject {

  public static final int DEFAULT_COMPRESSION_THRESHOLD = 16384;

  private int compressionThreshold;

  public CompressionUtils() {
    this(DEFAULT_COMPRESSION_THRESHOLD);
  }

  public CompressionUtils(int compressionThreshold) {
    this.compressionThreshold = compressionThreshold;
  }

  public void setCompressionThreshold(int compressionThreshold) {
    this.compressionThreshold = compressionThreshold;
  }

  public boolean isCompressionCandidate(byte[] data) {
    return data != null && data.length > compressionThreshold;
  }

  /**
   * Compress the given array of bytes.
   */
  public byte[] compress(byte[] in) {
    if (in == null) {
      throw new NullPointerException("Can't compress null");
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GZIPOutputStream gz = null;
    try {
      gz = new GZIPOutputStream(bos);
      gz.write(in);
    } catch (IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    } finally {
      CloseUtil.close(gz);
      CloseUtil.close(bos);
    }
    byte[] rv = bos.toByteArray();
    getLogger().debug("Compressed %d bytes to %d", in.length, rv.length);
    return rv;
  }

  /**
   * Decompress the given array of bytes.
   */
  public byte[] decompress(byte[] in) {
    ByteArrayOutputStream bos = null;
    if (in != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(in);
      bos = new ByteArrayOutputStream();
      GZIPInputStream gis;
      try {
        gis = new GZIPInputStream(bis);

        byte[] buf = new byte[8192];
        int r;
        while ((r = gis.read(buf)) > 0) {
          bos.write(buf, 0, r);
        }
      } catch (IOException e) {
        getLogger().warn("Failed to decompress data", e);
        bos = null;
      }
    }
    return bos == null ? null : bos.toByteArray();
  }
}
